"""A 96 hour NDI ring buffer with a commit step, run as a background daemon.

Recording is continuous and segmented. Segments older than the ring window are
deleted automatically. `commit` lifts the most recent stretch out of the ring into
permanent storage and empties the ring, without interrupting the recording.

The daemon holds a control socket on loopback; every other subcommand is a client
that talks to it.
"""

import argparse
import ctypes
import json
import os
import queue
import shutil
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

from ndi_capture import (
    AUDIO_GRACE, AUDIO_QUEUE, BUFFER_LIMIT, FRAME_AUDIO, FRAME_ERROR,
    FRAME_METADATA, FRAME_VIDEO, NDIAudioFrame, NDIMetadataFrame, NDIPerformance,
    NDIVideoFrame, VIDEO_FORMATS, VIDEO_QUEUE, audio_bytes, discover, load_runtime,
    open_receiver, socket_writer, video_bytes,
)

# Set NDI_NVR_ROOT to keep footage somewhere other than the home directory, which
# on most machines is the last place you want a 24/7 write load.
DEFAULT_ROOT = Path(os.environ.get("NDI_NVR_ROOT") or Path.home() / "NDI-NVR")
RING_DIR = "ring"
COMMITTED_DIR = "committed"
STATE_FILE = "nvr-state.json"
ACTIVE_CONFIG = ".active-config.json"
CONFIG_FILE = "config.json"
LOG_FILE = "nvr.log"
SEGMENT_STAMP = "%Y%m%d-%H%M%S"

# How often to repeat "still nothing out there" while a source is missing. Often
# enough that a tail shows the daemon is alive, rare enough not to bury the log.
MISSING_NOTICE_SECONDS = 900

# Ring scanning accepts every container it might have written, so switching
# containers does not orphan the segments recorded before the change.
CONTAINERS = {"mkv": ("matroska", ".mkv"), "mp4": ("mp4", ".mp4")}
SEGMENT_GLOBS = tuple(f"*{extension}" for _, extension in CONTAINERS.values())

DEFAULTS = {
    "source": None,
    "ring_hours": 96,
    "commit_hours": 48,
    "segment_seconds": 300,
    "container": "mkv",
    "encoder": "av1_nvenc",
    "preset": "p5",
    "quality": 58,
    "maxrate": "400k",
    "bufsize": "4M",
    "keyframe_seconds": 5,
    "pix_fmt": "yuv420p",
    "want_audio": True,
    "audio_bitrate": "96k",
    # libopus is the better codec at 96k, but the segment muxer mangles the first
    # Opus packet of every file, so each segment decodes with one error and loses
    # about 20ms. AAC segments cleanly, which matters more for footage you review.
    "audio_codec": "aac",
    "ffmpeg": "ffmpeg",
    "source_timeout": 10.0,
    "reconnect_delay": 5.0,
    "extra_output_args": [],
}


def now_stamp():
    return datetime.now().strftime(SEGMENT_STAMP)


def log(message):
    print(f"{datetime.now():%Y-%m-%d %H:%M:%S}  {message}", flush=True)


def human_duration(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"


def segment_time(path):
    """Segment filenames are strftime stamps, which is also their start time."""
    try:
        return datetime.strptime(path.stem, SEGMENT_STAMP)
    except ValueError:
        return None


def ring_segments(root):
    """Completed segments oldest first, plus whichever one is still being written.

    Freshness decides, not position, so a ring left behind by a stopped daemon
    counts as complete rather than stranding its last segment forever.
    """
    found = [p for pattern in SEGMENT_GLOBS for p in (root / RING_DIR).glob(pattern)]
    files = sorted((p for p in found if segment_time(p)), key=lambda p: p.stem)
    if not files:
        return [], None
    if time.time() - files[-1].stat().st_mtime <= 30:
        return files[:-1], files[-1]
    return files, None


def directory_size(path):
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


# --- the recording session ---------------------------------------------------

def build_command(config, video_port, audio_port, video_meta, audio_meta, ring):
    fps_n, fps_d = video_meta["fps_n"], video_meta["fps_d"]
    fps = fps_n / fps_d
    segment_format, extension = CONTAINERS[config["container"]]
    command = [
        config["ffmpeg"], "-hide_banner", "-loglevel", "error", "-y",
        "-probesize", "32", "-analyzeduration", "0",
        "-f", "rawvideo", "-pix_fmt", video_meta["pix_fmt"],
        "-video_size", f"{video_meta['width']}x{video_meta['height']}",
        "-framerate", f"{fps_n}/{fps_d}",
        "-use_wallclock_as_timestamps", "1",
        "-i", f"tcp://127.0.0.1:{video_port}",
    ]
    if audio_meta:
        command += [
            "-probesize", "32", "-analyzeduration", "0",
            "-f", "f32le", "-ar", str(audio_meta["rate"]), "-ac", str(audio_meta["channels"]),
            "-i", f"tcp://127.0.0.1:{audio_port}",
        ]

    command += [
        "-map", "0:v", "-c:v", config["encoder"], "-pix_fmt", config["pix_fmt"],
        "-fps_mode", "cfr", "-r", f"{fps_n}/{fps_d}",
        "-g", str(max(1, int(round(fps * config["keyframe_seconds"])))),
    ]
    if config["preset"]:
        command += ["-preset", config["preset"]]
    # nvenc and qsv spell constant quality differently from the software encoders.
    if config["encoder"].endswith("_nvenc"):
        command += ["-cq", str(config["quality"])]
    elif config["encoder"].endswith("_qsv"):
        command += ["-global_quality", str(config["quality"])]
    elif config["encoder"].endswith("_amf"):
        command += ["-rc", "cqp", "-qp_i", str(config["quality"]),
                    "-qp_p", str(config["quality"])]
    else:
        command += ["-crf", str(config["quality"])]
    if config["maxrate"]:
        command += ["-maxrate", config["maxrate"], "-bufsize", config["bufsize"]]
    if audio_meta:
        command += ["-map", "1:a", "-c:a", config["audio_codec"], "-b:a", config["audio_bitrate"]]

    command += [
        # Write the open segment out as it goes rather than holding it in buffers.
        # Killing ffmpeg outright still left the in-progress segment readable with
        # everything up to the moment it died, so a crash costs seconds, not a
        # whole segment. Note the file reads as zero bytes until its writer lets
        # go, which is Windows reporting stale directory metadata, not data loss.
        "-flush_packets", "1",
        "-f", "segment",
        "-segment_time", str(config["segment_seconds"]),
        # Cutting on clock multiples keeps filenames tidy and makes the window a
        # commit covers predictable.
        "-segment_atclocktime", "1",
        "-segment_format", segment_format,
        "-reset_timestamps", "1", "-strftime", "1",
    ]
    command += list(config["extra_output_args"])
    command.append(str(ring / f"{SEGMENT_STAMP}{extension}"))
    return command


def run_session(lib, source, config, root, state, should_stop):
    """Record until the source goes away or a stop is asked for.

    Returns the reason it ended so the supervisor knows whether to reconnect.
    """
    ring = root / RING_DIR
    ring.mkdir(parents=True, exist_ok=True)

    recv = open_receiver(lib, source)
    video, audio, meta = NDIVideoFrame(), NDIAudioFrame(), NDIMetadataFrame()
    video_meta, audio_meta = None, None
    buffered_video, buffered_audio, buffered_bytes = [], [], 0
    audio_deadline = None
    deadline = time.monotonic() + config["source_timeout"]

    try:
        while True:
            now = time.monotonic()
            if video_meta and (audio_meta or not config["want_audio"]):
                break
            if should_stop():
                return "stopped"
            if now >= deadline or buffered_bytes >= BUFFER_LIMIT:
                break
            if audio_deadline and now >= audio_deadline:
                break
            kind = lib.NDIlib_recv_capture_v2(recv, ctypes.byref(video), ctypes.byref(audio),
                                              ctypes.byref(meta), 500)
            if kind == FRAME_VIDEO:
                if video_meta is None:
                    if video.fourcc not in VIDEO_FORMATS:
                        lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                        return "unsupported_format"
                    source_pix_fmt, bpp = VIDEO_FORMATS[video.fourcc]
                    video_meta = {
                        "width": video.xres, "height": video.yres,
                        "fps_n": video.frame_rate_n, "fps_d": video.frame_rate_d,
                        "pix_fmt": source_pix_fmt, "bpp": bpp,
                    }
                    audio_deadline = now + AUDIO_GRACE
                if (video.xres, video.yres) == (video_meta["width"], video_meta["height"]):
                    chunk = video_bytes(video, video_meta["bpp"])
                    buffered_video.append(chunk)
                    buffered_bytes += len(chunk)
                lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
            elif kind == FRAME_AUDIO:
                if config["want_audio"]:
                    if audio_meta is None:
                        audio_meta = {"rate": audio.sample_rate, "channels": audio.no_channels}
                    chunk = audio_bytes(audio)
                    buffered_audio.append(chunk)
                    buffered_bytes += len(chunk)
                lib.NDIlib_recv_free_audio_v2(recv, ctypes.byref(audio))
            elif kind == FRAME_METADATA:
                lib.NDIlib_recv_free_metadata(recv, ctypes.byref(meta))
            elif kind == FRAME_ERROR:
                return "receiver_error"

        if video_meta is None:
            return "no_video"

        with socket.socket() as video_listener, socket.socket() as audio_listener:
            video_listener.bind(("127.0.0.1", 0))
            video_listener.listen(1)
            audio_listener.bind(("127.0.0.1", 0))
            audio_listener.listen(1)
            command = build_command(config, video_listener.getsockname()[1],
                                    audio_listener.getsockname()[1], video_meta, audio_meta, ring)

            proc = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True,
                # The daemon runs without a console, so ffmpeg would allocate its
                # own and flash a blank window on the desktop every rollover.
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
            errors = []
            threading.Thread(target=lambda: errors.extend(proc.stderr), daemon=True).start()

            video_listener.settimeout(20)
            audio_listener.settimeout(20)
            try:
                video_conn, _ = video_listener.accept()
                # ffmpeg probes input 0 before it opens input 1, so it needs a frame
                # before we can block waiting on the audio connection.
                video_conn.sendall(buffered_video[0])
                audio_conn = audio_listener.accept()[0] if audio_meta else None
            except socket.timeout:
                proc.kill()
                log(f"ffmpeg never connected back: {''.join(errors).strip()}")
                return "ffmpeg_failed"

        # One writer per input. A single thread feeding both deadlocks the moment
        # it blocks on video while ffmpeg is waiting for audio.
        video_queue = queue.Queue(maxsize=VIDEO_QUEUE)
        audio_queue = queue.Queue(maxsize=AUDIO_QUEUE)
        writer_state = {}
        writers = [threading.Thread(target=socket_writer, daemon=True,
                                    args=(video_conn, video_queue, writer_state, "video"))]
        if audio_conn:
            writers.append(threading.Thread(target=socket_writer, daemon=True,
                                            args=(audio_conn, audio_queue, writer_state, "audio")))
        for writer in writers:
            writer.start()

        frames = 0
        for chunk in buffered_video[1:]:
            video_queue.put(chunk)
            frames += 1
        for chunk in buffered_audio:
            audio_queue.put(chunk)
        buffered_video.clear()
        buffered_audio.clear()

        with state["lock"]:
            state["connected"] = True
            state["resolution"] = f"{video_meta['width']}x{video_meta['height']}"
            state["fps"] = round(video_meta["fps_n"] / video_meta["fps_d"], 2)
            state["audio"] = bool(audio_meta)
        log(f"recording {source['name']} at {video_meta['width']}x{video_meta['height']} "
            f"{video_meta['fps_n'] / video_meta['fps_d']:.2f}fps, audio={bool(audio_meta)}")

        reason = "stopped"
        last_video = time.monotonic()
        try:
            while True:
                if should_stop():
                    break
                if proc.poll() is not None:
                    reason = "ffmpeg_died"
                    log(f"ffmpeg exited {proc.returncode}: {''.join(errors).strip()}")
                    break
                if "error" in writer_state:
                    reason = "writer_error"
                    log(writer_state["error"])
                    break
                if time.monotonic() - last_video > config["source_timeout"]:
                    reason = "source_lost"
                    break

                kind = lib.NDIlib_recv_capture_v2(recv, ctypes.byref(video), ctypes.byref(audio),
                                                  ctypes.byref(meta), 1000)
                if kind == FRAME_VIDEO:
                    if (video.xres, video.yres) != (video_meta["width"], video_meta["height"]):
                        lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                        reason = "resolution_changed"
                        break
                    chunk = video_bytes(video, video_meta["bpp"])
                    lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                    last_video = time.monotonic()
                    try:
                        video_queue.put_nowait(chunk)
                        frames += 1
                    except queue.Full:
                        with state["lock"]:
                            state["overflow"] += 1
                    with state["lock"]:
                        state["frames"] = frames
                elif kind == FRAME_AUDIO:
                    if audio_conn:
                        chunk = audio_bytes(audio)
                        try:
                            audio_queue.put_nowait(chunk)
                        except queue.Full:
                            with state["lock"]:
                                state["overflow"] += 1
                    lib.NDIlib_recv_free_audio_v2(recv, ctypes.byref(audio))
                elif kind == FRAME_METADATA:
                    lib.NDIlib_recv_free_metadata(recv, ctypes.byref(meta))
                elif kind == FRAME_ERROR:
                    reason = "receiver_error"
                    break
        finally:
            total, dropped = NDIPerformance(), NDIPerformance()
            lib.NDIlib_recv_get_performance(recv, ctypes.byref(total), ctypes.byref(dropped))
            with state["lock"]:
                state["dropped"] = dropped.video_frames
                state["connected"] = False

            try:
                video_queue.put(None, timeout=30)
                if audio_conn:
                    audio_queue.put(None, timeout=30)
            except queue.Full:
                pass
            for writer in writers:
                writer.join(timeout=30)
            for conn in (video_conn, audio_conn):
                if conn:
                    try:
                        conn.close()
                    except OSError:
                        pass
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
        return reason
    finally:
        lib.NDIlib_recv_destroy(recv)


# --- retention and commit ----------------------------------------------------

def prune_ring(root, ring_hours):
    """Drop segments that have aged out. The in-progress one is never touched."""
    completed, _ = ring_segments(root)
    cutoff = datetime.now() - timedelta(hours=ring_hours)
    removed, freed = 0, 0
    for path in completed:
        if segment_time(path) < cutoff:
            try:
                freed += path.stat().st_size
                path.unlink()
                removed += 1
            except OSError as exc:
                log(f"could not remove {path.name}: {exc}")
    if removed:
        log(f"pruned {removed} segments past {ring_hours}h, freed {freed / 1_073_741_824:.2f} GiB")
    return removed, freed


def commit(root, hours, keep_ring):
    """Move the most recent `hours` of ring into permanent storage, then empty it.

    The segment ffmpeg is currently writing stays where it is either way.
    """
    completed, in_progress = ring_segments(root)
    if not completed:
        return {"committed": 0, "cleared": 0, "bytes": 0, "destination": None,
                "note": "nothing completed in the ring yet"}

    cutoff = datetime.now() - timedelta(hours=hours)
    to_commit = [p for p in completed if segment_time(p) >= cutoff]
    to_clear = [] if keep_ring else [p for p in completed if p not in to_commit]

    destination = root / COMMITTED_DIR / now_stamp()
    destination.mkdir(parents=True, exist_ok=True)

    moved, moved_bytes = 0, 0
    for path in to_commit:
        try:
            size = path.stat().st_size
            shutil.move(str(path), str(destination / path.name))
            moved += 1
            moved_bytes += size
        except OSError as exc:
            log(f"commit could not move {path.name}: {exc}")

    cleared = 0
    for path in to_clear:
        try:
            path.unlink()
            cleared += 1
        except OSError as exc:
            log(f"commit could not remove {path.name}: {exc}")

    if moved == 0:
        # Nothing landed, so do not leave an empty directory behind.
        try:
            destination.rmdir()
        except OSError:
            pass

    log(f"commit: {moved} segments ({moved_bytes / 1_073_741_824:.2f} GiB) to "
        f"{destination.name}, cleared {cleared}")
    return {
        "committed": moved,
        "cleared": cleared,
        "bytes": moved_bytes,
        "destination": str(destination) if moved else None,
        "in_progress_kept": in_progress.name if in_progress else None,
    }


# --- daemon ------------------------------------------------------------------

def find_source(lib, wanted, timeout):
    """Hands back the source to record and every name discovered, because the
    caller has to tell an empty network apart from an ambiguous one."""
    sources = discover(lib, timeout)
    names = [s["name"] for s in sources]
    if not sources:
        return None, names
    if not wanted:
        return (sources[0] if len(sources) == 1 else None), names
    exact = [s for s in sources if s["name"] == wanted]
    if exact:
        return exact[0], names
    partial = [s for s in sources if wanted.casefold() in s["name"].casefold()]
    return (partial[0] if len(partial) == 1 else None), names


def describe_absence(wanted, seen):
    """Why find_source came back empty. "No source" covers both a silent network
    and several sources with no way to pick one, and the fix differs."""
    if not seen:
        return "nothing is broadcasting NDI"
    listed = ", ".join(seen)
    if wanted:
        return f"{wanted!r} does not resolve to one source, visible: {listed}"
    return f"no source is pinned and {len(seen)} are visible: {listed}"


def control_server(root, state, config):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    port = listener.getsockname()[1]

    (root / STATE_FILE).write_text(json.dumps({
        "pid": os.getpid(), "port": port, "root": str(root),
        "source": config["source"], "started": datetime.now().isoformat(timespec="seconds"),
        "config": {k: v for k, v in config.items() if k != "lock"},
    }, indent=2))

    def serve():
        while not state["stop"].is_set():
            try:
                conn, _ = listener.accept()
            except OSError:
                break
            with conn:
                try:
                    conn.settimeout(120)
                    request = json.loads(conn.makefile("r", encoding="utf-8").readline() or "{}")
                    reply = handle_command(request, root, state, config)
                except Exception as exc:
                    reply = {"ok": False, "error": str(exc)}
                try:
                    conn.sendall((json.dumps(reply) + "\n").encode())
                except OSError:
                    pass

    threading.Thread(target=serve, daemon=True).start()
    return listener, port


def handle_command(request, root, state, config):
    command = request.get("cmd")
    if command == "stop":
        state["stop"].set()
        return {"ok": True, "message": "stopping"}

    if command == "status":
        completed, in_progress = ring_segments(root)
        with state["lock"]:
            snapshot = {k: state[k] for k in
                        ("connected", "frames", "dropped", "overflow", "reconnects",
                         "resolution", "fps", "audio", "last_reason")}
        ring_bytes = sum(p.stat().st_size for p in completed)
        oldest = segment_time(completed[0]) if completed else None
        return {"ok": True, "running": True, "source": config["source"],
                "started": state["started"], "root": str(root),
                "ring_segments": len(completed),
                "ring_bytes": ring_bytes,
                "ring_hours_held": round((datetime.now() - oldest).total_seconds() / 3600, 1)
                if oldest else 0,
                "in_progress": in_progress.name if in_progress else None,
                "committed_bytes": directory_size(root / COMMITTED_DIR)
                if (root / COMMITTED_DIR).exists() else 0,
                **snapshot}

    if command == "commit":
        return {"ok": True, **commit(root, request.get("hours", config["commit_hours"]),
                                     request.get("keep_ring", False))}

    if command == "segments":
        completed, in_progress = ring_segments(root)
        committed_sets = []
        if (root / COMMITTED_DIR).exists():
            for entry in sorted((root / COMMITTED_DIR).iterdir()):
                if entry.is_dir():
                    files = [f for pattern in SEGMENT_GLOBS for f in entry.glob(pattern)]
                    committed_sets.append({
                        "name": entry.name, "segments": len(files),
                        "bytes": sum(f.stat().st_size for f in files),
                    })
        return {"ok": True,
                "ring": [{"name": p.name, "bytes": p.stat().st_size} for p in completed],
                "in_progress": in_progress.name if in_progress else None,
                "committed": committed_sets}

    return {"ok": False, "error": f"unknown command {command!r}"}


def run_daemon(root, config):
    root.mkdir(parents=True, exist_ok=True)
    (root / RING_DIR).mkdir(exist_ok=True)

    state = {
        "lock": threading.Lock(), "stop": threading.Event(),
        "started": datetime.now().isoformat(timespec="seconds"),
        "connected": False, "frames": 0, "dropped": 0, "overflow": 0,
        "reconnects": 0, "resolution": None, "fps": None, "audio": None,
        "last_reason": None,
    }
    listener, port = control_server(root, state, config)
    log(f"daemon up, pid {os.getpid()}, control port {port}, root {root}")

    def housekeeping():
        while not state["stop"].wait(60):
            # Pruning is tied to recording, not to the clock. An outage used to
            # cost a segment every five minutes with nothing replacing it, so a
            # source that wandered off overnight quietly ate the ring it existed
            # to fill.
            with state["lock"]:
                recording = state["connected"]
            if not recording:
                continue
            try:
                prune_ring(root, config["ring_hours"])
            except Exception as exc:
                log(f"prune failed: {exc}")

    threading.Thread(target=housekeeping, daemon=True).start()

    lib = load_runtime()
    missing_since = None
    last_notice = 0.0
    try:
        while not state["stop"].is_set():
            source, seen = find_source(lib, config["source"], 2.0)
            if not source:
                # The one failure that used to log nothing at all, which left an
                # absent camera looking exactly like a dead daemon.
                now = time.monotonic()
                if missing_since is None:
                    missing_since = last_notice = now
                    log(f"waiting for a source: "
                        f"{describe_absence(config['source'], seen)}")
                elif now - last_notice >= MISSING_NOTICE_SECONDS:
                    last_notice = now
                    log(f"still waiting after {human_duration(now - missing_since)}: "
                        f"{describe_absence(config['source'], seen)}")
                with state["lock"]:
                    state["last_reason"] = "source_not_found"
                state["stop"].wait(config["reconnect_delay"])
                continue

            if missing_since is not None:
                log(f"source found after "
                    f"{human_duration(time.monotonic() - missing_since)}")
                missing_since = None

            reason = run_session(lib, source, config, root, state, state["stop"].is_set)
            with state["lock"]:
                state["last_reason"] = reason
            if state["stop"].is_set():
                break
            with state["lock"]:
                state["reconnects"] += 1
            log(f"session ended ({reason}), reconnecting in {config['reconnect_delay']}s")
            state["stop"].wait(config["reconnect_delay"])
    finally:
        lib.NDIlib_destroy()
        try:
            listener.close()
        except OSError:
            pass
        try:
            (root / STATE_FILE).unlink()
        except OSError:
            pass
        log("daemon stopped")


# --- client ------------------------------------------------------------------

def load_config(root, config_path=None):
    """DEFAULTS, then the config file, then whatever the command line said."""
    config = dict(DEFAULTS)
    path = Path(config_path) if config_path else root / CONFIG_FILE
    if path.exists():
        try:
            stored = json.loads(path.read_text(encoding="utf-8"))
        except ValueError as exc:
            sys.exit(f"{path} is not valid JSON: {exc}")
        unknown = set(stored) - set(DEFAULTS)
        if unknown:
            sys.exit(f"{path} has unknown settings: {', '.join(sorted(unknown))}")
        config.update(stored)
    return config


def apply_overrides(config, args):
    """Only flags actually typed override the file, so defaults stay out of the way."""
    for key in DEFAULTS:
        value = getattr(args, key, None)
        if value is not None:
            config[key] = value
    if getattr(args, "no_audio", False):
        config["want_audio"] = False
    if getattr(args, "audio", False):
        config["want_audio"] = True
    return config


def read_state(root):
    try:
        return json.loads((root / STATE_FILE).read_text())
    except (OSError, ValueError):
        return None


def talk(root, request, timeout=120):
    info = read_state(root)
    if not info:
        return None
    try:
        with socket.create_connection(("127.0.0.1", info["port"]), timeout=5) as conn:
            conn.settimeout(timeout)
            conn.sendall((json.dumps(request) + "\n").encode())
            return json.loads(conn.makefile("r", encoding="utf-8").readline() or "{}")
    except OSError:
        return None


def human_bytes(count):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if count < 1024 or unit == "TiB":
            return f"{count:.1f} {unit}"
        count /= 1024


def cmd_start(args):
    root = Path(args.root)
    if talk(root, {"cmd": "status"}):
        print("Already running. Use stop first.", file=sys.stderr)
        return 1

    root.mkdir(parents=True, exist_ok=True)
    stale = root / STATE_FILE
    if stale.exists():
        stale.unlink()

    config = apply_overrides(load_config(root, args.config), args)
    if config["container"] not in CONTAINERS:
        print(f"Unknown container {config['container']!r}. "
              f"Pick one of {', '.join(CONTAINERS)}.", file=sys.stderr)
        return 1
    # Hand the resolved settings over in a file rather than on a command line, so
    # nothing depends on how Windows quotes an argument.
    (root / ACTIVE_CONFIG).write_text(json.dumps(config, indent=2), encoding="utf-8")

    # --root is a top level option, so it has to precede the subcommand.
    child = [sys.executable, str(Path(__file__).resolve()), "--root", str(root), "_daemon"]

    handle = open(root / LOG_FILE, "a", encoding="utf-8")
    detached = subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
    # Terminals and task runners often put their children in a job object that kills
    # everything when the parent goes. Breaking out of it is what makes this outlive
    # the shell that launched it. Not every job permits it, and a daemon that quietly
    # dies with the window is worse than one that says it is going to.
    broke_away = False
    for flags in (detached | subprocess.CREATE_BREAKAWAY_FROM_JOB, detached):
        try:
            subprocess.Popen(
                child, stdin=subprocess.DEVNULL, stdout=handle, stderr=subprocess.STDOUT,
                creationflags=flags, close_fds=True,
            )
            broke_away = flags != detached
            break
        except OSError as exc:
            if flags == detached:
                print(f"Could not start the recorder: {exc}", file=sys.stderr)
                return 1

    for _ in range(40):
        time.sleep(0.5)
        reply = talk(root, {"cmd": "status"})
        if reply:
            print(f"Started. Recording to {root / RING_DIR}, log at {root / LOG_FILE}")
            if not broke_away:
                print("Warning: this shell would not let the recorder leave its job "
                      "object, so it will be killed when this shell exits. Use "
                      "`autostart on`, or start it from a plain terminal.")
            return 0
    print(f"Daemon did not report ready. Check {root / LOG_FILE}", file=sys.stderr)
    return 1


def cmd_stop(args):
    root = Path(args.root)
    reply = talk(root, {"cmd": "stop"})
    if not reply:
        print("Not running.", file=sys.stderr)
        return 1
    for _ in range(60):
        time.sleep(0.5)
        if not talk(root, {"cmd": "status"}):
            print("Stopped.")
            return 0
    print("Stop requested but the daemon is still up. Check the log.", file=sys.stderr)
    return 1


def cmd_status(args):
    root = Path(args.root)
    reply = talk(root, {"cmd": "status"})
    if not reply:
        print("Not running.")
        return 1
    print(f"source        {reply['source'] or '(only source on the network)'}")
    print(f"connected     {reply['connected']}  {reply.get('resolution') or ''} "
          f"{reply.get('fps') or ''}{'fps' if reply.get('fps') else ''}"
          f"{'  audio' if reply.get('audio') else '  no audio'}")
    print(f"started       {reply['started']}")
    print(f"frames        {reply['frames']}  dropped {reply['dropped']}  "
          f"overflow {reply['overflow']}  reconnects {reply['reconnects']}")
    print(f"ring          {reply['ring_segments']} segments, "
          f"{human_bytes(reply['ring_bytes'])}, {reply['ring_hours_held']}h held")
    print(f"in progress   {reply['in_progress']}")
    print(f"committed     {human_bytes(reply['committed_bytes'])}")
    if reply.get("last_reason"):
        print(f"last event    {reply['last_reason']}")
    return 0


def cmd_commit(args):
    root = Path(args.root)
    # Leaving hours out lets the running daemon apply its own configured window.
    request = {"cmd": "commit", "keep_ring": args.keep_ring}
    if args.hours is not None:
        request["hours"] = args.hours
    reply = talk(root, request)
    if not reply:
        # The daemon owns the files while running, but committing an idle ring is fine.
        if read_state(root):
            print("Daemon is not answering.", file=sys.stderr)
            return 1
        hours = args.hours if args.hours is not None else load_config(root)["commit_hours"]
        reply = {"ok": True, **commit(root, hours, args.keep_ring)}
    if not reply.get("ok"):
        print(reply.get("error", "commit failed"), file=sys.stderr)
        return 1
    if reply.get("note"):
        print(reply["note"])
        return 0
    print(f"Committed {reply['committed']} segments ({human_bytes(reply['bytes'])}) to "
          f"{reply['destination']}")
    print(f"Cleared {reply['cleared']} older segments from the ring.")
    if reply.get("in_progress_kept"):
        print(f"Left {reply['in_progress_kept']} in the ring, still being written.")
    return 0


def cmd_segments(args):
    root = Path(args.root)
    reply = talk(root, {"cmd": "segments"})
    if not reply:
        completed, in_progress = ring_segments(root)
        reply = {"ring": [{"name": p.name, "bytes": p.stat().st_size} for p in completed],
                 "in_progress": in_progress.name if in_progress else None,
                 "committed": []}
    ring = reply["ring"]
    total = sum(entry["bytes"] for entry in ring)
    print(f"ring: {len(ring)} segments, {human_bytes(total)}")
    if ring:
        print(f"  {ring[0]['name']}  ..  {ring[-1]['name']}")
    if reply.get("in_progress"):
        print(f"  in progress: {reply['in_progress']}")
    for entry in reply.get("committed", []):
        print(f"committed {entry['name']}: {entry['segments']} segments, "
              f"{human_bytes(entry['bytes'])}")
    return 0


def cmd_log(args):
    path = Path(args.root) / LOG_FILE
    if not path.exists():
        print("No log yet.", file=sys.stderr)
        return 1
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(lines[-args.lines:]))
    return 0


def cmd_config(args):
    root = Path(args.root)
    path = Path(args.config) if args.config else root / CONFIG_FILE
    config = apply_overrides(load_config(root, args.config), args)

    if config["container"] not in CONTAINERS:
        print(f"Unknown container {config['container']!r}. "
              f"Pick one of {', '.join(CONTAINERS)}.", file=sys.stderr)
        return 1

    if args.write:
        root.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote {path}")
        if talk(root, {"cmd": "status"}):
            print("The running recorder keeps its current settings until you restart it.")
        return 0

    print(f"# effective settings, root {root}")
    print(f"# {path}{'' if path.exists() else ' (does not exist yet)'}")
    for key in sorted(config):
        marker = "" if config[key] == DEFAULTS[key] else "  <- changed"
        print(f"{key:20} {json.dumps(config[key])}{marker}")
    return 0


def cmd_autostart(args):
    startup = Path(os.environ["APPDATA"]) / "Microsoft/Windows/Start Menu/Programs/Startup"
    shortcut = startup / "ndi-nvr.cmd"
    if args.action == "off":
        if shortcut.exists():
            shortcut.unlink()
            print(f"Removed {shortcut}")
        else:
            print("Autostart was not set.")
        return 0

    pythonw = Path(sys.executable).with_name("pythonw.exe")
    runner = pythonw if pythonw.exists() else Path(sys.executable)
    source = f' "{args.source}"' if args.source else ""
    config = f' --config "{args.config}"' if args.config else ""
    shortcut.write_text(
        f'@echo off\r\n"{runner}" "{Path(__file__).resolve()}" '
        f'--root "{args.root}" start{source}{config}\r\n',
        encoding="utf-8",
    )
    print(f"Wrote {shortcut}")
    print("The recorder will start at next logon.")
    return 0


def add_setting_flags(parser):
    """Every knob, on both `start` and `config`, defaulting to None so that an
    untyped flag never shadows what the config file says."""
    parser.add_argument("--config", help=f"settings file (default <root>/{CONFIG_FILE})")
    parser.add_argument("--ring-hours", type=float, dest="ring_hours",
                        help="how much history the ring keeps")
    parser.add_argument("--commit-hours", type=float, dest="commit_hours",
                        help="default window a commit preserves")
    parser.add_argument("--segment-seconds", type=int, dest="segment_seconds",
                        help="length of each file")
    parser.add_argument("--container", choices=sorted(CONTAINERS), help="segment container")
    parser.add_argument("--encoder", help="any ffmpeg video encoder, e.g. av1_nvenc, libsvtav1")
    parser.add_argument("--preset", help="encoder preset, or empty string for none")
    parser.add_argument("--quality", type=int, help="cq/crf/qp, lower is better")
    parser.add_argument("--maxrate", help="ceiling, or empty string to uncap")
    parser.add_argument("--bufsize", help="rate control buffer")
    parser.add_argument("--keyframe-seconds", type=int, dest="keyframe_seconds",
                        help="seek granularity, and the smallest possible segment")
    parser.add_argument("--pix-fmt", dest="pix_fmt", help="output pixel format")
    parser.add_argument("--audio-bitrate", dest="audio_bitrate", help="audio bitrate")
    parser.add_argument("--audio-codec", dest="audio_codec", help="aac, libopus, libmp3lame")
    parser.add_argument("--ffmpeg", help="path to ffmpeg if it is not on PATH")
    parser.add_argument("--source-timeout", type=float, dest="source_timeout",
                        help="seconds without video before treating the source as gone")
    parser.add_argument("--reconnect-delay", type=float, dest="reconnect_delay",
                        help="seconds to wait between reconnect attempts")
    audio = parser.add_mutually_exclusive_group()
    audio.add_argument("--no-audio", action="store_true", help="record video only")
    audio.add_argument("--audio", action="store_true", help="record audio (the default)")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=str(DEFAULT_ROOT), help="storage root")
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start", help="launch the recorder in the background")
    start.add_argument("source", nargs="?", help="source name or a unique part of one")
    add_setting_flags(start)
    start.set_defaults(func=cmd_start)

    config_parser = sub.add_parser("config", help="show or save settings")
    config_parser.add_argument("source", nargs="?", help="source name or part of one")
    add_setting_flags(config_parser)
    config_parser.add_argument("--write", action="store_true",
                               help="save the result as the config file")
    config_parser.set_defaults(func=cmd_config)

    sub.add_parser("stop", help="stop the recorder").set_defaults(func=cmd_stop)
    sub.add_parser("status", help="show what the recorder is doing").set_defaults(func=cmd_status)
    sub.add_parser("segments", help="list ring and committed footage").set_defaults(func=cmd_segments)

    commit_parser = sub.add_parser("commit", help="preserve recent footage and empty the ring")
    commit_parser.add_argument("--hours", type=float,
                               help="how far back to preserve (default: the configured window)")
    commit_parser.add_argument("--keep-ring", action="store_true",
                               help="leave older segments in the ring instead of clearing")
    commit_parser.set_defaults(func=cmd_commit)

    log_parser = sub.add_parser("log", help="show the tail of the daemon log")
    log_parser.add_argument("-n", "--lines", type=int, default=40)
    log_parser.set_defaults(func=cmd_log)

    auto = sub.add_parser("autostart", help="run the recorder at logon")
    auto.add_argument("action", choices=["on", "off"])
    auto.add_argument("source", nargs="?", help="source to record, if more than one is ever up")
    auto.add_argument("--config", help="settings file to pass through to start")
    auto.set_defaults(func=cmd_autostart)

    sub.add_parser("_daemon", help=argparse.SUPPRESS).set_defaults(func=None)

    args = parser.parse_args()

    if args.command == "_daemon":
        root = Path(args.root)
        config = dict(DEFAULTS)
        config.update(json.loads((root / ACTIVE_CONFIG).read_text(encoding="utf-8")))
        run_daemon(root, config)
        return 0

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
