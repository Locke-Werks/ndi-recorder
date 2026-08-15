"""Discover NDI sources and get one onto disk, two different ways.

`record` shells out to Vizrt's headless recorder, which writes SpeedHQ into a MOV.
Full bandwidth NDI is already SpeedHQ on the wire and lands untouched, but an HX
source arrives as H.264 or HEVC and gets re-encoded on the way in, inflating the
file several times over.

`encode` receives frames through the NDI runtime and pipes them straight into
ffmpeg, so nothing large ever hits the disk. Still a decode plus an encode, but you
choose the codec and the bitrate. Only the Advanced SDK's compressed receive path
would avoid the re-encode altogether.
"""

import argparse
import ctypes
import json
import os
import queue
import re
import socket
import subprocess
import sys
import threading
import time
from array import array
from datetime import datetime
from pathlib import Path

RUNTIME_ENV = "NDI_RUNTIME_DIR_V6"
RUNTIME_FALLBACK = r"C:\Program Files\NDI\NDI 6 Tools\Runtime"
RUNTIME_DLL = "Processing.NDI.Lib.x64.dll"
RECORDER = Path(r"C:\Program Files\NDI\NDI 6 Tools\Studio Monitor\Application.NDIRecording.x64.exe")

INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*]')

RECV_COLOR_UYVY_BGRA = 1
RECV_BANDWIDTH_HIGHEST = 100
FRAME_VIDEO, FRAME_AUDIO, FRAME_METADATA, FRAME_ERROR = 1, 2, 3, 4

ENCODER_CANDIDATES = ["hevc_nvenc", "hevc_qsv", "hevc_amf", "libx265"]

# Ceilings on what gets held while ffmpeg starts, so a silent source cannot
# buffer its way through memory waiting for audio that is never coming.
BUFFER_LIMIT = 128 * 1024 * 1024
# Held frames all reach ffmpeg at once and so share a wallclock timestamp. Audio
# normally shows up within a frame or two, and this caps the damage when it does not.
AUDIO_GRACE = 0.5

# How far each writer may run ahead of ffmpeg before frames start going on the
# floor. Reaching either means the encoder cannot keep up with the source.
VIDEO_QUEUE = 60
AUDIO_QUEUE = 400


def fourcc(code):
    return int.from_bytes(code.encode(), "little")


# NDI hands these over as-is, so name the ffmpeg equivalent and the pixel width.
VIDEO_FORMATS = {
    fourcc("UYVY"): ("uyvy422", 2),
    fourcc("BGRA"): ("bgra", 4),
    fourcc("BGRX"): ("bgr0", 4),
    fourcc("RGBA"): ("rgba", 4),
    fourcc("RGBX"): ("rgb0", 4),
}


class NDIFindCreate(ctypes.Structure):
    _fields_ = [
        ("show_local_sources", ctypes.c_bool),
        ("p_groups", ctypes.c_char_p),
        ("p_extra_ips", ctypes.c_char_p),
    ]


class NDISource(ctypes.Structure):
    _fields_ = [
        ("p_ndi_name", ctypes.c_char_p),
        ("p_url_address", ctypes.c_char_p),
    ]


class NDIRecvCreate(ctypes.Structure):
    _fields_ = [
        ("source_to_connect_to", NDISource),
        ("color_format", ctypes.c_int),
        ("bandwidth", ctypes.c_int),
        ("allow_video_fields", ctypes.c_bool),
        ("p_ndi_recv_name", ctypes.c_char_p),
    ]


class NDIVideoFrame(ctypes.Structure):
    _fields_ = [
        ("xres", ctypes.c_int),
        ("yres", ctypes.c_int),
        ("fourcc", ctypes.c_int),
        ("frame_rate_n", ctypes.c_int),
        ("frame_rate_d", ctypes.c_int),
        ("picture_aspect_ratio", ctypes.c_float),
        ("frame_format_type", ctypes.c_int),
        ("timecode", ctypes.c_int64),
        ("p_data", ctypes.c_void_p),
        ("line_stride_in_bytes", ctypes.c_int),
        ("p_metadata", ctypes.c_char_p),
        ("timestamp", ctypes.c_int64),
    ]


class NDIAudioFrame(ctypes.Structure):
    _fields_ = [
        ("sample_rate", ctypes.c_int),
        ("no_channels", ctypes.c_int),
        ("no_samples", ctypes.c_int),
        ("timecode", ctypes.c_int64),
        ("p_data", ctypes.c_void_p),
        ("channel_stride_in_bytes", ctypes.c_int),
        ("p_metadata", ctypes.c_char_p),
        ("timestamp", ctypes.c_int64),
    ]


class NDIMetadataFrame(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_int),
        ("timecode", ctypes.c_int64),
        ("p_data", ctypes.c_char_p),
    ]


class NDIPerformance(ctypes.Structure):
    _fields_ = [
        ("video_frames", ctypes.c_int64),
        ("audio_frames", ctypes.c_int64),
        ("metadata_frames", ctypes.c_int64),
    ]


def load_runtime():
    directory = Path(os.environ.get(RUNTIME_ENV) or RUNTIME_FALLBACK)
    dll = directory / RUNTIME_DLL
    if not dll.exists():
        sys.exit(f"NDI runtime not found at {dll}. Install NDI Tools or set {RUNTIME_ENV}.")

    # The runtime resolves its own dependencies out of its install folder.
    os.add_dll_directory(str(directory))
    lib = ctypes.CDLL(str(dll))

    lib.NDIlib_initialize.restype = ctypes.c_bool
    lib.NDIlib_find_create_v2.argtypes = [ctypes.POINTER(NDIFindCreate)]
    lib.NDIlib_find_create_v2.restype = ctypes.c_void_p
    lib.NDIlib_find_wait_for_sources.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    lib.NDIlib_find_wait_for_sources.restype = ctypes.c_bool
    lib.NDIlib_find_get_current_sources.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
    lib.NDIlib_find_get_current_sources.restype = ctypes.POINTER(NDISource)
    lib.NDIlib_find_destroy.argtypes = [ctypes.c_void_p]
    lib.NDIlib_find_destroy.restype = None

    lib.NDIlib_recv_create_v3.argtypes = [ctypes.POINTER(NDIRecvCreate)]
    lib.NDIlib_recv_create_v3.restype = ctypes.c_void_p
    lib.NDIlib_recv_capture_v2.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(NDIVideoFrame),
        ctypes.POINTER(NDIAudioFrame), ctypes.POINTER(NDIMetadataFrame), ctypes.c_uint32,
    ]
    lib.NDIlib_recv_capture_v2.restype = ctypes.c_int
    lib.NDIlib_recv_free_video_v2.argtypes = [ctypes.c_void_p, ctypes.POINTER(NDIVideoFrame)]
    lib.NDIlib_recv_free_video_v2.restype = None
    lib.NDIlib_recv_free_audio_v2.argtypes = [ctypes.c_void_p, ctypes.POINTER(NDIAudioFrame)]
    lib.NDIlib_recv_free_audio_v2.restype = None
    lib.NDIlib_recv_free_metadata.argtypes = [ctypes.c_void_p, ctypes.POINTER(NDIMetadataFrame)]
    lib.NDIlib_recv_free_metadata.restype = None
    lib.NDIlib_recv_get_performance.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(NDIPerformance), ctypes.POINTER(NDIPerformance),
    ]
    lib.NDIlib_recv_get_performance.restype = None
    lib.NDIlib_recv_destroy.argtypes = [ctypes.c_void_p]
    lib.NDIlib_recv_destroy.restype = None
    lib.NDIlib_destroy.restype = None

    if not lib.NDIlib_initialize():
        sys.exit("NDIlib_initialize failed: the CPU may not meet the NDI runtime's requirements.")
    return lib


def discover(lib, seconds=2.0, groups=None, extra_ips=None, include_local=True):
    settings = NDIFindCreate(
        show_local_sources=include_local,
        p_groups=groups.encode() if groups else None,
        p_extra_ips=extra_ips.encode() if extra_ips else None,
    )
    finder = lib.NDIlib_find_create_v2(ctypes.byref(settings))
    if not finder:
        sys.exit("NDIlib_find_create_v2 failed.")

    try:
        # Keep waiting for the whole window rather than stopping at the first
        # response, so slower responders on the subnet still land in the list.
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            lib.NDIlib_find_wait_for_sources(finder, max(1, int(min(500, remaining * 1000))))

        count = ctypes.c_uint32(0)
        array_ptr = lib.NDIlib_find_get_current_sources(finder, ctypes.byref(count))
        if not array_ptr:
            return []
        # The array belongs to the finder and dies with it, so copy the strings out.
        sources = [
            {
                "name": array_ptr[i].p_ndi_name.decode("utf-8", "replace"),
                "url": (array_ptr[i].p_url_address or b"").decode("utf-8", "replace"),
            }
            for i in range(count.value)
        ]
    finally:
        lib.NDIlib_find_destroy(finder)

    return sorted(sources, key=lambda s: s["name"].casefold())


def select(sources, wanted):
    if wanted:
        exact = [s for s in sources if s["name"] == wanted]
        if exact:
            return exact[0]
        partial = [s for s in sources if wanted.casefold() in s["name"].casefold()]
        if len(partial) == 1:
            return partial[0]
        if not partial:
            sys.exit(f"No NDI source matches {wanted!r}.")
        names = "\n".join(f"  {s['name']}" for s in partial)
        sys.exit(f"{wanted!r} matches more than one source:\n{names}")

    if len(sources) == 1:
        return sources[0]
    if not sys.stdin.isatty():
        sys.exit("Several sources found and no terminal to choose from. Pass a source name.")

    for i, source in enumerate(sources, 1):
        print(f"  {i}. {source['name']}")
    while True:
        answer = input(f"Source [1-{len(sources)}]: ").strip()
        if answer.isdigit() and 1 <= int(answer) <= len(sources):
            return sources[int(answer) - 1]


def default_output(source_name, directory, suffix):
    stem = INVALID_FILENAME_CHARS.sub("_", source_name).strip()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return Path(directory) / f"{stem} {stamp}{suffix}"


def make_reporter():
    """One status line, rewritten in place on a terminal and spaced out in a log."""
    tty = sys.stdout.isatty()
    last = [0.0]

    def show(text, force=False):
        now = time.monotonic()
        if not force and now - last[0] < (0.25 if tty else 5.0):
            return
        last[0] = now
        if tty:
            width = max(20, os.get_terminal_size().columns - 1)
            print(f"\r{text[:width]:<{width}}", end="", flush=True)
        else:
            print(text, flush=True)

    return show


# --- recording through the Vizrt tool ---------------------------------------

def pump_recorder_status(proc, report, verbose):
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        if verbose:
            print(line, flush=True)
        else:
            report(line)


def stop_recorder(proc, timeout=15.0):
    """Killing the recorder leaves an unfinalized MOV that needs -rebuild, so ask first."""
    for tag, grace in (("<stop/>", 2.0), ("<quit/>", timeout)):
        if proc.poll() is not None:
            return proc.returncode
        try:
            proc.stdin.write(tag + "\n")
            proc.stdin.flush()
        except (OSError, ValueError):
            break
        try:
            return proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            continue

    try:
        proc.stdin.close()
    except (OSError, ValueError):
        pass
    try:
        return proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            return proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            return proc.wait()


def record(source, output, duration=None, thumbnails=False, autochop=False,
           audio_gain=None, verbose=False):
    if not RECORDER.exists():
        sys.exit(f"Recorder not found at {RECORDER}. Install NDI Tools.")

    command = [str(RECORDER), "-i", source["name"], "-o", str(output), "-noautostart"]
    if source.get("url"):
        command += ["-u", source["url"]]
    if not thumbnails:
        command.append("-nothumbnail")
    if not autochop:
        command.append("-noautochop")
    if audio_gain is not None:
        command += ["-audiolevelgain", str(audio_gain)]

    proc = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        # Own process group, so Ctrl+C reaches this script and lets it stop the
        # recorder in order instead of the console killing it mid-file. No window,
        # so nothing flashes up when this is driven from a GUI or a scheduled task.
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
    )
    report = make_reporter()
    threading.Thread(target=pump_recorder_status, args=(proc, report, verbose), daemon=True).start()

    proc.stdin.write("<start/>\n")
    proc.stdin.flush()

    print(f"Recording {source['name']} to {output}.mov")
    print("Ctrl+C to stop." if duration is None else f"Stopping after {duration}s.")

    deadline = time.monotonic() + duration if duration else None
    try:
        while proc.poll() is None:
            if deadline and time.monotonic() >= deadline:
                break
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass

    code = stop_recorder(proc)
    print()

    written = sorted(Path(output).parent.glob(f"{Path(output).name}*"))
    for path in written:
        print(f"{path}  ({path.stat().st_size / 1_048_576:.1f} MiB)")
    if not written:
        print("No file was written.", file=sys.stderr)
    return code if code is not None else 0


# --- live transcode ----------------------------------------------------------

def preset_args(encoder, preset):
    if preset:
        return ["-preset", preset]
    if encoder.endswith("_nvenc"):
        return ["-preset", "p5"]
    if encoder.startswith("lib"):
        return ["-preset", "medium"]
    return []


def quality_args(encoder, quality):
    if encoder.endswith("_nvenc"):
        return ["-cq", str(quality)]
    if encoder.endswith("_qsv"):
        return ["-global_quality", str(quality)]
    if encoder.endswith("_amf"):
        return ["-rc", "cqp", "-qp_i", str(quality), "-qp_p", str(quality)]
    if encoder.startswith("lib"):
        return ["-crf", str(quality)]
    return []


def pick_encoder(width, height, pix_fmt):
    """Hardware encoders fail on formats they cannot take, so test before committing."""
    for candidate in ENCODER_CANDIDATES:
        probe = ["ffmpeg", "-hide_banner", "-v", "error", "-f", "lavfi",
                 "-i", f"nullsrc=s={width}x{height}", "-frames:v", "1",
                 "-c:v", candidate, "-pix_fmt", pix_fmt, "-f", "null", "-"]
        if subprocess.run(probe, capture_output=True,
                          creationflags=subprocess.CREATE_NO_WINDOW).returncode == 0:
            return candidate
    sys.exit("No working HEVC encoder found. Pass --encoder explicitly.")


def video_bytes(frame, bytes_per_pixel):
    row = frame.xres * bytes_per_pixel
    stride = frame.line_stride_in_bytes
    if stride == row:
        return ctypes.string_at(frame.p_data, row * frame.yres)
    # Padded rows have to be repacked; ffmpeg's rawvideo demuxer assumes none.
    return b"".join(
        ctypes.string_at(frame.p_data + y * stride, row) for y in range(frame.yres)
    )


def audio_bytes(frame):
    """NDI audio is planar float. ffmpeg's f32le wants it interleaved."""
    channels, samples = frame.no_channels, frame.no_samples
    interleaved = array("f")
    interleaved.frombytes(b"\x00\x00\x00\x00" * (samples * channels))
    for channel in range(channels):
        plane = array("f")
        plane.frombytes(
            ctypes.string_at(frame.p_data + channel * frame.channel_stride_in_bytes, samples * 4)
        )
        interleaved[channel::channels] = plane
    return interleaved.tobytes()


def socket_writer(conn, work, state, label):
    """Feed one ffmpeg input. A sentinel of None means drain and close cleanly."""
    try:
        while True:
            chunk = work.get()
            if chunk is None:
                break
            conn.sendall(chunk)
    except OSError as exc:
        state["error"] = f"{label} stream stopped: {exc}"
    finally:
        # Half closing is the EOF that lets ffmpeg finalize this stream.
        try:
            conn.shutdown(socket.SHUT_WR)
        except OSError:
            pass


def open_receiver(lib, source):
    # Keep the encoded strings alive until create() has copied them.
    name = source["name"].encode()
    url = (source.get("url") or "").encode() or None
    recv_name = b"ndi_capture"
    settings = NDIRecvCreate(
        source_to_connect_to=NDISource(name, url),
        color_format=RECV_COLOR_UYVY_BGRA,
        bandwidth=RECV_BANDWIDTH_HIGHEST,
        allow_video_fields=False,
        p_ndi_recv_name=recv_name,
    )
    recv = lib.NDIlib_recv_create_v3(ctypes.byref(settings))
    if not recv:
        sys.exit("NDIlib_recv_create_v3 failed.")
    return recv


def encode(lib, source, output, duration=None, encoder="auto", quality=26, preset=None,
           pix_fmt="yuv420p", audio_bitrate="192k", want_audio=True, connect_timeout=15.0,
           extra=None):
    if encoder == "auto":
        # Probe before connecting. Encoder support does not vary with resolution,
        # and a receiver left undrained while ffmpeg starts up loses frames.
        encoder = pick_encoder(1920, 1080, pix_fmt)

    recv = open_receiver(lib, source)
    video, audio, meta = NDIVideoFrame(), NDIAudioFrame(), NDIMetadataFrame()

    # ffmpeg needs geometry and rates before it will start, so everything that
    # arrives in the meantime has to be held. Buffering video but not audio, or
    # the reverse, offsets the two for the whole recording.
    first_video = None
    buffered_video, buffered_audio, buffered_bytes = [], [], 0
    audio_format = None
    audio_deadline = None
    print(f"Connecting to {source['name']}...")

    deadline = time.monotonic() + connect_timeout
    try:
        while True:
            now = time.monotonic()
            if first_video and (audio_format or not want_audio):
                break
            if now >= deadline or buffered_bytes >= BUFFER_LIMIT:
                break
            if audio_deadline and now >= audio_deadline:
                break
            kind = lib.NDIlib_recv_capture_v2(recv, ctypes.byref(video), ctypes.byref(audio),
                                              ctypes.byref(meta), 500)
            if kind == FRAME_VIDEO:
                if first_video is None:
                    if video.fourcc not in VIDEO_FORMATS:
                        lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                        lib.NDIlib_recv_destroy(recv)
                        sys.exit(f"Unsupported NDI pixel format {video.fourcc:#x}.")
                    source_pix_fmt, bpp = VIDEO_FORMATS[video.fourcc]
                    first_video = {
                        "width": video.xres, "height": video.yres,
                        "fps_n": video.frame_rate_n, "fps_d": video.frame_rate_d,
                        "pix_fmt": source_pix_fmt, "bpp": bpp,
                    }
                    audio_deadline = now + AUDIO_GRACE
                if (video.xres, video.yres) == (first_video["width"], first_video["height"]):
                    chunk = video_bytes(video, first_video["bpp"])
                    buffered_video.append(chunk)
                    buffered_bytes += len(chunk)
                lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
            elif kind == FRAME_AUDIO:
                if want_audio:
                    if audio_format is None:
                        audio_format = {"rate": audio.sample_rate, "channels": audio.no_channels}
                    chunk = audio_bytes(audio)
                    buffered_audio.append(chunk)
                    buffered_bytes += len(chunk)
                lib.NDIlib_recv_free_audio_v2(recv, ctypes.byref(audio))
            elif kind == FRAME_METADATA:
                lib.NDIlib_recv_free_metadata(recv, ctypes.byref(meta))
            elif kind == FRAME_ERROR:
                lib.NDIlib_recv_destroy(recv)
                sys.exit("The NDI receiver reported an error.")

        if first_video is None:
            lib.NDIlib_recv_destroy(recv)
            sys.exit(f"No video from {source['name']} within {connect_timeout:.0f}s.")
        if want_audio and audio_format is None:
            print("No audio on this source, recording video only.")

        width, height = first_video["width"], first_video["height"]
        fps = first_video["fps_n"] / first_video["fps_d"]

        command = [
            "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
            # Nothing about a raw stream needs probing, and probing costs us a
            # stall while ffmpeg waits for data we have not produced yet.
            "-probesize", "32", "-analyzeduration", "0",
            "-f", "rawvideo", "-pix_fmt", first_video["pix_fmt"],
            "-video_size", f"{width}x{height}",
            "-framerate", f"{first_video['fps_n']}/{first_video['fps_d']}",
            # A source rarely runs at exactly its nominal rate. Timing video off
            # arrival instead of the declared rate stops it sliding against audio,
            # which is paced by its own sample count and cannot drift.
            "-use_wallclock_as_timestamps", "1",
        ]

        with socket.socket() as video_listener, socket.socket() as audio_listener:
            video_listener.bind(("127.0.0.1", 0))
            video_listener.listen(1)
            command += ["-i", f"tcp://127.0.0.1:{video_listener.getsockname()[1]}"]

            if audio_format:
                audio_listener.bind(("127.0.0.1", 0))
                audio_listener.listen(1)
                command += [
                    "-probesize", "32", "-analyzeduration", "0",
                    "-f", "f32le", "-ar", str(audio_format["rate"]),
                    "-ac", str(audio_format["channels"]),
                    "-i", f"tcp://127.0.0.1:{audio_listener.getsockname()[1]}",
                ]

            command += ["-map", "0:v", "-c:v", encoder, "-pix_fmt", pix_fmt,
                        # Wallclock input is uneven by nature; duplicate or drop
                        # against the nominal rate to land a constant rate file.
                        "-fps_mode", "cfr",
                        "-r", f"{first_video['fps_n']}/{first_video['fps_d']}"]
            command += preset_args(encoder, preset) + quality_args(encoder, quality)
            if audio_format:
                command += ["-map", "1:a", "-c:a", "aac", "-b:a", audio_bitrate]
            if "hevc" in encoder and output.suffix.lower() in (".mp4", ".mov", ".m4v"):
                # QuickTime and Apple devices ignore HEVC tagged hev1.
                command += ["-tag:v", "hvc1"]
            command += list(extra or []) + [str(output)]

            proc = subprocess.Popen(
                command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW,
            )
            errors = []
            threading.Thread(target=lambda: errors.extend(proc.stderr), daemon=True).start()

            frames = 0
            video_listener.settimeout(20)
            audio_listener.settimeout(20)
            try:
                video_conn, _ = video_listener.accept()
                # ffmpeg opens and probes input 0 before it touches input 1, so it
                # needs a frame in hand before we can afford to block on the audio
                # connection that has not been made yet.
                video_conn.sendall(buffered_video[0])
                frames = 1
                audio_conn = audio_listener.accept()[0] if audio_format else None
            except socket.timeout:
                proc.kill()
                sys.exit("ffmpeg never connected back:\n" + "".join(errors).strip())

        # ffmpeg reads its two inputs in whatever order it needs to interleave
        # them, so one thread feeding both deadlocks as soon as it blocks writing
        # video while ffmpeg is waiting on audio. Give each input its own writer.
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

        for chunk in buffered_video[1:]:
            video_queue.put(chunk)
            frames += 1
        for chunk in buffered_audio:
            audio_queue.put(chunk)
        buffered_video.clear()
        buffered_audio.clear()

        print(f"Encoding {source['name']} to {output}")
        print(f"{width}x{height} @ {fps:.2f}fps, {first_video['pix_fmt']} in, "
              f"{encoder} {pix_fmt} out")
        print("Ctrl+C to stop." if duration is None else f"Stopping after {duration}s.")

        report = make_reporter()
        started = time.monotonic()
        stop_at = started + duration if duration else None
        overflow = 0

        try:
            while proc.poll() is None and "error" not in writer_state:
                if stop_at and time.monotonic() >= stop_at:
                    break
                kind = lib.NDIlib_recv_capture_v2(recv, ctypes.byref(video), ctypes.byref(audio),
                                                  ctypes.byref(meta), 1000)
                if kind == FRAME_VIDEO:
                    if video.xres != width or video.yres != height:
                        lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                        print("\nSource changed resolution, stopping.", file=sys.stderr)
                        break
                    chunk = video_bytes(video, first_video["bpp"])
                    lib.NDIlib_recv_free_video_v2(recv, ctypes.byref(video))
                    try:
                        video_queue.put_nowait(chunk)
                        frames += 1
                    except queue.Full:
                        overflow += 1
                    elapsed = time.monotonic() - started
                    size = output.stat().st_size / 1_048_576 if output.exists() else 0
                    report(f"{elapsed:6.1f}s  {frames:6d} frames  "
                           f"{frames / max(elapsed, 0.001):5.1f} fps  {size:7.1f} MiB")
                elif kind == FRAME_AUDIO:
                    if audio_conn:
                        chunk = audio_bytes(audio)
                        try:
                            audio_queue.put_nowait(chunk)
                        except queue.Full:
                            overflow += 1
                    lib.NDIlib_recv_free_audio_v2(recv, ctypes.byref(audio))
                elif kind == FRAME_METADATA:
                    lib.NDIlib_recv_free_metadata(recv, ctypes.byref(meta))
                elif kind == FRAME_ERROR:
                    print("\nThe NDI receiver reported an error.", file=sys.stderr)
                    break
        except KeyboardInterrupt:
            pass

        total, dropped = NDIPerformance(), NDIPerformance()
        lib.NDIlib_recv_get_performance(recv, ctypes.byref(total), ctypes.byref(dropped))

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
            code = proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.terminate()
            code = proc.wait(timeout=10)
    finally:
        lib.NDIlib_recv_destroy(recv)

    print()
    if code != 0:
        print("".join(errors).strip(), file=sys.stderr)
    if writer_state.get("error"):
        print(writer_state["error"], file=sys.stderr)
    if dropped.video_frames or dropped.audio_frames:
        print(f"NDI dropped {dropped.video_frames} video and {dropped.audio_frames} "
              f"audio frames, so audio and video may drift.", file=sys.stderr)
    if overflow:
        print(f"{overflow} frames went on the floor because the encoder fell behind. "
              f"Try a faster --encoder or --preset.", file=sys.stderr)
    if output.exists():
        elapsed = time.monotonic() - started
        print(f"{output}  ({output.stat().st_size / 1_048_576:.1f} MiB, "
              f"{frames} frames in {elapsed:.1f}s)")
    else:
        print("No file was written.", file=sys.stderr)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--timeout", type=float, default=2.0,
                        help="seconds to spend discovering sources (default 2)")
    parser.add_argument("--groups", help="restrict discovery to these NDI groups")
    parser.add_argument("--extra-ips", help="comma separated IPs to probe beyond the local subnet")
    parser.add_argument("--no-local", action="store_true", help="hide sources published by this machine")
    sub = parser.add_subparsers(dest="command")

    listing = sub.add_parser("list", help="list visible NDI sources and exit")
    listing.add_argument("--json", action="store_true", help="emit JSON instead of a table")

    rec = sub.add_parser("record", help="record with the Vizrt tool, SpeedHQ into a MOV")
    rec.add_argument("source", nargs="?", help="source name or a unique part of one")
    rec.add_argument("-o", "--output", help="output path without extension")
    rec.add_argument("-d", "--dir", default=".", help="directory for the default filename")
    rec.add_argument("-t", "--duration", type=float, help="stop after this many seconds")
    rec.add_argument("--thumbnails", action="store_true", help="keep the preview streams")
    rec.add_argument("--autochop", action="store_true", help="let the recorder split files")
    rec.add_argument("--audio-gain", type=float, help="dB gain applied to recorded audio")
    rec.add_argument("-v", "--verbose", action="store_true", help="print every status line")

    enc = sub.add_parser("encode", help="transcode live through ffmpeg, nothing large on disk")
    enc.add_argument("source", nargs="?", help="source name or a unique part of one")
    enc.add_argument("-o", "--output", help="output file, extension picks the container")
    enc.add_argument("-d", "--dir", default=".", help="directory for the default filename")
    enc.add_argument("-t", "--duration", type=float, help="stop after this many seconds")
    enc.add_argument("--encoder", default="auto",
                     help="ffmpeg encoder, or auto to probe (default auto)")
    enc.add_argument("-q", "--quality", type=int, default=26,
                     help="cq for nvenc, crf for x265, lower is better (default 26)")
    enc.add_argument("--preset", help="encoder preset, defaults per encoder")
    enc.add_argument("--pix-fmt", default="yuv420p",
                     help="output pixel format (default yuv420p, use yuv422p to keep chroma)")
    enc.add_argument("--audio-bitrate", default="192k", help="AAC bitrate (default 192k)")
    enc.add_argument("--no-audio", action="store_true", help="drop the audio stream")
    enc.add_argument("--ffmpeg-arg", action="append", default=[], dest="extra",
                     help="extra ffmpeg output argument, repeat as needed")

    args = parser.parse_args()

    lib = load_runtime()
    try:
        sources = discover(lib, args.timeout, args.groups, args.extra_ips, not args.no_local)
        if not sources:
            print("No NDI sources found.", file=sys.stderr)
            return 1

        if args.command == "record":
            source = select(sources, args.source)
            output = Path(args.output) if args.output else default_output(source["name"], args.dir, "")
            output.parent.mkdir(parents=True, exist_ok=True)
            return record(source, output, args.duration, args.thumbnails,
                          args.autochop, args.audio_gain, args.verbose)

        if args.command == "encode":
            source = select(sources, args.source)
            output = Path(args.output) if args.output else default_output(source["name"], args.dir, ".mp4")
            output.parent.mkdir(parents=True, exist_ok=True)
            return encode(lib, source, output, args.duration, args.encoder, args.quality,
                          args.preset, args.pix_fmt, args.audio_bitrate,
                          not args.no_audio, extra=args.extra)

        if args.command == "list" and args.json:
            print(json.dumps(sources, indent=2))
            return 0

        width = max(len(s["name"]) for s in sources)
        for i, source in enumerate(sources, 1):
            print(f"{i:>3}. {source['name']:<{width}}  {source['url']}")
        return 0
    finally:
        lib.NDIlib_destroy()


if __name__ == "__main__":
    sys.exit(main())
