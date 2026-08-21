# ndi-recorder

A 24/7 network video recorder in two Python files, zero dependencies, and no build step.

## The premise

NVR software is normally a 400 MB installer, a background service, a web stack, a
database, a licensing server, a tray icon with a gradient in it, and an annual renewal.
It records a camera to a disk. That is the entire job description. Recording a video
stream to a disk is a problem the computer industry solved before most of us were born,
and somehow it now ships with a EULA.

This does the same job in about a thousand lines of Python that call two things you
already have installed.

There is no `pip install` step. Not "minimal dependencies," not "just a few packages."
Zero. The imports are all standard library. There is no compiler, no toolchain, no
`node_modules`, no Docker image, no Rust rewrite, no config DSL, no plugin architecture,
and no opinion about how you should organize your life.

The trick is that all the hard parts were already sitting on your hard drive:

- **NDI Tools** ships `Processing.NDI.Lib.x64.dll`, which knows how to find and receive
  NDI sources. Python's `ctypes` calls it directly. No SDK download, no bindings package,
  no wheel that only builds on the maintainer's laptop.
- **ffmpeg** already does every codec that matters. It just cannot ingest NDI, because
  the NDI input device was removed from upstream ffmpeg in 2021 over licensing.

So the entire program is: ask the DLL for frames, shove the bytes down a socket at
ffmpeg, and get out of the way. That is it. That is the whole architecture. You could
draw it on a napkin and have room left for the tip calculation.

## What it actually does

- Finds NDI sources on the network and records one continuously
- Encodes to AV1 on your GPU at roughly 250 kbps for 720p30, which is about 2.5 GB a day
- Writes clock-aligned segments so any moment is one file away
- Keeps a rolling window of history and deletes the rest without being asked
- Reconnects on its own when the source sleeps, reboots, or wanders off the wifi
- Runs detached with no console window, and starts itself at logon if you want
- Answers `status` from a control socket instead of making you read a log file

## Requirements

- Windows
- [NDI Tools](https://ndi.video/tools/) for the runtime DLL
- ffmpeg on `PATH`, ideally a build with `av1_nvenc` or another hardware encoder
- Python 3.8 or newer

## Quick start

```
python ndi_nvr.py start                     # records the only source it can see
python ndi_nvr.py start "Front Door"        # or name one, partial match is fine
python ndi_nvr.py status
python ndi_nvr.py commit                    # preserve recent footage, empty the ring
python ndi_nvr.py stop
python ndi_nvr.py autostart on "Front Door" # runs at logon, no console
```

Footage goes to `~/NDI-NVR` unless you set `NDI_NVR_ROOT` or pass `--root`. Point it at
a drive you do not mind writing to forever.

## The ring, and committing out of it

Continuous recording without a retention policy is just a slow way to fill a disk. This
uses a ring buffer with an escape hatch.

The ring holds the last **96 hours**. Anything older is deleted automatically, no
prompting, no dialog asking whether you are sure.

Pruning only runs while something is actually being recorded. If the source goes
away, the ring stops shrinking and waits, because deleting history you cannot
replace is a strange way to react to an outage.

When something happens that you want to keep, `commit` takes the most recent **48 hours**
out of the ring, moves it into `committed/<timestamp>/` where nothing will ever delete it,
and then **empties the ring entirely**. The 48 to 96 hour tail is discarded. That erasure
is on purpose: commit means "this is the interesting part, throw away the rest."

Recording never pauses for any of this. The segment ffmpeg currently has open is never
moved or deleted, so a commit cannot corrupt the file being written.

If you want the gentler version, `commit --keep-ring` preserves the window without
clearing what is left behind.

```
python ndi_nvr.py commit                       # 48 hours out, ring emptied
python ndi_nvr.py commit --hours 12            # smaller window
python ndi_nvr.py commit --keep-ring           # keep the rest of the ring
python ndi_nvr.py segments                     # what is in the ring and what was committed
```

## Configuration

Every knob is a flag, and every flag can be saved so you never type it again.

```
python ndi_nvr.py config                                   # show effective settings
python ndi_nvr.py config --encoder libsvtav1 --quality 40 --write
python ndi_nvr.py config --container mp4 --audio-codec libopus --write
```

Settings live in `<root>/config.json`. Precedence is defaults, then that file, then
anything you type on the command line. A flag you do not type never overrides the file,
so `config --write` actually sticks.

| setting | default | what it does |
|---|---|---|
| `source` | `null` | source name, or part of one. Null means "the only one visible" |
| `ring_hours` | `96` | how much history the ring keeps |
| `commit_hours` | `48` | default window a commit preserves |
| `segment_seconds` | `300` | length of each file |
| `container` | `mkv` | `mkv` or `mp4` |
| `encoder` | `av1_nvenc` | any ffmpeg video encoder |
| `preset` | `p5` | encoder preset, empty string for none |
| `quality` | `58` | cq, crf, or qp depending on encoder. Lower is better |
| `maxrate` | `400k` | bitrate ceiling, empty string to uncap |
| `bufsize` | `4M` | rate control buffer |
| `keyframe_seconds` | `5` | seek granularity |
| `pix_fmt` | `yuv420p` | output pixel format |
| `want_audio` | `true` | record audio at all |
| `audio_bitrate` | `96k` | audio bitrate |
| `audio_codec` | `aac` | `aac`, `libopus`, whatever ffmpeg has |
| `ffmpeg` | `ffmpeg` | path to ffmpeg if it is not on `PATH` |
| `source_timeout` | `10.0` | seconds without video before declaring the source gone |
| `reconnect_delay` | `5.0` | seconds between reconnect attempts |
| `extra_output_args` | `[]` | raw ffmpeg output arguments, for when you know better |

## What it costs

Measured on a 720p30 source with 96 kbps stereo audio, encoding AV1 on an RTX 4090:

| setting | total bitrate | per day | per year |
|---|---|---|---|
| cq 45 | 774 kbps | 7.8 GB | 2.8 TB |
| cq 55 | 369 kbps | 3.7 GB | 1.4 TB |
| cq 63 | 230 kbps | 2.3 GB | 0.8 TB |
| VBR capped 150k | 256 kbps | 2.6 GB | 0.9 TB |

At the default settings a 96 hour ring is about 10 GB and each commit is about 5 GB.
Scene complexity moves these numbers, so treat them as the right order of magnitude
rather than a promise.

## Field notes

Things that cost real time, written down so they cost you none.

**H.266 is a trap.** It is the newest and most efficient thing on paper, and it is
useless here. Mainline ffmpeg builds ship VVC decoders but no `libvvenc` encoder,
consumer GPUs do not encode it, software encoding runs far slower than realtime, and
almost nothing plays the result. AV1 gets you hardware encode and plays in every browser.

**AV1 has a bitrate floor and you cannot argue with it.** Asking for 80 kbps and asking
for 150 kbps produced files of identical size. Denoising the input first changed nothing.
Below roughly 250 kbps at 720p30 you are negotiating with a wall.

**NVENC on Ada cannot encode 4:2:2.** NDI decodes to `yuv422p`, and feeding that to
`hevc_nvenc` fails with "No capable devices found," which is a spectacular way of saying
"wrong pixel format." Set `-pix_fmt yuv420p` and the GPU it just told you did not exist
will encode happily.

**One thread cannot feed two ffmpeg inputs.** ffmpeg reads its inputs in whatever order
it needs to interleave them, so a single writer deadlocks the instant it blocks on video
while ffmpeg is waiting for audio. It will run fine for twenty seconds first, which is
exactly long enough to convince you it works. Each input gets its own thread here.

**Sources lie about their frame rate.** A camera claiming 30fps delivering 29.63 will
drift audio against video by half a second every thirty seconds if you believe the label.
Timestamp video by arrival and let ffmpeg reconcile to constant rate.

**The segment muxer mangles Opus.** Every segment decodes with one bad packet at the
start, losing about 20 ms. AAC segments cleanly, which is why it is the default even
though Opus is the better codec at this bitrate.

**A detached process still needs to escape the job object.** Windows terminals and task
runners put children in job objects that kill everything when the parent exits, which
makes "runs in the background" quietly false. `CREATE_BREAKAWAY_FROM_JOB` is the
difference between a daemon and a process that dies when you close the window.

**No console means ffmpeg makes its own.** A detached parent has no console to inherit,
so every child helpfully allocates one and flashes a blank black window on the desktop
at every segment rollover. `CREATE_NO_WINDOW` on the child fixes it.

## The other script

`ndi_capture.py` is the interactive half, for when you want one recording rather than
a surveillance apparatus.

```
python ndi_capture.py list                    # what is on the network
python ndi_capture.py record "Studio"         # via the recorder NDI Tools ships, SpeedHQ in a MOV
python ndi_capture.py encode "Studio" -t 60   # live transcode through ffmpeg
```

`record` hands off to Vizrt's own headless recorder. For full bandwidth NDI that is a
straight passthrough with no re-encode. For an HX source it is not: HX arrives as H.264
or HEVC, gets decoded by the NDI runtime, and gets written back out as SpeedHQ, which
inflated a 720p30 phone camera to 86 Mbps. `encode` exists because of that.

## Known rough edges

Being honest about what has and has not been exercised:

- The ring has not yet aged past 96 hours in testing, so the pruning path has run many
  times with nothing to delete. The deletion code itself is shared with commit, which has
  been exercised.
- `autostart on` writes the Startup entry correctly but the logon path is unverified.
- Windows reports an open segment as zero bytes until its writer lets go. That is stale
  directory metadata, not data loss. Killing ffmpeg outright left the in-progress segment
  readable with everything up to the moment it died.
- One source at a time. Running several is a matter of separate roots and separate
  daemons, which works but is not a feature so much as an absence of one.

## License

MIT. See [LICENSE](LICENSE).
