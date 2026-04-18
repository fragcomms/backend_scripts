import sys
import os
import json
import subprocess


def get_audio_tracks_info(mka_path):
  cmd = [
    "ffprobe",
    "-v",
    "error",
    "-select_streams",
    "a",
    "-show_entries",
    "stream=index:stream_tags=title",
    "-of",
    "json",
    mka_path,
  ]
  result = subprocess.run(cmd, capture_output=True, text=True)
  if result.returncode != 0:
    print(f"Error reading .mka file: {result.stderr}", file=sys.stderr)
    sys.exit(1)

  data = json.loads(result.stdout)
  track_info = {}

  for i, stream in enumerate(data.get("streams", [])):
    tags = stream.get("tags", {})
    title = tags.get("title", f"track_{i}")
    track_info[i] = title

  return track_info


def extract_track(mka_path, track_index, output_wav):
  cmd = [
    "ffmpeg",
    "-y",
    "-v",
    "error",
    "-fflags",
    "+discardcorrupt",
    "-i",
    mka_path,
    "-map",
    f"0:a:{track_index}",
    "-ar",
    "16000",
    "-ac",
    "1",
    "-c:a",
    "pcm_s16le",
    output_wav,
  ]
  try:
    subprocess.run(cmd, check=True)
    return True
  except subprocess.CalledProcessError:
    if os.path.exists(output_wav) and os.path.getsize(output_wav) > 44:
      return True
    return False
