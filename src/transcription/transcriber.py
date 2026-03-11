import whisperx
import gc
import torch
import os
import subprocess
import json
import sys

# config - TODO: identify if theres anything else required or "nice to haves" for configuration
DEVICE = "cuda"
BATCH_SIZE = 4
COMPUTE_TYPE = "float16"
MODEL_TYPE = "large-v3"
OUTPUT_DIR = None


def get_output_dir(dir, audio_path):
  if dir:
    target_dir = os.path.abspath(dir)
  else:
    target_dir = os.path.dirname(os.path.abspath(audio_path))

  os.makedirs(target_dir, exist_ok=True)
  return target_dir


def get_audio_track_count(filepath):
  """Uses ffprobe to count number of audio streams."""
  try:
    cmd = [
      "ffprobe",
      "-v",
      "error",
      "-select_streams",
      "a",
      "-show_entries",
      "stream=index",
      "-of",
      "csv=p=0",
      filepath,
    ]
    output = subprocess.check_output(cmd, text=True).strip()
    if not output:
      return 0
    return len(output.splitlines())
  except subprocess.CalledProcessError:
    print(
      "Error: Could not determine audio tracks. Is ffprobe installed?", file=sys.stderr
    )
    return


def get_track_title(filepath, track_index):
  """Uses ffprobe to extract the 'title' metadata (User ID) from a specific track."""
  try:
    cmd = [
      "ffprobe",
      "-v",
      "error",
      "-select_streams",
      f"a:{track_index}",
      "-show_entries",
      "stream_tags=title",
      "-of",
      "csv=p=0",
      filepath,
    ]
    output = subprocess.check_output(cmd, text=True).strip()
    # Basic sanitization to ensure valid filename (alphanumeric + underscores/dashes)
    # Discord IDs are just numbers, but this is safe fallback
    return output if output else None
  except Exception:
    return None


def extract_track(input_path, track_index, output_wav):
  """Extracts a specific audio track to a temp WAV file (16kHz mono)."""
  cmd = [
    "ffmpeg",
    "-y",
    "-v",
    "error",
    "-i",
    input_path,
    "-map",
    f"0:a:{track_index}",
    "-ac",
    "1",  # mono
    "-ar",
    "16000",
    output_wav,
  ]
  subprocess.run(cmd, check=True)


def process_audio(audio_path, prompt=None):
  # error checking
  audio_file = os.path.abspath(audio_path)
  if not os.path.exists(audio_file):
    raise FileNotFoundError(f"File not found at {audio_file}")
  num_tracks = get_audio_track_count(audio_file)
  print(f"Processing: {audio_file}", file=sys.stdout)

  if num_tracks == 0:
    return []

  save_dir = get_output_dir(OUTPUT_DIR, audio_file)
  base_audio_name = os.path.splitext(os.path.basename(audio_file))[0]
  print(f"Output directory set to: {save_dir}", file=sys.stdout)

  vocab = "Rush B, CT, T spawn, lit, one tap, eco, drop, awp, mid, rotate, flank, default, plant, defuse, peek, flash, smoke."

  # set up for transcribing
  asr_options = {
    "initial_prompt": prompt if prompt else vocab,
    "condition_on_previous_text": False,
    "beam_size": 5,
    "patience": 2.0,
    "temperatures": [0.0, 0.2, 0.4],
  }

  vad_options = {
    "vad_onset": 0.05,
    "vad_offset": 0.05,
    "min_duration_on": 0.1,
    "min_duration_off": 0.2,
  }

  model = whisperx.load_model(
    MODEL_TYPE,
    DEVICE,
    compute_type=COMPUTE_TYPE,
    vad_method="silero",  # required because pyannote is broken
    asr_options=asr_options,
    vad_options=vad_options,
  )
  output_files = []

  for i in range(num_tracks):
    user_id = get_track_title(audio_file, i)
    track_identifier = user_id if user_id else f"track_{i + 1}"
    print(
      f"Processing Track {i + 1}/{num_tracks} (ID: {track_identifier})",
      file=sys.stdout,
    )

    # Create temp file for this track
    temp_wav = os.path.join(save_dir, f"temp_{base_audio_name}_{track_identifier}.wav")
    output_file = os.path.join(save_dir, f"{base_audio_name}_{track_identifier}.json")

    try:
      # Extract specific track
      extract_track(audio_file, i, temp_wav)

      # Load audio
      # using an analogy - load the gun
      audio = whisperx.load_audio(temp_wav)

      # Transcribe - shoot the gun
      # forcing the language to english for now because
      # the majority of comms are in english
      result = model.transcribe(audio, batch_size=BATCH_SIZE, language="en")

      # Align - inspect the aftermath and readjust shooting angle
      # We load/unload align model per track because language might differ per track (highly unlikely)
      print(f"Aligning Track {i + 1} ({result['language']})", file=sys.stdout)
      model_a, metadata = whisperx.load_align_model(
        language_code=result["language"], device=DEVICE
      )
      result = whisperx.align(
        result["segments"],
        model_a,
        metadata,
        audio,
        DEVICE,
        return_char_alignments=False,
      )

      # Cleanup Align Model to save vram
      gc.collect()
      torch.cuda.empty_cache()
      del model_a

      print(f"Writing to {os.path.basename(output_file)}", file=sys.stdout)

      json_data = {"discord_id": track_identifier, "segments": []}

      for segment in result["segments"]:
        json_data["segments"].append(
          {
            "start": round(segment["start"], 2),
            "end": round(segment["end"], 2),
            "text": segment["text"].strip(),
          }
        )

      with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

      output_files.append(output_file)

    except Exception as e:
      print(f"Error processing track {i + 1}: {e}", sys.stderr)
    finally:
      # Clean up temp wav
      if os.path.exists(temp_wav):
        os.remove(temp_wav)

  gc.collect()
  torch.cuda.empty_cache()
  del model

  return output_files


# main function used for debugging
def main():
  if len(sys.argv) < 2:
    print("Usage: python transcriber.py <audio_path> [prompt]", sys.stderr)
    sys.exit(1)

  audio_path = os.path.abspath(sys.argv[1])
  prompt = sys.argv[2] if len(sys.argv) > 2 else None

  try:
    files = process_audio(audio_path, prompt)
    print("\n---Completed---")
    for filepath in files:
      event = {
        "type": "transcribe_complete",
        "payload": {
          "filepath": filepath,
          "model_id": "1",
          "original_audio": audio_path,
        },
      }
      print(f"DATA_OUTPUT:{json.dumps(event)}", flush=True)
  except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
  main()
