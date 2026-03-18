import warnings
import logging

warnings.filterwarnings("ignore", category=UserWarning)

import gc
import torch
import os
import subprocess
import json
import sys
import soundfile as sf  

logging.getLogger("nemo_logger").setLevel(logging.ERROR)

from nemo.collections.asr.models import EncDecMultiTaskModel

# Configuration
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MODEL_TYPE = "nvidia/canary-1b-v2"
OUTPUT_DIR = None

print("Loading Silero VAD...", file=sys.stdout)
vad_model, vad_utils = torch.hub.load(repo_or_dir='snakers4/silero-vad',
                                      model='silero_vad',
                                      trust_repo=True)
get_speech_timestamps = vad_utils[0]
read_audio = vad_utils[2]


def get_output_dir(dir, audio_path):
  if dir:
    target_dir = os.path.abspath(dir)
  else:
    target_dir = os.path.dirname(os.path.abspath(audio_path))

  os.makedirs(target_dir, exist_ok=True)
  return target_dir


def get_audio_track_count(filepath):
  try:
    cmd = [
      "ffprobe", "-v", "error", "-select_streams", "a",
      "-show_entries", "stream=index", "-of", "csv=p=0", filepath,
    ]
    output = subprocess.check_output(cmd, text=True).strip()
    if not output: return 0
    return len(output.splitlines())
  except subprocess.CalledProcessError:
    print("Error: Could not determine audio tracks. Is ffprobe installed?", file=sys.stderr)
    return 0


def get_track_title(filepath, track_index):
  try:
    cmd = [
      "ffprobe", "-v", "error", "-select_streams", f"a:{track_index}",
      "-show_entries", "stream_tags=title", "-of", "csv=p=0", filepath,
    ]
    output = subprocess.check_output(cmd, text=True).strip()
    return output if output else None
  except Exception:
    return None


def extract_track(input_path, track_index, output_wav):
  cmd = [
    "ffmpeg", "-y", "-v", "error", "-i", input_path,
    "-map", f"0:a:{track_index}", "-ac", "1", "-ar", "16000", output_wav,
  ]
  subprocess.run(cmd, check=True)


def process_audio(audio_path, prompt=None):
  audio_file = os.path.abspath(audio_path)
  if not os.path.exists(audio_file):
    raise FileNotFoundError(f"File not found at {audio_file}")
    
  num_tracks = get_audio_track_count(audio_file)
  print(f"Processing: {audio_file}", file=sys.stdout)

  if num_tracks == 0:
    return []

  save_dir = get_output_dir(OUTPUT_DIR, audio_file)
  base_audio_name = os.path.splitext(os.path.basename(audio_file))[0]
  
  print(f"Loading NeMo Model: {MODEL_TYPE}...", file=sys.stdout)
  
  # load model into ram first
  model = EncDecMultiTaskModel.from_pretrained(model_name=MODEL_TYPE, map_location="cpu")
  model.eval()

  if DEVICE == "cuda":
    # float32 is computationally expensive, so we use bfloat16 because its cheaper on vram usage
    model = model.bfloat16()
    model = model.cuda()

  decode_cfg = model.cfg.decoding
  decode_cfg.beam.beam_size = 8
  decode_cfg.beam.len_pen = 1.0
  model.change_decoding_strategy(decode_cfg)

  output_files = []

  for i in range(num_tracks):
    user_id = get_track_title(audio_file, i)
    track_identifier = user_id if user_id else f"track_{i + 1}"
    print(f"\nProcessing Track {i + 1}/{num_tracks} (ID: {track_identifier})", file=sys.stdout)

    temp_wav = os.path.join(save_dir, f"temp_{base_audio_name}_{track_identifier}.wav")
    output_file = os.path.join(save_dir, f"{base_audio_name}_{track_identifier}.json")
    json_data = {"discord_id": track_identifier, "segments": []}

    try:
      extract_track(audio_file, i, temp_wav)

      # silero vad to smart slice
      wav = read_audio(temp_wav)
      speech_timestamps = get_speech_timestamps(
        wav, 
        vad_model, 
        sampling_rate=16000,
        threshold=0.75,                
        min_speech_duration_ms=250,   
        min_silence_duration_ms=500,  
        speech_pad_ms=150             
      )

      audio_data, sample_rate = sf.read(temp_wav)
      
      batch_paths = []
      batch_offsets = []
      chunk_idx = 0

      # slice the audio into separate portions so my vram doesn't die
      for stamp in speech_timestamps:
        seg_start = stamp['start']
        seg_end = stamp['end']
        max_samples = int(60.0 * sample_rate)

        while seg_start < seg_end:
          chunk_end = min(seg_start + max_samples, seg_end)
          time_offset = seg_start / sample_rate
          
          chunk_data = audio_data[seg_start:chunk_end]
          chunk_path = os.path.join(save_dir, f"chunk_{chunk_idx}_{track_identifier}.wav")
          sf.write(chunk_path, chunk_data, sample_rate)
          
          batch_paths.append(chunk_path)
          batch_offsets.append((time_offset, chunk_end / sample_rate))
          
          seg_start = chunk_end
          chunk_idx += 1

      # transcribe
      if batch_paths:
        print(f"  -> Transcribing {len(batch_paths)} segments in true batches...", file=sys.stdout)
        
        with torch.autocast(device_type="cuda", dtype=torch.bfloat16):
          outputs = model.transcribe(
            audio=batch_paths, 
            batch_size=8, 
            task='asr', source_lang='en', target_lang='en', pnc='no', timestamps=False
          )

        # 5. Map the outputs back to their saved timestamps
        for idx, out in enumerate(outputs):
          if out:
            text = out.text.strip()
            if text:
              start_time_sec, end_time_sec = batch_offsets[idx]
              
              json_data["segments"].append({
                "start": round(start_time_sec, 2),
                "end": round(end_time_sec, 2),
                "text": text,
              })

      # Clean up all the mini-chunks to save disk space
      for path in batch_paths:
        if os.path.exists(path):
          os.remove(path)

      # Write final JSON
      print(f"Writing stitched output to {os.path.basename(output_file)}", file=sys.stdout)
      with open(output_file, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

      output_files.append(output_file)

    except Exception as e:
      print(f"Error processing track {i + 1}: {e}", file=sys.stderr)
    finally:
      if os.path.exists(temp_wav):
        os.remove(temp_wav)

  del model
  gc.collect()
  torch.cuda.empty_cache()

  return output_files


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