import sys
import os
import json
import time
import gc
import torch
from config import MAP_GLOSSARY, GENERAL_CS2_TERMS, PHRASES_TO_BOOST
from utils_audio import get_audio_tracks_info
from engine_asr import ASREngine
from engine_llm import LLMEngine


def main(input_mka, map_name="Nuke"):
  total_start_time = time.time()

  track_info = get_audio_tracks_info(input_mka)
  print(f"Found {len(track_info)} audio tracks: {list(track_info.values())}")

  hotwords = MAP_GLOSSARY.get("Generic", []) + MAP_GLOSSARY.get(map_name, [])
  extended_hotwords = list(
    set(
      hotwords
      + GENERAL_CS2_TERMS
      + PHRASES_TO_BOOST
      + [w.lower() for w in hotwords + GENERAL_CS2_TERMS]
      + [w.upper() for w in hotwords + GENERAL_CS2_TERMS]
    )
  )

  # transcribe
  asr_engine = ASREngine(extended_hotwords)
  all_tracks_data = asr_engine.process_all_tracks(input_mka, track_info)

  # Clear VRAM between models
  del asr_engine
  gc.collect()
  torch.cuda.empty_cache()
  torch.cuda.reset_peak_memory_stats()
  torch.cuda.reset_accumulated_memory_stats()

  llm_engine = LLMEngine()
  all_tracks_data = llm_engine.correct_transcriptions(
    all_tracks_data, map_name, hotwords, GENERAL_CS2_TERMS
  )

  # Destroy vLLM properly and clean up PyTorch distributed process groups
  from vllm.distributed.parallel_state import destroy_model_parallel
  import torch.distributed as dist

  destroy_model_parallel()
  if dist.is_initialized():
    dist.destroy_process_group()

  print("\n[PHASE 3] Saving output JSONs...", flush=True)
  output_files = []

  # Safely extract directory and base filename
  base_dir = os.path.dirname(input_mka)
  base_name = os.path.splitext(os.path.basename(input_mka))[0]

  # --- DEBUG INFO ---
  print(f"[DEBUG] Raw input_mka path: '{input_mka}'", flush=True)
  print(f"[DEBUG] base_dir resolved to: '{base_dir}'", flush=True)
  print(f"[DEBUG] base_name resolved to: '{base_name}'", flush=True)
  print(
    f"[DEBUG] Tracks with transcription data: {list(all_tracks_data.keys())}",
    flush=True,
  )
  # ------------------

  for track_idx, track_title in track_info.items():
    if track_title in all_tracks_data:
      title = "".join(
        c for c in track_title if c.isalnum() or c in (" ", "_", "-")
      ).strip()

      # Build an absolute path
      output_json = os.path.join(base_dir, f"{base_name}_{title}.json")

      print(f"[DEBUG] Attempting to write JSON to: '{output_json}'", flush=True)

      final_output = {
        "discord_id": track_title,
        "segments": all_tracks_data[track_title],
      }

      try:
        with open(output_json, "w", encoding="utf-8") as f:
          json.dump(final_output, f, indent=2, ensure_ascii=False)

        # Verify it actually exists on the filesystem right after writing
        if os.path.exists(output_json):
          print(f"[DEBUG] SUCCESS: Verified file exists at '{output_json}'", flush=True)
        else:
          print(
            f"[DEBUG] ERROR: Python wrote the file, but OS says it doesn't exist at '{output_json}'",
            flush=True,
          )

      except Exception as e:
        print(f"[DEBUG] FATAL ERROR writing to '{output_json}': {e}", flush=True)

      output_files.append(os.path.abspath(output_json))
    else:
      print(
        f"[DEBUG] SKIPPING track '{track_title}': No transcription data found for this track.",
        flush=True,
      )

  total_time = time.time() - total_start_time
  minutes, seconds = divmod(total_time, 60)
  print(f"\nPipeline Complete! Total: {int(minutes)}m {seconds:.2f}s", flush=True)

  return output_files


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print(
      "Usage: python transcriber-para.py <path_to_audio.mka> [map_name]",
      file=sys.stderr,
    )
    sys.exit(1)

  input_file = os.path.abspath(sys.argv[1])
  map_context = sys.argv[2] if len(sys.argv) > 2 else "Nuke"

  try:
    # Run the pipeline and get the paths to the JSONs
    generated_files = main(input_file, map_context)
    print("\n---Completed---")

    # tell orchestrator we are done
    for filepath in generated_files:
      event = {
        "type": "transcribe_complete",
        "payload": {
          "filepath": filepath,
          "model_id": "1",  # should change to a different ID
          "original_audio": input_file,
        },
      }
      print(f"DATA_OUTPUT:{json.dumps(event)}", flush=True)

  except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
