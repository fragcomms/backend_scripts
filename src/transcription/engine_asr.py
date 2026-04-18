# engine_asr.py
import os
import warnings

# =====================================================================
# 1. THE PRE-EMPTIVE STRIKE (MUST BE AT THE ABSOLUTE TOP)
# We must silence the loggers before Python even imports NeMo,
# otherwise NeMo will initialize its noisy default loggers instantly.
# =====================================================================
os.environ["NEMO_LOG_LEVEL"] = "ERROR"  # Kills NeMo's config wall of text
os.environ["FFMPEG_LOG_LEVEL"] = "quiet"  # Kills "[opus] Error parsing" warnings
warnings.filterwarnings("ignore")  # Kills PyTorch Lightning deprecation warnings

# =====================================================================
# 2. SAFE TO IMPORT HEAVY LIBRARIES
# =====================================================================
import threading
import concurrent.futures
import torch
import torchaudio

from nemo.utils import logging as nemo_logging
import nemo.collections.asr as nemo_asr
from omegaconf import open_dict, OmegaConf
from silero_vad import load_silero_vad, get_speech_timestamps, read_audio
from config import ASR_MODEL_NAME
from utils_audio import extract_track


class ASREngine:
  def __init__(self, extended_hotwords):
    # Double-tap the logger just to be absolutely certain
    nemo_logging.setLevel(nemo_logging.ERROR)

    print("\n[PHASE 1] Loading Parakeet ASR...")
    self.model = nemo_asr.models.ASRModel.from_pretrained(ASR_MODEL_NAME)

    # Cast to FP16 (Half-Precision): Halves VRAM usage and doubles compute speed
    self.model = self.model.to("cuda").half()
    self._configure_decoding(extended_hotwords)

    # Thread-local storage ensures our VAD model doesn't collide across concurrent workers
    self.thread_local = threading.local()

  def _configure_decoding(self, extended_hotwords):
    """Injects our CS2 dictionary into the ASR's decoding tree to bias it towards gaming terms."""
    decoding_cfg = self.model.cfg.decoding
    with open_dict(decoding_cfg):
      decoding_cfg.strategy = "malsd_batch"
      if "malsd_batch" not in decoding_cfg:
        decoding_cfg.malsd_batch = OmegaConf.create({})
      decoding_cfg.malsd_batch.beam_size = 4
      if "boosting_tree" not in decoding_cfg.malsd_batch:
        decoding_cfg.malsd_batch.boosting_tree = OmegaConf.create({})

      decoding_cfg.malsd_batch.boosting_tree.key_phrases_list = extended_hotwords
      decoding_cfg.malsd_batch.boosting_tree.context_score = (
        5.0  # 5x weight for CS2 phrases
      )
      decoding_cfg.malsd_batch.boosting_tree.use_triton = False

      # Disable dataloader bottlenecks (unnecessary for short, in-memory audio chunks)
      decoding_cfg.pretokenize = False
      decoding_cfg.use_bucketing = False
    self.model.change_decoding_strategy(decoding_cfg)

  def _get_local_vad(self):
    """Singleton pattern per-thread: Silero VAD isn't thread-safe, so each worker gets its own instance."""
    if not hasattr(self.thread_local, "vad_model"):
      self.thread_local.vad_model = load_silero_vad()
    return self.thread_local.vad_model

  def prep_audio_track(self, input_mka, track_idx):
    # Use /dev/shm (Linux RAM disk) to entirely bypass physical SSD read/write lag
    temp_wav = f"/dev/shm/temp_track_{track_idx}.wav"
    if not extract_track(input_mka, track_idx, temp_wav):
      return []

    vad = self._get_local_vad()
    wav_tensor = read_audio(temp_wav)

    # speech_pad_ms=400 acts as a buffer so the ASR doesn't clip the first/last letters of quick callouts
    speech_timestamps = get_speech_timestamps(
      wav_tensor,
      vad,
      sampling_rate=16000,
      threshold=0.5,
      speech_pad_ms=400,
      min_silence_duration_ms=600,
      min_speech_duration_ms=100,
    )

    track_chunks = []
    for idx, stamp in enumerate(speech_timestamps):
      start_sec = round(stamp["start"] / 16000.0, 2)
      end_sec = round(stamp["end"] / 16000.0, 2)
      chunk = wav_tensor[stamp["start"] : stamp["end"]]

      # Save sliced chunks to RAM disk to feed into NeMo later
      temp_chunk_wav = f"/dev/shm/chunk_temp_{track_idx}_{idx}.wav"
      torchaudio.save(temp_chunk_wav, chunk.unsqueeze(0), 16000)
      track_chunks.append(
        {
          "track_idx": track_idx,
          "start": start_sec,
          "end": end_sec,
          "filepath": temp_chunk_wav,
        }
      )

    if os.path.exists(temp_wav):
      os.remove(temp_wav)
    return track_chunks

  def process_all_tracks(self, input_mka, track_info):
    """
    Architecture:
    Phase 1a (CPU): Use concurrent threads to slice all audio simultaneously using VAD.
    Phase 1b (GPU): Batch all sliced audio together and push through NeMo in one massive pass.
    """
    all_chunks_info = []

    # Phase 1a: CPU-bound VAD processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
      futures = [
        executor.submit(self.prep_audio_track, input_mka, i)
        for i in range(len(track_info))
      ]
      for future in concurrent.futures.as_completed(futures):
        all_chunks_info.extend(future.result())

    all_chunks_info = sorted(
      all_chunks_info, key=lambda x: (x["track_idx"], x["start"])
    )
    chunk_files = [c["filepath"] for c in all_chunks_info]
    all_tracks_data = {}

    if chunk_files:
      # Phase 1b: GPU-bound transcription
      with torch.inference_mode():
        # batch_size=128 maxes out GPU throughput
        results = self.model.transcribe(chunk_files, batch_size=16, num_workers=6)
        transcriptions = results[0] if isinstance(results, tuple) else results

      # Mapping results back to Discord IDs
      for meta, hyp in zip(all_chunks_info, transcriptions):
        best_hyp = hyp[0] if isinstance(hyp, list) and len(hyp) > 0 else hyp
        text = best_hyp.text if hasattr(best_hyp, "text") else str(best_hyp)
        if text and text.strip():
          track_title = track_info.get(meta["track_idx"], f"track_{meta['track_idx']}")
          all_tracks_data.setdefault(track_title, []).append(
            {
              "start": meta["start"],
              "end": meta["end"],
              "raw_text": text.strip(),
              "clean_text": "",
              "locations": [],
            }
          )
        # Cleanup RAM disk
        if os.path.exists(meta["filepath"]):
          os.remove(meta["filepath"])

    return all_tracks_data
