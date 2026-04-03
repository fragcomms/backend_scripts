import sys
import os
import json
import subprocess
import gc
import re
import time
import threading
import concurrent.futures

import torch
import torchaudio
import nemo.collections.asr as nemo_asr
from omegaconf import open_dict, OmegaConf
from silero_vad import load_silero_vad, get_speech_timestamps, read_audio
from vllm import LLM, SamplingParams

# --- 1. GLOBALS & GLOSSARIES ---
MAP_GLOSSARY = {
    "Generic": [
        "CT-Spawn", "T-Spawn", "CT", "T", "Spawn", "A-Site", "B-Site",
        "A", "B"
    ],
    "Nuke": [
        "Silo", "Marshmallow", "Squeaky", "Vent", "Secret", "Unbreakable",
        "Garage", "Mini", "Heaven", "Rafters", "Blue Box", "Yellow",
        "Hut", "Radio", "Trophy", "Ramp", "Credit", "Africa", "Asia",
        "Tetris", "Rafters", "Outside", "Cross", "Single", "Double", "Hell",
        "Control", "Glaive"
    ],
    "Mirage": [
        "Palace", "Tetris", "Connector", "Jungle", "Ticket", "Catwalk",
        "Underpass", "Delpan", "Bench", "Firebox", "Default", "Window",
    ],
    "Inferno": [
        "Coffins", "Pit", "Apartments", "Boiler", "Banana", "Church",
        "Library", "Graveyard", "Ruins", "Top Mid",
    ],
}

GENERAL_CS2_TERMS = [
    "ping", "planting", "NT", "nice try", "fake", "plant", "defuse", "plan", "bomb",
    "pistols", "eco", "dead", "last one", "guns", "bought", "saving", "buy", "force",
    "CZ", "deagle", "dualies", "five-seven", "glock", "p2k", "p250", "r8", "tec9", "usp",
    "AK", "AUG", "AWP", "AVP", "famas", "auto", "galil", "A1S", "M4", "krieg", "SG", "scout",
    "mac10", "mp5", "mp7", "mp9", "pp", "p90", "ump",
    "mag7", "swag7", "nova", "sawed-off", "XM", "auto shotty", "m249", "negev",
    "bayonet", "bowie", "butterfly", "classic", "falchion", "flip", "gut", "huntsman", "karambit",
    "kukri", "m9", "navaja", "nomad", "paracord", "daggers", "skeleton", "stiletto", "survival",
    "talon", "ursus", "zeus", "knife", "dinked", "rat",
    "one HP", "lit", "low", "fifty", "raging", "rage",
    "one", "two", "three", "four", "all five", "all"
]

PHRASES_TO_BOOST = [
    #"one outside", "two outside", "on A", "on B", "last guy",
    #"still secret", "push outside", "going T-spawn",
]

CS2_PHONETIC_FIXES = {
    "Duke": "Nuke",
    "goaded": "goated",
    "hot": "hut",
    "happen": "heaven",
    "fruit's": "threw",
    "empty": "NT",
    "tea spawn": "T-spawn",
    "ex i'm": "XM",
    "pink": "ping",
    "thing": "ping",
    "too many": "two mini",
    "tea": "T",
    "sea tea": "CT",
    "see tea": "CT",
    "dinged": "dinked",
    "acres": "AKs",
    "swan": "one",
    "reaching": "raging"
}

LLM_MODEL_NAME = "Qwen/Qwen2.5-7B-Instruct-AWQ"

def get_audio_tracks_info(mka_path):
    cmd = [
        "ffprobe", "-v", "error", "-select_streams", "a",
        "-show_entries", "stream=index:stream_tags=title", "-of", "json", mka_path,
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
        "ffmpeg", "-y", "-v", "error", "-i", mka_path,
        "-map", f"0:a:{track_index}", "-ar", "16000", "-ac", "1", output_wav,
    ]
    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError:
        if os.path.exists(output_wav) and os.path.getsize(output_wav) > 44:
            return True
        return False

def generate_llm_prompt(transcript, map_name, valid_locations, general_terms):
    map_callouts = ", ".join(valid_locations)
    cs_terms = ", ".join(general_terms)
    
    return f"""<|im_start|>system
You are a Counter-Strike 2 Transcription Soft-Regex Filter. Your goal is to correct phonetic errors in "Current" using the provided Knowledge Base.

### KNOWLEDGE BASE:
Map: {map_name}
Callouts: {map_callouts}
Terms: {cs_terms}

### CRITICAL RULES:
1. DO NOT TOUCH VALID WORDS. If a word is already a valid English word, a valid CS2 term, or a valid Map Callout, LEAVE IT ALONE. If "Current" is regular conversation (personal life, work, non-gaming), return it 100% UNCHANGED.
2. SINGLE LETTER CALLOUTS. Letters like "A", "B", "T", and "CT" are locations or teams. Capitalize them. Do NOT convert valid English words (like "ass", "as", or "at") into single-letter callouts.
3. ASR STUTTERS & FRAGMENTS. The audio transcriber frequently makes mistakes by cutting words into single meaningless letters (e.g., "w", "f", "m") or fragments like "Und". DO NOT turn these into CS2 callouts. Leave them exactly as they are. "A", "B", "CT", and "T" are the ONLY valid letter callouts.
4. NO CENSORSHIP. Do not replace profanity (e.g., "fuck", "shit", "ass") with map callouts. Leave profanity exactly as it is.
5. NO ADDITIONAL WORDS. Do not add new words. Sentences that are short should stay short.
6. Output ONLY the corrected string. No quotes, labels, or explanations.

### EXAMPLES OF WHAT TO FIX:
Current: "He is pushing see tea."
Output: He is pushing CT-spawn.

Current: "Too many"
Output: Two mini.

Current: "One on a."
Output: One on A.

Current: "One outside."
Output: One outside.

Current: "Side Go Kill Saint, please!"
Output: Site go kill site, please!

Current: "A Site planning"
Output: A Site planting

Current: "Oh anti bro, anti"
Output: Oh NT bro, NT

### EXAMPLES OF WHAT NOT TO CHANGE (CRITICAL):
Current: "Dude, he's on B"
Output: Dude, he's on B

Current: "Und"
Output: Und

Current: "They're saving it in a 12-4."
Output: They're saving it in a 12-4.

Current: "On ass."
Output: On ass.

Current: "He did w he went garage so fast."
Output: He did w he went garage so fast.

Current: "We need an anti strat."
Output: We need an anti strat.

### ACTUAL TASK:
Current: "{transcript}"
Output:<|im_end|>
<|im_start|>assistant
"""

def main(input_mka, map_name="Nuke"):
    total_start_time = time.time()
    track_info = get_audio_tracks_info(input_mka)
    track_count = len(track_info)
    print(f"Found {track_count} audio tracks: {list(track_info.values())}", file=sys.stdout)
    
    hotwords = MAP_GLOSSARY.get("Generic", []) + MAP_GLOSSARY.get(map_name, [])
    extended_hotwords = list(set(
        hotwords + GENERAL_CS2_TERMS + PHRASES_TO_BOOST +
        [w.lower() for w in hotwords + GENERAL_CS2_TERMS] +
        [w.upper() for w in hotwords + GENERAL_CS2_TERMS]
    ))

    all_tracks_data = {}

    # --- PHASE 1: AUDIO EXTRACTION (PARAKEET) ---
    print("\n[PHASE 1] Loading Parakeet ASR...", file=sys.stdout)
    phase1_start = time.time()
    asr_model = nemo_asr.models.ASRModel.from_pretrained("nvidia/parakeet-tdt-0.6b-v3")
    decoding_cfg = asr_model.cfg.decoding
    
    with open_dict(decoding_cfg):
        decoding_cfg.strategy = "malsd_batch"
        if "malsd_batch" not in decoding_cfg:
            decoding_cfg.malsd_batch = OmegaConf.create({})
        decoding_cfg.malsd_batch.beam_size = 4
        if "boosting_tree" not in decoding_cfg.malsd_batch:
            decoding_cfg.malsd_batch.boosting_tree = OmegaConf.create({})
        decoding_cfg.malsd_batch.boosting_tree.key_phrases_list = extended_hotwords
        decoding_cfg.malsd_batch.boosting_tree.context_score = 5.0
        decoding_cfg.malsd_batch.boosting_tree.use_triton = False
        
    asr_model.change_decoding_strategy(decoding_cfg)
    thread_local = threading.local()

    def get_local_vad():
        if not hasattr(thread_local, "vad_model"):
            thread_local.vad_model = load_silero_vad()
        return thread_local.vad_model

    def prep_audio_track(track_idx):
        temp_wav = f"/dev/shm/temp_track_{track_idx}.wav"
        if not extract_track(input_mka, track_idx, temp_wav):
            return []
        vad = get_local_vad()
        wav_tensor = read_audio(temp_wav)
        speech_timestamps = get_speech_timestamps(
            wav_tensor, 
            vad, 
            sampling_rate=16000, 
            threshold=0.5,                # Catch quieter, "mumbled" callouts
            speech_pad_ms=400,             # Add more buffer before/after speech so first/last letters aren't clipped
            min_silence_duration_ms=600,
            min_speech_duration_ms=100
            
        )
        track_chunks = []
        for idx, stamp in enumerate(speech_timestamps):
            start_sec = round(stamp["start"] / 16000.0, 2)
            end_sec = round(stamp["end"] / 16000.0, 2)
            chunk = wav_tensor[stamp["start"] : stamp["end"]]
            temp_chunk_wav = f"/dev/shm/chunk_temp_{track_idx}_{idx}.wav"
            torchaudio.save(temp_chunk_wav, chunk.unsqueeze(0), 16000)
            track_chunks.append({
                "track_idx": track_idx, "start": start_sec, 
                "end": end_sec, "filepath": temp_chunk_wav
            })
        if os.path.exists(temp_wav):
            os.remove(temp_wav)
        return track_chunks

    optimal_threads = 6
    all_chunks_info = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=optimal_threads) as executor:
        futures = [executor.submit(prep_audio_track, i) for i in range(track_count)]
        for future in concurrent.futures.as_completed(futures):
            all_chunks_info.extend(future.result())

    all_chunks_info = sorted(all_chunks_info, key=lambda x: (x["track_idx"], x["start"]))
    chunk_files = [c["filepath"] for c in all_chunks_info]
    
    if chunk_files:
        with torch.inference_mode():
            results = asr_model.transcribe(chunk_files, batch_size=24)
            transcriptions = results[0] if isinstance(results, tuple) else results
            
        for meta, hyp in zip(all_chunks_info, transcriptions):
            best_hyp = hyp[0] if isinstance(hyp, list) and len(hyp) > 0 else hyp
            text = best_hyp.text if hasattr(best_hyp, "text") else str(best_hyp)
            if text and text.strip():
                track_title = track_info.get(meta['track_idx'], f"track_{meta['track_idx']}")
                all_tracks_data.setdefault(track_title, []).append({
                    "start": meta["start"], "end": meta["end"],
                    "raw_text": text.strip(), "clean_text": "", "locations": []
                })
            if os.path.exists(meta["filepath"]):
                os.remove(meta["filepath"])

    # Clear VRAM for LLM
    del asr_model
    gc.collect()
    torch.cuda.empty_cache()
    torch.cuda.reset_peak_memory_stats()
    torch.cuda.reset_accumulated_memory_stats()
    
    # --- PHASE 2: TEXT CORRECTION (QWEN) ---
    print("\n[PHASE 2] Loading Qwen 2.5 via vLLM...", file=sys.stdout)
    llm = LLM(
        model=LLM_MODEL_NAME, 
        gpu_memory_utilization=0.95, 
        quantization="awq_marlin", 
        max_num_seqs=128, 
        max_model_len=1024, 
        enable_prefix_caching=True,
        dtype="half", 
        disable_log_stats=True,
    )

    sampling_params = SamplingParams(
        temperature=0.05,   # 0.0 is too rigid; 0.2 allows for phonetic "leaps"
        max_tokens=100,
        top_p=0.9,        # Helps pick the most likely "gaming" term
        stop=["\n", "</s>", "<|im_end|>", "<|endoftext|>"], 
    )

    flat_prompts = []
    flat_refs = []

    for track_id, segments in all_tracks_data.items():
        for seg in segments:
            current_text = seg["raw_text"]
            
            # 1. APPLY MANUAL FIXES FIRST!
            for bad, good in CS2_PHONETIC_FIXES.items():
                current_text = re.sub(rf'\b{re.escape(bad)}\b', good, current_text, flags=re.IGNORECASE)
            
            # Update the raw_text so the LLM gets the pre-cleaned version
            seg["raw_text"] = current_text 
            
            # 2. THEN SEND TO LLM
            flat_prompts.append(generate_llm_prompt(current_text, map_name, hotwords, GENERAL_CS2_TERMS))
            flat_refs.append(seg)

    if flat_prompts:
        outputs = llm.generate(flat_prompts, sampling_params)
        for output, seg_ref in zip(outputs, flat_refs):
            # Take only the first line of the output
            raw_output = output.outputs[0].text.strip()
            clean_text = raw_output.split('\n')[0] 
            
            # Remove any leading "Output: " or "Fixed: "
            clean_text = re.sub(r'^(Output|Fixed|Corrected):\s*', '', clean_text, flags=re.IGNORECASE)
            clean_text = clean_text.strip('"').strip("'").strip()

            original_text = seg_ref["raw_text"]
            
            # Safety: If LLM returned garbage or a generic "No change" message
            if len(clean_text) < 1 or "no correction" in clean_text.lower():
                clean_text = original_text

            seg_ref["clean_text"] = clean_text
            seg_ref["changed"] = (clean_text.lower() != original_text.lower())

    # --- PHASE 3: SAVE JSONS ---
    print("\n[PHASE 3] Saving output JSONs...", file=sys.stdout)
    for track_idx, track_title in track_info.items():
        if track_title in all_tracks_data:
            title = "".join(c for c in track_title if c.isalnum() or c in (' ', '_', '-')).strip()
            output_json = input_mka.replace(".mka", f"_{title}.json")
            final_output = {"discord_id": track_title, "segments": all_tracks_data[track_title]}
            with open(output_json, "w", encoding="utf-8") as f:
                json.dump(final_output, f, indent=2, ensure_ascii=False)
                
    from vllm.distributed.parallel_state import destroy_model_parallel
    destroy_model_parallel()

    total_time = time.time() - total_start_time
    minutes, seconds = divmod(total_time, 60)
    print(f"\nPipeline Complete! Total: {int(minutes)}m {seconds:.2f}s")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python transcriber.py <path_to_audio.mka> [map_name]", file=sys.stderr)
        sys.exit(1)
    
    input_file = os.path.abspath(sys.argv[1])
    map_context = sys.argv[2] if len(sys.argv) > 2 else "Nuke"
    main(input_file, map_context)
