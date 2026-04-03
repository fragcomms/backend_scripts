# engine_llm.py
import re
from vllm import LLM, SamplingParams
from config import LLM_MODEL_NAME, CS2_PHONETIC_FIXES
import os

# gloo bypass
os.environ["VLLM_WORKER_MULTIPROC_METHOD"] = "spawn"
os.environ["VLLM_HOST_IP"] = "127.0.0.1"
os.environ["GLOO_SOCKET_IFNAME"] = "lo"
os.environ["NCCL_SOCKET_IFNAME"] = "lo"


class LLMEngine:
  def __init__(self):
    print("\n[PHASE 2] Loading Qwen 2.5 via vLLM...")
    self.llm = LLM(
      model=LLM_MODEL_NAME,
      gpu_memory_utilization=0.95,
      quantization="awq_marlin",
      max_num_seqs=128,
      max_model_len=1024,
      dtype="half",
      disable_log_stats=True,
      enforce_eager=True,  # according to AI charts are not useful for one-shot prompts
    )

    self.sampling_params = SamplingParams(
      temperature=0.05,  # 0.1 too much creativeness
      max_tokens=100,
      top_p=0.9,
      stop=["\n", "</s>", "<|im_end|>", "<|endoftext|>"],
    )

  def generate_prompt(self, transcript, map_name, valid_locations, general_terms):
    map_callouts = ", ".join(valid_locations)
    cs_terms = ", ".join(general_terms)
    return f"""<|im_start|>system
You are a Counter-Strike 2 Transcription Soft-Regex Filter. Your goal is to correct phonetic errors in "Current" using the provided Knowledge Base.

### KNOWLEDGE BASE:
Map: {map_name}
Callouts: {map_callouts}
Terms: {cs_terms}

### CRITICAL RULES:
1. DO NOT TOUCH VALID WORDS. If a word is already a valid English word, a valid CS2 term, or a valid Map Callout, LEAVE IT ALONE. If "Current" is regular conversation, return it 100% UNCHANGED.
2. SINGLE LETTER CALLOUTS. Letters like "A", "B", "T", and "CT" are locations or teams. Capitalize them. Do NOT convert valid English words into single-letter callouts.
3. ASR STUTTERS & FRAGMENTS. DO NOT turn fragments (e.g., "w", "f", "Und") into CS2 callouts. Leave them exactly as they are.
4. NO CENSORSHIP. Leave profanity exactly as it is.
5. NO ADDITIONAL WORDS. Sentences that are short should stay short.
6. Output ONLY the corrected string. No quotes, labels, or explanations.

### ACTUAL TASK:
Current: "{transcript}"
Output:<|im_end|>
<|im_start|>assistant
"""

  def correct_transcriptions(self, all_tracks_data, map_name, hotwords, general_terms):
    flat_prompts = []
    flat_refs = []

    # Flatten the nested dictionary into a single 1D list so we can feed it to the GPU all at once
    for track_id, segments in all_tracks_data.items():
      for seg in segments:
        current_text = seg["raw_text"]

        # fix the phonetic issues first before feeding it into the LLM
        for bad, good in CS2_PHONETIC_FIXES.items():
          current_text = re.sub(
            rf"\b{re.escape(bad)}\b", good, current_text, flags=re.IGNORECASE
          )

        seg["raw_text"] = current_text
        flat_prompts.append(
          self.generate_prompt(current_text, map_name, hotwords, general_terms)
        )
        flat_refs.append(seg)

    if flat_prompts:
      outputs = self.llm.generate(flat_prompts, self.sampling_params)

      for output, seg_ref in zip(outputs, flat_refs):
        raw_output = output.outputs[0].text.strip()
        clean_text = raw_output.split("\n")[0]

        clean_text = re.sub(
          r"^(Output|Fixed|Corrected|Current):\s*", "", clean_text, flags=re.IGNORECASE
        )
        clean_text = clean_text.strip('"').strip("'").strip()

        original_text = seg_ref["raw_text"]

        # fallback
        if len(clean_text) < 1 or "no correction" in clean_text.lower():
          clean_text = original_text

        seg_ref["clean_text"] = clean_text
        seg_ref["changed"] = clean_text.lower() != original_text.lower()

    return all_tracks_data
