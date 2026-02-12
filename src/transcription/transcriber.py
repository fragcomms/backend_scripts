import argparse
import whisperx
import gc
import torch
import os
import subprocess

# config - TODO: identify if theres anything else required or "nice to haves" for configuration
DEVICE = "cuda"
BATCH_SIZE = 4
COMPUTE_TYPE = "float16"
MODEL_TYPE = "large-v3"

def get_audio_track_count(file_path):
    """Uses ffprobe to count number of audio streams."""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "a",
            "-show_entries", "stream=index",
            "-of", "csv=p=0",
            file_path
        ]
        output = subprocess.check_output(cmd, text=True).strip()
        if not output:
            return 0
        return len(output.splitlines())
    except subprocess.CalledProcessError:
        print("Error: Could not determine audio tracks. Is ffprobe installed?")
        return
      
def get_track_title(file_path, track_index):
    """Uses ffprobe to extract the 'title' metadata (User ID) from a specific track."""
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", f"a:{track_index}",
            "-show_entries", "stream_tags=title",
            "-of", "csv=p=0",
            file_path
        ]
        output = subprocess.check_output(cmd, text=True).strip()
        # Basic sanitization to ensure valid filename (alphanumeric + underscores/dashes)
        # Discord IDs are just numbers, but this is safe fallback
        return None
    except Exception:
        return None

def extract_track(input_path, track_index, output_wav):
    """Extracts a specific audio track to a temp WAV file (16kHz mono)."""
    cmd = [
        "ffmpeg", "-y", "-v", "error",
        "-i", input_path,
        "-map", f"0:a:{track_index}",
        "-ac", "1", # mono
        "-ar", "16000",
        output_wav
    ]
    subprocess.run(cmd, check=True)
    
def process_audio(audio_path, prompt=None):
    # error checking
    audio_file = os.path.abspath(audio_path)
    if not os.path.exists(audio_file):
        raise FileNotFoundError(f"File not found at {audio_file}")
    num_tracks = get_audio_track_count(audio_file)
    print(f"Processing: {audio_file}")
    
    if num_tracks == 0:
        return []
        
    # set up for transcribing
    asr_options = {
        "initial_prompt": prompt
    } if prompt else None
    
    vad_options = {
        "vad_onset": 0.5,
        "vad_offset": 0.5
    }
    
    model = whisperx.load_model(
        MODEL_TYPE,
        DEVICE,
        compute_type=COMPUTE_TYPE,
        vad_method="silero", # required because pyannote is broken
        asr_options=asr_options,
        vad_options=vad_options
    )
    output_files = []
    
    for i in range(num_tracks):
        user_id = get_track_title(audio_file, i)
        track_identifier = user_id if user_id else f"track_{i+1}"
        print(f"\n=== Processing Track {i+1}/{num_tracks} (ID: {track_identifier}) ===")

        # Create temp file for this track
        temp_wav = os.path.join(os.path.dirname(audio_file), f"temp_{track_identifier}.wav")

        try:
            # Extract specific track
            extract_track(audio_file, i, temp_wav)

            # Load audio
            # using an analogy - load the gun
            audio = whisperx.load_audio(temp_wav)

            # Transcribe - shoot the gun
            result = model.transcribe(audio, batch_size=BATCH_SIZE)

            # Align - inspect the aftermath and readjust shooting angle
            # We load/unload align model per track because language might differ per track (highly unlikely)
            print(f"--- Aligning Track {i+1} ({result['language']}) ---")
            model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=DEVICE)
            result = whisperx.align(result["segments"], model_a, metadata, audio, DEVICE, return_char_alignments=False)

            # Cleanup Align Model to save vram
            gc.collect()
            torch.cuda.empty_cache()
            del model_a

            # Save Result
            base_name = os.path.splitext(audio_file)[0]
            output_file = f"{track_identifier}.txt"

            print(f"--- Writing to {os.path.basename(output_file)} ---")
            with open(output_file, "w", encoding="utf-8") as f:
                for segment in result["segments"]:
                    start = round(segment['start'], 2)
                    end = round(segment['end'], 2)
                    text = segment['text'].strip()
                    f.write(f"[{start:.2f}s - {end:.2f}s]: {text}\n")

            output_files.append(output_file)

        except Exception as e:
            print(f"Error processing track {i+1}: {e}")
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
    parser = argparse.ArgumentParser(description="Run WhisperX on multi-track audio.")
    parser.add_argument("audio_path", type=str, help="Absolute path to the audio file")
    parser.add_argument("prompt", type=str, nargs="?", default=None)
    # ex: python3 main.py /ex/am/ple 
    args = parser.parse_args()

    try:
        files = process_audio(args.audio_path, args.prompt)
        print("\n---Completed---")
        for f in files:
            print(f)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
