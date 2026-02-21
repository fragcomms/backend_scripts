import os
import subprocess
import socket
import time
import sys
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager
from pydantic import BaseModel
import uvicorn

# Configuration
# Adjust these paths if your folder structure is different
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRANSCRIPT_SCRIPT = os.path.join(BASE_DIR, "transcripts", "transcriber.py")
PARSER_SCRIPT = os.path.join(BASE_DIR, "dem_parser", "parser.py")
DOWNLOADER_SCRIPT = os.path.join(BASE_DIR, "steam_demo_downloader", "demodownloader.py")
# DOWNLOADER_HOST = "127.0.0.1"
# DOWNLOADER_PORT = 6000

downloader_process: Optional[subprocess.Popen] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global downloader_process
    print("Starting Steam Downloader Service...")
    
    downloader_process = subprocess.Popen(
        [sys.executable, DOWNLOADER_SCRIPT],
        cwd=os.path.dirname(DOWNLOADER_SCRIPT),
        stdin=subprocess.PIPE,  # REQUIRED
        # stdout=sys.stdout,      # Pass logs to main console (optional)
        # stderr=sys.stderr,
        text=True,              # Allows passing strings directly
        bufsize=1               # Line buffered
    )
    
    yield # App runs here
    
    # Cleanup on shutdown
    print("Shutting down Downloader Service...")
    if downloader_process:
        downloader_process.terminate()

app = FastAPI(title="CS2 & Audio Orchestrator", lifespan=lifespan)

class TranscriptRequest(BaseModel):
    file_path: str
    prompt: Optional[str] = None

class ParseRequest(BaseModel):
    demo_path: str

class DownloadRequest(BaseModel):
    sharecode: str
    
def send_via_pipe(sharecode: str):
    global downloader_process
    if downloader_process and downloader_process.poll() is None:
        try:
            # Write sharecode + newline
            downloader_process.stdin.write(f"{sharecode}\n")
            downloader_process.stdin.flush() # make sure it goes in
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to pipe to downloader: {e}")
    else:
        raise HTTPException(status_code=503, detail="Downloader service is not running.")

def run_subprocess(command: list):
    print(f"Executing: {' '.join(command)}")
    try:
        result = subprocess.run(
            command, 
            capture_output=True, 
            text=True, 
            check=True
        )
        print(result.stdout)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        raise HTTPException(status_code=500, detail=f"Script failed: {e.stderr}")       

#routes

@app.post("/download")
async def trigger_download(req: DownloadRequest):
    """Sends a sharecode to the background Steam downloader via Pipe."""
    send_via_pipe(req.sharecode)
    return {"status": "queued", "sharecode": req.sharecode, "message": "Sent to downloader service"}

@app.post("/parse")
async def trigger_parse(req: ParseRequest, background_tasks: BackgroundTasks):
    """Runs the demo parser on a specific file."""
    if not os.path.exists(req.demo_path):
        raise HTTPException(status_code=404, detail="Demo file not found")
    
    # Run in background so API doesn't hang
    cmd = [sys.executable, PARSER_SCRIPT, req.demo_path]
    background_tasks.add_task(run_subprocess, cmd)
    
    return {"status": "processing", "file": req.demo_path}

@app.post("/transcribe")
async def trigger_transcribe(req: TranscriptRequest, background_tasks: BackgroundTasks):
    """Runs WhisperX on an audio file."""
    if not os.path.exists(req.file_path):
        raise HTTPException(status_code=404, detail="Audio file not found")
    
    cmd = [sys.executable, TRANSCRIPT_SCRIPT, req.file_path]
    if req.prompt:
        cmd.append(req.prompt)
        
    # Run in background so API doesn't hang (Whisper is slow)
    background_tasks.add_task(run_subprocess, cmd)
    
    return {"status": "processing", "file": req.file_path}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)