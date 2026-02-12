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
DOWNLOADER_HOST = "127.0.0.1"
DOWNLOADER_PORT = 6000

@asynccontextmanager
async def lifespan(app: FastAPI):
    # [STARTUP LOGIC] Code before yield runs on start
    if not is_port_open(DOWNLOADER_HOST, DOWNLOADER_PORT):
        print("Starting Steam Downloader Service...")
        subprocess.Popen(
            [sys.executable, DOWNLOADER_SCRIPT],
            cwd=os.path.dirname(DOWNLOADER_SCRIPT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        time.sleep(2) # Give it time to initialize
    else:
        print("Steam Downloader Service is already running.")
    
    yield # Hand over control to the application

app = FastAPI(title="CS2 & Audio Orchestrator", lifespan=lifespan)

class TranscriptRequest(BaseModel):
    file_path: str
    prompt: Optional[str] = None

class ParseRequest(BaseModel):
    demo_path: str

class DownloadRequest(BaseModel):
    sharecode: str

def is_port_open(host, port):
    """Checks if the downloader service is already running."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((host, port)) == 0

def send_to_downloader(sharecode: str):
    """Connects to the running demodownloader TCP server."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((DOWNLOADER_HOST, DOWNLOADER_PORT))
            # Send sharecode + newline as expected by your handle_tcp_client
            message = f"{sharecode}\n".encode('utf-8')
            s.sendall(message)
    except ConnectionRefusedError:
        raise HTTPException(status_code=503, detail="Downloader service is not reachable.")

def run_subprocess(command: list):
    """Runs a script and logs output."""
    print(f"Executing: {' '.join(command)}")
    try:
        # We wait for the process to finish. 
        # For long jobs, you might want to use Popen and not wait, 
        # but for simplicity, we wait here to return status.
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
    """Sends a sharecode to the background Steam downloader."""
    send_to_downloader(req.sharecode)
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