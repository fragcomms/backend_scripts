from datetime import datetime  # ????????????
import os
import subprocess
import time
import sys
import logging
import json
import asyncio
import asyncpg
from typing import Optional, Dict
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv
from fastapi.responses import FileResponse

load_dotenv()

# Configuration
# Adjust these paths if your folder structure is different
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRANSCRIPT_SCRIPT = os.path.join(BASE_DIR, "transcription", "transcriber.py")
PARSER_SCRIPT = os.path.join(BASE_DIR, "dem_parser", "parser.py")
DOWNLOADER_SCRIPT = os.path.join(BASE_DIR, "steam_demo_downloader", "demodownloader.py")
DB_CONFIG = {
  "host": os.getenv("PG_HOST"),
  "port": os.getenv("PG_PORT"),
  "user": os.getenv("PG_USER"),
  "password": os.getenv("PG_PASS"),
  "database": os.getenv("PG_DB"),
}

TASK_CONTEXT: Dict[str, dict] = {}
# decided storing fragmented data from downloader here
# so that way there can only be one query for each parsed demo
downloader_process: Optional[subprocess.Popen] = None

db_pool: Optional[asyncpg.Pool] = None

logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
  handlers=[
    logging.FileHandler(f"{int(time.time())}.log"),
    logging.StreamHandler(sys.stdout),
  ],
)
logger = logging.getLogger("Orchestrator")


# DATABASE
async def insert_into_db(record: dict, event_type: str):
  if not db_pool:
    logger.error("DB Pool not initialized. No insertion.")
    return
  if event_type == "parse_meta_complete":
    logger.info("Inserting into fragcomms database, demo table")
    query = """
    INSERT INTO demos (
      outcome, file_path, length_ticks, fetch_time, match_code,
      map, tick_interval, score_t, score_ct
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    RETURNING demo_id"""

    try:
      raw_time = record.get("fetch_time")
      if isinstance(raw_time, str):
        fetch_dt = datetime.fromisoformat(raw_time)
      else:
        fetch_dt = raw_time

      # Optional: If datetime is naive (no timezone), force it to UTC for Postgres
      if fetch_dt.tzinfo is None:
        # We assume the time from Steam is UTC if not specified
        from datetime import timezone

        fetch_dt = fetch_dt.replace(tzinfo=timezone.utc)

    except Exception as e:
      logger.error(f"Date parsing failed for {raw_time}: {e}")
      return

    try:
      demo_id = await db_pool.fetchval(
        query,
        record["outcome"],
        record["file_path"],
        record["length_ticks"],
        fetch_dt,
        record["match_code"],
        record["map"],
        record["tick_interval"],
        record["score_t"],
        record["score_ct"],
      )
      logger.info(f"Insertion successful: {record['match_code']}")
      return demo_id
    except Exception as e:
      logger.error(f"Demo DB Insertion failed: {e}")

  elif event_type == "transcribe_complete":
    logger.info("Inserting into fragcomms database, audios table")
    query = """
    INSERT INTO transcripts (
      file_path, audio_id, model_id
    ) VALUES ($1, $2, $3)"""

    try:
      await db_pool.execute(
        query,
        record.get("filepath"),
        int(record.get("audio_id")),
        int(record.get("model_id")),
      )
      logger.info(f"Successfully linked {os.path.basename(record.get('filepath'))}")
    except Exception as e:
      logger.error(f"Transcript DB Insertion failed: {e}")


# SUBPROCESSES
#
#
async def handle_subprocess_event(event: dict, task_name: str):
  event_type = event.get("type")
  payload = event.get("payload", {})

  logger.info(f"Event Received: {event_type}")

  if event_type == "download_complete":
    demo_path = payload.get("demo_path")
    match_code = payload.get("match_code")
    fetch_time = payload.get("fetch_time")

    job_id = None
    for key, context in TASK_CONTEXT.items():
      if context.get("is_watcher") and context.get("match_code") == match_code:
        job_id = key
        break

    parser_task_name = f"Parser_{match_code[-5:]}"
    TASK_CONTEXT[parser_task_name] = {
      "match_code": match_code,
      "fetch_time": fetch_time,
      "job_id": job_id,
    }

    logger.info(f"Triggering parser for {match_code}")

    cmd = [sys.executable, PARSER_SCRIPT, demo_path, match_code, fetch_time]
    await launch_subprocess(cmd, parser_task_name)

  elif event_type == "parse_meta_complete":
    context = TASK_CONTEXT.pop(task_name, {})
    if not context:
      logger.error(f"Lost context for task {task_name}! Cannot save to DB.")
      return

    db_record = {
      "outcome": payload.get("outcome"),
      "file_path": payload.get("file_path"),
      "length_ticks": payload.get("length_ticks"),
      "fetch_time": context.get("fetch_time"),
      "match_code": context.get("match_code"),
      "map": payload.get("map"),
      "tick_interval": payload.get("tick_interval"),
      "score_t": payload.get("score_t"),
      "score_ct": payload.get("score_ct"),
    }

    demo_id = await insert_into_db(db_record, event_type)
    job_id = context.get("job_id")
    if job_id and job_id in TASK_CONTEXT:
      watcher = TASK_CONTEXT[job_id]
      watcher["demo_id"] = demo_id

      map_name = payload.get("map", "unknown_map")
      base_prompt = watcher.get("base_prompt")

      final_prompt = f"CS2, Counter-Strike, {map_name}"
      if base_prompt:
        final_prompt = f"{base_prompt}, {final_prompt}"

      transcriber_task_name = f"Transcriber_{job_id}"
      TASK_CONTEXT[transcriber_task_name] = {
        "audio_id": watcher["audio_id"],
        "job_id": job_id,
      }

      transcriber_cmd = [sys.executable, TRANSCRIPT_SCRIPT, watcher["audio_file_path"]]
      transcriber_cmd.append(final_prompt)

      logger.info(
        f"Parser finished, launching transcriber with map context: {map_name}"
      )
      await launch_subprocess(transcriber_cmd, transcriber_task_name)

  elif event_type == "transcribe_complete":
    context = TASK_CONTEXT.get(task_name, {})
    audio_id = context.get("audio_id")

    if not audio_id:
      logger.error(f"Lost context for {task_name}! Cannot save to DB.")
      return

    filepath = payload.get("filepath")
    logger.info(f"Transcript ready: {os.path.basename(filepath)}")
    payload["audio_id"] = audio_id

    await insert_into_db(payload, event_type)

    job_id = context.get("job_id")
    if job_id and job_id in TASK_CONTEXT:
      TASK_CONTEXT[job_id]["transcript_done"] = True
      await check_replay_watcher(job_id)


async def listen_to_process(process, task_name):
  try:
    while True:
      line_bytes = await process.stdout.readline()
      if not line_bytes:
        break

      line = line_bytes.decode("utf-8").strip()
      if not line:
        continue

      if line.startswith("DATA_OUTPUT:"):
        try:
          json_part = line.replace("DATA_OUTPUT:", "", 1)
          data = json.loads(json_part)
          await handle_subprocess_event(data, task_name)
        except Exception as e:
          logger.error(f"[{task_name}] Event Error: {e}")
      else:
        logger.info(f"[{task_name}] {line}")
    # await process.wait()  # clean up zomie processes
    # logger.info(f"[{task_name}] Process finished.")
  finally:
    await process.wait()
    logger.info(f"[{task_name}] Process finished with code {process.returncode}")

    if process.returncode != 0:
      context = TASK_CONTEXT.pop(task_name, {})
      job_id = context.get("job_id")

      if job_id and job_id in TASK_CONTEXT:
        logger.warning(f"Cleaning dead watcher: {job_id} due to {task_name} failure")
        del TASK_CONTEXT[job_id]
    else:
      TASK_CONTEXT.pop(task_name, None)


async def launch_subprocess(cmd: list, task_name: str):
  # async method
  script_path = cmd[1] if len(cmd) > 1 else None
  working_dir = os.path.dirname(script_path) if script_path else None
  if working_dir == "":
    working_dir = None

  run_cmd = cmd.copy()
  if run_cmd[0] == sys.executable and "-u" not in run_cmd:
    run_cmd.insert(1, "-u")

  logger.info(f"Launching task: {task_name}")
  # debug
  # logger.info(f"Command: {run_cmd}")
  # logger.info(f"CWD: {working_dir}")

  try:
    process = await asyncio.create_subprocess_exec(
      *run_cmd,
      cwd=working_dir,
      stdin=asyncio.subprocess.PIPE,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.STDOUT,
    )

    asyncio.create_task(listen_to_process(process, task_name))
    return process
  except Exception as e:
    logger.error(f"Failed to launch {task_name}: {e}")
    return None


# HELPER FUNCTION FOR DOWNLOADER
async def send_via_pipe(match_code: str):
  if downloader_process and downloader_process.returncode is None:
    try:
      downloader_process.stdin.write(f"{match_code}\n".encode())
      await downloader_process.stdin.drain()  # make sure it goes in
    except Exception as e:
      raise HTTPException(status_code=500, detail=f"Failed to pipe to downloader: {e}")
  else:
    raise HTTPException(status_code=503, detail="Downloader service is not running.")


# HELPER FUNCTION FOR TASK_CONTEXT
async def check_replay_watcher(job_id: str):
  watcher = TASK_CONTEXT.get(job_id)
  # remove misc requests
  if not watcher or not watcher.get("is_watcher"):
    return

  # we check if the fields are filled
  if watcher.get("demo_id") is not None and watcher.get("transcript_done") is True:
    logger.info(f"Watcher complete for {job_id}. Inserting replay")

    query = """
    INSERT INTO replays (demo_id, audio_id, name)
    VALUES ($1, $2, $3)
    """

    try:
      async with db_pool.acquire() as conn:
        await conn.execute(
          query, watcher["demo_id"], watcher["audio_id"], watcher["replay_name"]
        )
      logger.info(f"Successfully created replay: {watcher['replay_name']}")

      del TASK_CONTEXT[job_id]
    except Exception as e:
      logger.error(f"Replay DB Insertion failed: {e}")


# ROUTES
#
#


@asynccontextmanager
async def lifespan(app: FastAPI):
  global downloader_process, db_pool
  logger.info("Starting Services...")

  try:
    db_pool = await asyncpg.create_pool(**DB_CONFIG)
    logger.info("Connected to DB")
  except Exception as e:
    logger.critical(f"Failed to connect to DB: {e}")

  downloader_process = await launch_subprocess(
    [sys.executable, DOWNLOADER_SCRIPT], "Downloader"
  )

  yield

  if downloader_process:
    downloader_process.terminate()
    try:
      await asyncio.wait_for(downloader_process.wait(), timeout=2)
    except asyncio.TimeoutError:
      downloader_process.kill()

  if db_pool:
    await db_pool.close()


class DownloadRequest(BaseModel):
  match_code: str


class ParseRequest(BaseModel):
  demo_path: str
  match_code: str
  fetch_time: str


class TranscriptRequest(BaseModel):
  audio_id: int
  prompt: Optional[str] = None


class CreateReplayRequest(BaseModel):
  match_code: str
  audio_id: int
  prompt: Optional[str] = None
  replay_name: str


app = FastAPI(title="CS2 & Audio Orchestrator", lifespan=lifespan)


@app.post("/download")
async def trigger_download(req: DownloadRequest):
  """Sends a match_code to the background Steam downloader via Pipe."""
  await send_via_pipe(req.match_code)
  return {
    "status": "queued",
    "match_code": req.match_code,
    "message": "Sent to downloader service",
  }


# ONLY USE FOR DEBUG
@app.post("/parse")
async def trigger_parse(req: ParseRequest):
  """Runs the demo parser on a specific file."""
  if not os.path.exists(req.demo_path):
    raise HTTPException(status_code=404, detail="Demo file not found")

  task_name = f"Parser_{req.match_code[-5:]}"
  TASK_CONTEXT[task_name] = {
    "match_code": req.match_code,
    "fetch_time": req.fetch_time,
  }

  # Run in background so API doesn't hang
  cmd = [sys.executable, PARSER_SCRIPT, req.demo_path, req.match_code, req.fetch_time]
  await launch_subprocess(cmd, task_name)

  return {"status": "parsing", "file": req.demo_path, "message": "Sent to parser"}


@app.post("/transcribe")
async def trigger_transcribe(req: TranscriptRequest):
  """Runs WhisperX on an audio file."""
  if not db_pool:
    raise HTTPException(status_code=500, detail="Database not connected")

  async with db_pool.acquire() as conn:
    record = await conn.fetchrow(
      "SELECT file_path FROM audios WHERE audio_id = $1", req.audio_id
    )
  if not record or not record["file_path"]:
    raise HTTPException(status_code=404, detail="Audio ID not found in database")

  file_path = record["file_path"]

  if not os.path.exists(file_path):
    raise HTTPException(
      status_code=404, detail=f"Audio file not found on disk: {file_path}"
    )

  task_name = f"Transcriber_{req.audio_id}"
  TASK_CONTEXT[task_name] = {"audio_id": req.audio_id}

  cmd = [sys.executable, TRANSCRIPT_SCRIPT, file_path]
  if req.prompt:
    cmd.append(req.prompt)

  await launch_subprocess(cmd, task_name)

  return {"status": "processing", "audio_id": req.audio_id, "file": file_path}


@app.post("/create_replay")
async def create_replay(req: CreateReplayRequest):
  if not db_pool:
    raise HTTPException(status_code=500, detail="Database not connected")

  async with db_pool.acquire() as conn:
    record = await conn.fetchrow(
      "SELECT file_path FROM audios WHERE audio_id = $1", req.audio_id
    )

  if not record or not os.path.exists(record["file_path"]):
    raise HTTPException(status_code=404, detail="Audio file not found on disk or DB")

  # if not os.path.exists(req.demo_path):
  #   raise HTTPException(status_code=404, detail="Demo file not found")

  # define what fields are required for the watcher
  job_id = f"job_{req.match_code[-5:]}_{req.audio_id}"
  TASK_CONTEXT[job_id] = {
    "is_watcher": True,
    "match_code": req.match_code,
    "replay_name": req.replay_name,
    "audio_id": req.audio_id,
    "demo_id": None,
    "transcript_done": False,
    "audio_file_path": record["file_path"],
    "base_prompt": req.prompt,
  }

  await send_via_pipe(req.match_code)

  return {
    "status": "processing",
    "job_id": job_id,
    "message": "Pipeline initialized: downloader started",
  }


# it needs to have .json prefixed already
# helper for nodejs backend
@app.get("/get_json")
async def get_parsed_json(filepath: str):
  if not os.path.exists(filepath):
    raise HTTPException(status_code=404, detail="JSON file not found on remote server")
  return FileResponse(filepath, media_type="application/json")


# helper for nodejs backend
@app.get("/get_audio")
async def get_audio(filepath: str):
  if not os.path.exists(filepath):
    raise HTTPException(status_code=404, detail="Audio file not found on remote server")
  return FileResponse(filepath, media_type="audio/x-matroska")


@app.get("/get_transcript")
async def get_transcript(filepath: str):
  if not os.path.exists(filepath):
    raise HTTPException(
      status_code=404, detail="Transcript file not found on remote server"
    )
  return FileResponse(filepath, media_type="text/plain")


@app.get("/health")
async def health_check():
  return {"status": "ok"}


if __name__ == "__main__":
  uvicorn.run(app, host="0.0.0.0", port=8000)
