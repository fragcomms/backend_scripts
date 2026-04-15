import gevent.monkey
gevent.monkey.patch_all()


import logging
import os
import gevent
import sys
import requests
import bz2
import json
from gevent.queue import Queue
from gevent.event import AsyncResult
from dotenv import load_dotenv
from datetime import timezone, datetime

os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = (
  "python"  # required for protobuf compilation
)

from steam.client import SteamClient

# build first then import otherwise it will break
try:
  from cs2module.protobufs import build

  print("Compiling Protobufs...")
  build()
  print("Compilation Complete.\n")
except Exception as e:
  logging.error(f"Build failed: {e}")
  sys.exit(1)  # Stop immediately if build fails

from cs2module.cs2client import CS2Client

load_dotenv()

bot_user = os.getenv("BOT_USERNAME")
bot_pw = os.getenv("BOT_PASSWORD")
# sharecode = os.getenv("AARON_KNOWNCODE")
DEMO_OUTPUT_DIR = os.getenv("DEMO_OUTPUT_DIR", "replays")
# ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://127.0.0.1:8000")



# Setup logging AFTER getting sharecode otherwise risk of key leak
script_dir = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(format="%(message)s", level=logging.INFO)


class CS2DemoDownloader:
  def __init__(self):
    self.bot_user = os.getenv("BOT_USERNAME")
    self.bot_pw = os.getenv("BOT_PASSWORD")
    self.output_dir = os.getenv("DEMO_OUTPUT_DIR", "replays")
    
    self.client = SteamClient()
    self.cs2 = CS2Client(self.client)
    
    # states
    self.request_queue = Queue()
    self.current_job_result = None
    self.workers_started = False
    
    self._register_events()
    
  def _register_events(self):
    self.client.on("logged_on", self.on_logged_on)
    self.client.on("disconnected", self.on_disconnect)
    self.client.on("relogged", self.on_relogged)
    self.cs2.on(4004, self.on_welcomed) # welcomed by GC 4004
    self.cs2.on(9139, self.on_match_list) # fetched match list 9139
    
  # HELPER FUNCTIONS FOR SIMPLIFYING GC COMMUNICATIONS
  
  def on_logged_on(self):
    logging.info("Logged into Steam.")
    self.client.games_played([730]) # mimick bot playing cs2
    gevent.sleep(2) # sleep required as steam takes a while to register events
    self.cs2.send_hello()
    # gevent.spawn(self.test_forced_disconnect)
  
  def on_disconnect(self):
    logging.warning("Disconnected from Steam. Reconnecting...")
    gevent.sleep(5)
    if self.client.relogin_available:
      logging.info("Valid session found. Reconnecting to Steam...")
      self.client.reconnect(maxdelay=30)
    else:
      logging.info("Session invalid. Attempting headless login...")
      self.client.login(username=self.bot_user, password=self.bot_pw)
      
  def on_relogged(self):
    logging.info("Relogged successfully. Launching CS2 session...")
    self.client.games_played([730])
    gevent.sleep(2)
    self.cs2.send_hello()
    
  def test_forced_disconnect(self):
    logging.info("TESTING: Disconnect timer started for 10 seconds...")
    gevent.sleep(10)
    logging.warning("TESTING: Dropping connection now")
    self.client.disconnect()
    
  def on_welcomed(self, *args):
    logging.info("Welcomed by GC")
    if not self.workers_started:
      logging.info("Starting background worker threads...")
      gevent.spawn(self.worker_loop)
      gevent.spawn(self.console_input_listener)
      gevent.spawn(self.gc_keep_alive)
      self.workers_started = True
      
  def on_match_list(self, message):
    if self.current_job_result is None:
      logging.warning("Received 9139 message but no worker was waiting for it.")
      return
    self.current_job_result.set(message)
    
  # potential solution to keep account up 24/7
  def gc_keep_alive(self):
    while True:
      if self.client.logged_on:
        try:
          self.cs2.send_hello()
          logging.debug("Sent heartbeat to GC")
        except Exception as e:
          logging.error(f"Heartbeat failed: {e}")
        gevent.sleep(600)
        
  # WORKER AND I/O PROCESSES
  def console_input_listener(self):
    logging.info("Listening for sharecodes via stdin...")
    while True:
      try:
        gevent.select.select([sys.stdin], [], [])
        line = sys.stdin.readline()
        if not line:
          break
          
        sharecode = line.strip()
        if sharecode:
          logging.info(f"Received via pipe: {sharecode}")
          self.request_queue.put(sharecode)
      except Exception as e:
        logging.error(f"Input error: {e}")
        break
        
  def worker_loop(self):
    logging.info("Worker started. Waiting for sharecodes...")
    while True:
      sharecode = self.request_queue.get()
      logging.info(f"Processing: {sharecode}")
      self.current_job_result = AsyncResult()
      
      try:
        self.cs2.set_target_match(sharecode)
        self.cs2.request_match_info()
        
        response_message = self.current_job_result.get(timeout=10)
        self.process_match_data(sharecode, response_message)
      except gevent.Timeout:
        logging.error(f"Timeout waiting for response for {sharecode}")
      except Exception as e:
        logging.error(f"Error processing {sharecode}: {e}")
      finally:
        self.current_job_result = None
        gevent.sleep(2)
        
  # DATA EXTRACTION AND DOWNLOADS
  def process_match_data(self, sharecode, message):
    if not message.matches:
      logging.warning(f"No match found for {sharecode}")
      return
      
    match_url = message.matches[0].roundstatsall[-1].map
    match_time_iso = datetime.fromtimestamp(message.matches[0].matchtime, tz=timezone.utc).isoformat()
    logging.info(f"Processed: [{sharecode}] | Time: {match_time_iso}")
    self.download_replay(match_url, sharecode, match_time_iso)
    
  def download_replay(self, url, sharecode, match_time_iso):
    full_output_path = self.output_dir if os.path.isabs(self.output_dir) else os.path.join(script_dir, self.output_dir)
    logging.info(f"Starting download: {url}")
    bz2_filename = os.path.basename(url)
    bz2_filepath = os.path.join(full_output_path, bz2_filename)
    
    try:
      # downloading file
      with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(bz2_filepath, "wb") as f:
          for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
      logging.info(f"Download complete: {bz2_filepath}")
      
      # decompressing file
      final_filename = os.path.splitext(bz2_filename)[0]
      final_filepath = os.path.join(full_output_path, final_filename)
      logging.info(f"Decompressing to: {final_filepath}")
      
      with bz2.open(bz2_filepath, "rb") as source, open(final_filepath, "wb") as dest:
        for data in iter(lambda: source.read(100 * 1024), b""):
          dest.write(data)
          
      logging.info("Decompression successful.")
      os.remove(bz2_filepath) # COMMENT IF YOU DON'T WANT TO DELETE ORIGINAL FILE
      
      # payload for orchestrator
      event = {
        "type": "download_complete",
        "payload": {
          "match_code": sharecode,
          "fetch_time": match_time_iso,
          "demo_path": os.path.abspath(final_filepath),
        }
      }
      print(f"DATA_OUTPUT:{json.dumps(event)}", flush=True)
      
    except Exception as e:
      logging.error(f"Failed to download/decompress for {sharecode}: {e}")
      
  def run(self):
    logging.info("Steam account service started")
    try:
      self.client.cli_login(username=self.bot_user, password=self.bot_pw)
      self.client.run_forever()
    except KeyboardInterrupt:
      logging.info("Shutting down...")
      sys.exit(0)
    except Exception as e:
      logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
  app = CS2DemoDownloader()
  app.run()

