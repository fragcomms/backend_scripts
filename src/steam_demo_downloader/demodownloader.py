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
from datetime import timezone

os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = (
  "python"  # required for protobuf compilation
)
from steam.client import SteamClient
from datetime import datetime

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

client = SteamClient()
cs2 = CS2Client(client)

request_queue = Queue()
current_job_result = None
match_links = None


def console_input_listener():
  logging.info("Listening for sharecodes via stdin...")
  while True:
    try:
      # specifically use select to be non-blocking friendly
      gevent.select.select([sys.stdin], [], [])
      line = sys.stdin.readline()
      if not line:
        break  # EOF (Parent process closed)

      sharecode = line.strip()
      if sharecode:
        logging.info(f"Received via Pipe: {sharecode}")
        request_queue.put(sharecode)
    except Exception as e:
      logging.error(f"Input error: {e}")
      break


# the worker to get through the queue
def worker_loop():
  global current_job_result
  logging.info("Worker started. Waiting for sharecodes...")

  while True:  # run indefinitely so its on standby
    sharecode = request_queue.get()
    logging.info(f"Processing: {sharecode}")
    current_job_result = AsyncResult()

    try:
      cs2.set_target_match(sharecode)
      cs2.request_match_info()

      response_message = current_job_result.get(timeout=10)
      process_match_data(sharecode, response_message)

    except gevent.Timeout:
      logging.error(f"Timeout waiting for response for {sharecode}")
    except Exception as e:
      logging.error(f"Error processing {sharecode}: {e}")
    finally:
      current_job_result = None
      gevent.sleep(1)


# processes match data and grabs necessary information
def process_match_data(sharecode, message):
  if not message.matches:
    logging.warning(f"No match found for {sharecode}")
    return
  # logging.info(message)
  match_url = message.matches[0].roundstatsall[-1].map
  match_time_iso = datetime.fromtimestamp(
    message.matches[0].matchtime, tz=timezone.utc
  ).isoformat()
  logging.info(f"Processed: [{sharecode}]")
  # logging.info(f"Download Link: {match_url}")
  logging.info(f"Time: {match_time_iso}")
  download_replay(match_url, sharecode, match_time_iso)


def download_replay(url, sharecode, match_time_iso, output_dir=DEMO_OUTPUT_DIR):
  if os.path.isabs(output_dir):
    full_output_path = output_dir
  else:
    full_output_path = os.path.join(script_dir, output_dir)

  if not os.path.exists(full_output_path):
    os.makedirs(full_output_path)

  logging.info(f"Starting download: {url}")

  bz2_filename = os.path.basename(url)
  bz2_filepath = os.path.join(full_output_path, bz2_filename)

  with requests.get(url, stream=True) as r:
    r.raise_for_status()
    with open(bz2_filepath, "wb") as f:
      for chunk in r.iter_content(chunk_size=8192):
        f.write(chunk)
  logging.info(f"Download complete: {bz2_filepath}")

  final_filename = os.path.splitext(bz2_filename)[0]
  final_filepath = os.path.join(full_output_path, final_filename)

  logging.info(f"Decompressing to: {final_filepath}")

  try:
    with bz2.open(bz2_filepath, "rb") as source, open(final_filepath, "wb") as dest:
      for data in iter(lambda: source.read(100 * 1024), b""):
        dest.write(data)

    logging.info("Decompression successful.")
    os.remove(bz2_filepath)
    event = {
      "type": "download_complete",
      "payload": {
        "match_code": sharecode,
        "fetch_time": match_time_iso,
        "demo_path": os.path.abspath(final_filepath),
      },
    }
    # trigger_parser(final_filepath, sharecode, match_time_iso)
    print(f"DATA_OUTPUT:{json.dumps(event)}", flush=True)

  except Exception as e:
    logging.error(f"Failed to decompress {bz2_filepath}: {e}")


@client.on("logged_on")  # for steam client
def start_csgo():
  logging.info("Logged into Steam.")
  client.games_played([730])  # mimick bot playing cs2
  gevent.sleep(1)  # sleep required as steam takes a while to register events
  cs2.send_hello()


# apparently steam disconnects our account if we ever log on for too long
# i am too lazy to look at the debug so i will leave this as a potential solution
@client.on("disconnected")
def on_disconnect():
  logging.warning("Disconnected from Steam. Attempting to reconnect...")
  gevent.sleep(5)
  client.reconnect()


# one potential solution
@client.on("relogged")
def handle_relog():
  logging.info("Relogged successfully. Re-launching CS2 session...")
  client.games_played([730])
  gevent.sleep(2)
  cs2.send_hello()


# heartbeat keep alive
# also a potential solution
def gc_keep_alive():
  while True:
    if client.logged_on:
      try:
        cs2.send_hello()
        logging.debug("Sent heartbeat to GC")
      except Exception as e:
        logging.error(f"Heartbeat failed: {e}")
    gevent.sleep(300)


@cs2.on(4004)  # welcomed
def query_sharecode(*args):
  logging.info("Welcomed by GC")
  gevent.spawn(worker_loop)
  gevent.spawn(console_input_listener)
  gevent.spawn(gc_keep_alive)
  # request_queue.put("CSGO-ySXxw-kOz5h-D795M-EuouP-ZbaEC")
  # if sharecode:
  #     request_queue.put(sharecode)
  #     request_queue.put("CSGO-ySXxw-kOz5h-D795M-EuouP-ZbaEC")
  #     request_queue.put("CSGO-82VPt-Px2FC-ViiRG-3SaFk-tXvzF")


@cs2.on(9139)  # match list fetched
def on_match_list(message):
  global current_job_result

  # ignore if return was empty
  if current_job_result is None:
    logging.warning("Received 9139 message but no worker was waiting for it.")
    return

  # add to queue
  current_job_result.set(message)


if __name__ == "__main__":
  try:
    logging.info("Steam account service started")
    client.cli_login(username=bot_user, password=bot_pw)
    client.run_forever()
  except KeyboardInterrupt:
    logging.info("Shutting down...")
    sys.exit(0)
  except Exception as e:
    logging.error(f"Fatal error: {e}")
