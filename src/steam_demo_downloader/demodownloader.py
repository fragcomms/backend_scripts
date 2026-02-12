import logging, time, os, gevent, sys, requests, bz2
from gevent.queue import Queue
from gevent.event import AsyncResult
from gevent.server import StreamServer
from dotenv import load_dotenv
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python" # required for protobuf compilation
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
    sys.exit(1) # Stop immediately if build fails

from cs2module.cs2client import CS2Client

load_dotenv()

bot_user = os.getenv('BOT_USERNAME')
bot_pw = os.getenv('BOT_PASSWORD')
# sharecode = os.getenv("AARON_KNOWNCODE")
DEMO_OUTPUT_DIR = os.getenv("DEMO_OUTPUT_DIR", "replays")
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://127.0.0.1:8000")

# Setup logging AFTER getting sharecode otherwise risk of key leak
script_dir = os.path.dirname(os.path.abspath(__file__))
log_filename = f'{int(time.time())}.log'
full_log_path = os.path.join(script_dir, log_filename)
logging.basicConfig(filename=full_log_path,
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                    level=logging.INFO)

client = SteamClient()
cs2 = CS2Client(client)

request_queue = Queue()
current_job_result = None
match_links = None

def handle_tcp_client(socket, address):
    logging.info(f"External connection from {address}")
    
    fileobj = socket.makefile(mode='rb')
    
    while True:
        # Wait for data
        line = fileobj.readline()
        
        # If line is empty, client disconnected
        if not line:
            break
            
        sharecode = line.strip().decode('utf-8')
        if sharecode:
            logging.info(f"Received from external source: {sharecode}")
            request_queue.put(sharecode)
            
    logging.info(f"Connection closed {address}")

# the worker to get through the queue
def worker_loop():
    global current_job_result
    logging.info("Worker started. Waiting for sharecodes...")
    
    while True: # run indefinitely so its on standby
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
    logging.info(f"Processed: [{sharecode}]")
    logging.info(f"Download Link: {message.matches[0].roundstatsall[-1].map}")
    logging.info(f"Time: {datetime.fromtimestamp(message.matches[0].matchtime).strftime('%Y-%m-%d %H:%M:%S')}")
    download_replay(message.matches[0].roundstatsall[-1].map)
    
def download_replay(url, output_dir=DEMO_OUTPUT_DIR):
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
        with open(bz2_filepath, 'wb') as f:
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
        trigger_parser(final_filepath)
        
    except Exception as e:
        logging.error(f"Failed to decompress {bz2_filepath}: {e}")

def trigger_parser(demo_path):
    """Sends a request to server.py to start parsing this file."""
    url = f"{ORCHESTRATOR_URL}/parse"
    
    # We must use absolute path because server.py might be running from a different folder
    payload = {"demo_path": os.path.abspath(demo_path)}
    
    try:
        logging.info(f"Triggering parser for: {demo_path}")
        # We use a short timeout so the downloader doesn't hang if the server is busy
        resp = requests.post(url, json=payload, timeout=5)
        
        if resp.status_code == 200:
            logging.info("Parser triggered successfully.")
        else:
            logging.error(f"Failed to trigger parser. Status: {resp.status_code} - {resp.text}")
            
    except Exception as e:
        logging.error(f"Could not connect to orchestrator at {url}: {e}")

@client.on('logged_on') # for steam client
def start_csgo():
    logging.info("Logged into Steam.")
    client.games_played([730]) # mimick bot playing cs2
    gevent.sleep(1) # sleep required as steam takes a while to register events
    cs2.send_hello()
    
@cs2.on(4004) # welcomed
def query_sharecode(*args):
    logging.info(f"Welcomed by GC")
    gevent.spawn(worker_loop)
    # if sharecode:
    #     request_queue.put(sharecode)
    #     request_queue.put("CSGO-H4mYW-j8mEB-jwxyH-KBEeK-5b9eD")
    #     request_queue.put("CSGO-82VPt-Px2FC-ViiRG-3SaFk-tXvzF")
    
@cs2.on(9139) # match list fetched
def on_match_list(message):
    global current_job_result
    
    # ignore if return was empty
    if current_job_result is None:
        logging.warning("Received 9139 message but no worker was waiting for it.")
        return

    #add to queue
    current_job_result.set(message)

if __name__ == "__main__":
    server = StreamServer(('127.0.0.1', 6000), handle_tcp_client)
    server.start() # Starts in background
    logging.info("TCP Server listening on 127.0.0.1:6000")
    client.cli_login(username=bot_user, password=bot_pw)
    client.run_forever()