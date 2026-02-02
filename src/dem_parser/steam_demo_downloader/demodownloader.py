import logging, time, os, gevent, sys
from gevent.queue import Queue
from gevent.event import AsyncResult
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
sharecode = os.getenv("AARON_KNOWNCODE")

# Setup logging AFTER getting sharecode otherwise risk of key leak
logging.basicConfig(filename=f'{int(time.time())}.log',
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                    level=logging.DEBUG)

client = SteamClient()
cs2 = CS2Client(client)

request_queue = Queue()
current_job_result = None

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
    logging.info(f"Processed: [{sharecode}]")
    logging.info(f"Download Link: {message.matches[0].roundstatsall[-1].map}")
    logging.info(f"Time: {datetime.fromtimestamp(message.matches[0].matchtime).strftime('%Y-%m-%d %H:%M:%S')}")

@client.on('logged_on')
def start_csgo():
    logging.info("Logged into Steam.")
    client.games_played([730]) # mimick bot playing cs2
    gevent.sleep(1) # sleep required as steam takes a while to register events
    cs2.send_hello()
    gevent.sleep(1)
    
    # start worker loop
    gevent.spawn(worker_loop)
    # if sharecode:
    #     request_queue.put(sharecode)
    #     request_queue.put("CSGO-H4mYW-j8mEB-jwxyH-KBEeK-5b9eD")
    #     request_queue.put("CSGO-82VPt-Px2FC-ViiRG-3SaFk-tXvzF")
    
@cs2.on(4004) # welcomed
def query_sharecode(*args):
    logging.info(f"Welcomed by GC")
    
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
    client.cli_login(username=bot_user, password=bot_pw)
    client.run_forever()