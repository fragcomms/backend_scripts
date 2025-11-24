import logging
import time
import os
from nextcodefetcher import get_next_share_code
from dotenv import load_dotenv
import gevent
from steam.client import SteamClient
from cs2module.cs2client import CS2Client

load_dotenv()

bot_user = os.getenv('BOT_USERNAME')
bot_pw = os.getenv('BOT_PASSWORD')
sharecode = get_next_share_code(os.getenv("AARON_STEAM64ID"),
                                os.getenv("AARON_AUTHCODE"),
                                os.getenv("AARON_KNOWNCODE"))

# Setup logging AFTER getting sharecode otherwise risk of key leak
logging.basicConfig(filename=f'{int(time.time())}.log',
                    format='[%(asctime)s] %(levelname)s %(name)s: %(message)s',
                    level=logging.DEBUG)

client = SteamClient()
cs2 = CS2Client(client)

@client.on('logged_on')
def start_csgo():
    logging.info("Logged into Steam.")
    client.games_played([730]) # mimick bot playing cs2
    gevent.sleep(2) # sleep required as steam takes a while to register events
    cs2.send_hello()

if __name__ == "__main__":
    client.cli_login(username=bot_user, password=bot_pw)
    client.run_forever() # maybe not