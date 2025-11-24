import requests
import os
from dotenv import load_dotenv
load_dotenv()

def get_next_share_code(target_steamid, auth_code, last_known_share_code):
    url = "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1"
    api_key = os.getenv('API_KEY')

    params = {
        "key": api_key,
        "steamid": target_steamid,      # The user's 64-bit SteamID
        # The user's Game Auth Code (AAAA-BBBB-...)
        "steamidkey": auth_code,
        # The last match code you have (CSGO-...)
        "knowncode": last_known_share_code
    }

    response = requests.get(url, params=params)
    code = response.status_code

    if code == 200:
        return response.json()["result"]
    elif code == 412:
        # TODO: return invalid share code
        return None

    # if none of the cases above,
    return None

# #just a test
# print(get_next_share_code(os.getenv("AARON_STEAM64ID"), 
#                           os.getenv("AARON_AUTHCODE"), 
#                           os.getenv("AARON_KNOWNCODE"), 
#                           os.getenv("API_KEY")))
