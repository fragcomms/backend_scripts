from demoparser2 import DemoParser

parser = DemoParser("furia-vs-falcons-m1-inferno-11-16.dem")

event_names = parser.list_game_events()

# dfs = {}
# for event in event_names:
#   try:
#     dfs[event] = parser.parse_event(event)
#     print(f"parsed {event}")
#   except Exception as e:
#     print(f"failed on {event}: {e}")
    

# Look for the specific event that signifies the match going "Live"
events = parser.parse_events(["begin_new_match", "round_announce_match_start"])

for event in events:
  print(f"Event: {event[0]}, Tick: {event[1]}")