#this exports a JSON of all ticks (which we prolly wont use)
#and also makes a folder of CSVs with player movement info
from demoparser2 import DemoParser
import json
import os
import csv

parser = DemoParser("src/replays/parivision-vs-astralis-m1-overpass.dem")

event_names = [
    "begin_new_match", "round_start", "round_end", "round_mvp", 
    "player_death", "bomb_planted", "bomb_defused", "hostage_rescued", 
    "weapon_fire", "flashbang_detonate", "hegrenade_detonate", 
    "molotov_detonate", "smokegrenade_detonate", "player_hurt", 
    "player_blind"
]

all_events = parser.parse_events(event_names, other=["game_time", "team_num"])

# find match start tick
begin_new_match_df = next((df for event_name, df in all_events if event_name == 'begin_new_match'), None)
match_start_tick = begin_new_match_df['tick'].iloc[0] if begin_new_match_df is not None else 0

#find round 1 start tick (skip warmup if applicable)
parse_rounds = parser.parse_event("round_start")
r1_tick = parse_rounds['tick'].iloc[0] if parse_rounds is not None else 0
print(f"Round 1 start tick: {r1_tick}")

#find last tick of match
all_ticks_df = parser.parse_ticks(["tick"])
max_tick_obj = all_ticks_df.max()
max_tick = max_tick_obj.iloc[0] #should be around 227,237 for pari vs astralis match that i tested with, ymmv
print(f"Last tick: {max_tick}")


#sampling interval for ticks (every 8) smooth movement that isn't too heavy on cpu
tick_interval = 8
wanted_ticks = list(range(r1_tick, max_tick + 1, tick_interval))


# Filter out events before the match start
filtered_events = [(event_name, df[df['tick'] >= match_start_tick]) for event_name, df in all_events]

#wanted props for json, csv
wanted_props = ["equipment_value_this_round", "cash_spent_this_round", "is_alive", "team_num", "player_name", "score", "player_steamid", "X", "Y"]
csv_wanted_props = ["tick", "player_name", "player_steamid", "team_num", "is_alive", "health", "X", "Y", "pitch", "yaw"]

#parse all
csv_ticks = parser.parse_ticks(csv_wanted_props, ticks=wanted_ticks)
very_last_csv_tick = parser.parse_ticks(csv_wanted_props, ticks=[max_tick])


tick_values = set()

for _, df in filtered_events:
    tick_values.update(df['tick'].unique())

all_ticks = parser.parse_ticks(wanted_props, ticks=list(tick_values))

# Convert ticks to JSON structure
ticks_json = {}

for row in all_ticks.itertuples():
    tick = row.tick

    if tick not in ticks_json:
        ticks_json[tick] = {"tick": tick, "players": []}
    
    ticks_json[tick]["players"].append({
        "steamid": row.player_steamid,
        "name": row.player_name,
        "x": float(row.X),
        "y": float(row.Y),
        "alive": bool(row.is_alive),
        "team": int(row.team_num),
        "value": int(row.equipment_value_this_round),
        "spent": int(row.cash_spent_this_round),
        "score": int(row.score)
    })

#convert ticks from dict -> array sorted by tick
ticks_list = [ticks_json[t] for t in sorted(ticks_json.keys())]

#extract event logs as JSON
def df_to_json(df):
    if df is None:
        return[]
    return df.to_dict(orient="records")

shot_events = df_to_json(next((df for event_name, df in filtered_events if event_name == 'weapon_fire'), None))
hit_events = df_to_json(next((df for event_name, df in filtered_events if event_name == 'player_hurt'), None))
flash_events = df_to_json(next((df for event_name, df in filtered_events if event_name == 'player_blind'), None))

#final repaly JSON object
replay_json = {
    "ticks": ticks_list,
    "shots": shot_events,
    "hits": hit_events,
    "flashes": flash_events
}

#write player movements to CSV
csv_filepath = "output/player_movements.csv"
csv_ticks = csv_ticks[csv_wanted_props]
last_tick = very_last_csv_tick[csv_wanted_props]
#csv_ticks = csv_ticks + last_tick

print(last_tick)

csv_ticks.to_csv(csv_filepath, index=False)
last_tick.to_csv(csv_filepath, mode='a', index=False, header=False)
print(f"Wrote {csv_filepath}")






os.makedirs("output", exist_ok=True)

with open("output/replay.json", "w") as f:
    json.dump(replay_json, f, indent=2)

print("Wrote output/replay.json")