from demoparser2 import DemoParser
import json
import os
import sys
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()

TICK_INTERVAL = 12
OUTPUT_FOLDER = os.getenv("PARSER_OUTPUT_DIR", "output")

def get_demo_path():
    if len(sys.argv) < 2:
        print("Usage: python3 parser.py <path_to_demo>")
        sys.exit(1)
    demo_path = sys.argv[1]
    if not os.path.exists(demo_path):
        print(f"Error: File '{demo_path}' not found.")
        sys.exit(1)
    return demo_path

def parse_game_events(parser, match_start_tick):
    event_names = ["weapon_fire", "player_death", "round_start", "round_end", "bomb_planted"]
    events_df = parser.parse_events(event_names, other=["game_time", "team_num"])
    
    processed_events = {}
    for event_name, df in events_df:
        if df is None or df.empty:
            continue
            
        if event_name in ["round_start", "round_end"]:
            buffer = 2000 # required to capture the first round tick start and ends
            df = df[df['tick'] >= (match_start_tick - buffer)]
        else:
            df = df[df['tick'] >= match_start_tick]
        
        wanted_cols = ["tick"] # tick always required
        rename_map = {}

        if event_name == "player_death":
            rename_map = {
                "user_steamid": "vic",      # Victim
                "attacker_steamid": "att",  # Attacker
                "assister_steamid": "ass",  # assister, NaN if none
                "weapon": "weapon",
                "headshot": "hs"
            }
            wanted_cols += ["vic", "att", "ass", "weapon", "hs"]

        elif event_name == "weapon_fire":
            rename_map = {
                "user_steamid": "sid",      # shooter
                "weapon": "weapon"
            }
            wanted_cols += ["sid", "weapon"]

        elif event_name == "bomb_planted":
            rename_map = {
                "user_steamid": "sid",      # planter
                "site": "site"
            }
            wanted_cols += ["sid", "site"]
            
        elif event_name == "round_end":
            rename_map = {
                "winner": "winner",         # which side won the round
                "reason": "reason"
            }
            wanted_cols += ["winner", "reason"]
            
        elif event_name == "round_start":
            rename_map = {
                "timelimit": "time",
            }
            wanted_cols += ["time"]

        # Apply Rename
        df = df.rename(columns=rename_map)

        # keep columns that are wanted and discard rest
        existing_cols = [c for c in wanted_cols if c in df.columns]
        df = df[existing_cols]

        processed_events[event_name] = df.to_dict(orient="records")

    return processed_events

def get_match_metadata(parser):
    try:
        match_start_df = parser.parse_event("begin_new_match")
        start_tick = int(match_start_df['tick'].iloc[0]) if not match_start_df.empty else 0
    except:
        start_tick = 0
    
    # Fallback to round_start if begin_new_match is missing
    if start_tick == 0:
        try:
            round_start_df = parser.parse_event("round_start")
            if not round_start_df.empty:
                start_tick = int(round_start_df['tick'].iloc[0])
        except:
            pass

    max_tick_df = parser.parse_ticks(["tick"])
    end_tick = int(max_tick_df['tick'].max())
    return start_tick, end_tick

def process_ticks(parser, start_tick, end_tick):
    wanted_ticks = np.arange(start_tick, end_tick + 1, TICK_INTERVAL)
    if wanted_ticks[-1] != end_tick:
        wanted_ticks = np.append(wanted_ticks, end_tick)

    # we need Name/Team for lookup, but won't save them in timeline
    props = [
        "player_steamid", "player_name", "team_num", 
        "is_alive", "health", "X", "Y", "Z", "pitch", "yaw"
    ]
    
    print(f"Fetching {len(wanted_ticks)} ticks...")
    df = parser.parse_ticks(props, ticks=list(wanted_ticks))
    
    # Map SteamID -> Name/Team
    # this runs BEFORE we strip columns, so we can capture the names
    player_info = df.groupby("player_steamid").first()[["player_name", "team_num"]].reset_index()
    player_lookup = {}
    for _, row in player_info.iterrows():
        player_lookup[str(row['player_steamid'])] = {
            "name": row['player_name'],
            "team": int(row['team_num'])
        }

    # to make sure we only get alive players during the ticks
    df = df[df['is_alive'] == True]

    # rounding floats to make sure no crazy value (0.032193120310) happens
    df['X'] = df['X'].astype(float).round(2)
    df['Y'] = df['Y'].astype(float).round(2)
    df['Z'] = df['Z'].astype(float).round(2)
    df['pitch'] = df['pitch'].round(0).astype(int)
    df['yaw'] = df['yaw'].round(0).astype(int)
    
    # rename columns to short keys
    col_map = {
        "player_steamid": "sid",
        "health": "hp",
        "X": "x",
        "Y": "y",
        "Z": "z",
        "pitch": "p",
        "yaw": "rot"  # 'rot' for rotation
    }
    df = df.rename(columns=col_map)

    # keep ONLY these columns + tick
    # we drop name, team, is_alive, and any other misc
    keep_cols = ["tick", "sid", "hp", "x", "y", "p", "rot"]
    
    # Filter to ensure we only have existing columns (avoids errors if a tick is empty)
    existing_cols = [c for c in keep_cols if c in df.columns]
    df = df[existing_cols]

    # convert to timeline list
    timeline = []
    grouped = df.groupby("tick")
    
    for tick, group in grouped:
        # Drop 'tick' from the inner records
        group_data = group.drop(columns=["tick"])
        players_data = group_data.to_dict(orient="records")
        
        timeline.append({
            "tick": int(tick),
            "p": players_data
        })
        
    return timeline, player_lookup

def save_json(data, filename):
    if os.path.isabs(OUTPUT_FOLDER):
        output_dir = OUTPUT_FOLDER
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, OUTPUT_FOLDER)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    filepath = os.path.join(output_dir, filename)
    
    print(f"Saving to {filepath}...")
    with open(filepath, "w") as f:
        # Use separators to strip whitespace completely (it makes a difference in size)
        json.dump(data, f, separators=(',', ':'))
        # json.dump(data, f, indent=1) # use if you need to understand how the structure is
    print("Done.")

def main():
    demo_path = get_demo_path()
    parser = DemoParser(demo_path)
    start_tick, end_tick = get_match_metadata(parser)

    # Process ticks and get the player lookup table
    ticks_data, player_lookup = process_ticks(parser, start_tick, end_tick)
    events_data = parse_game_events(parser, start_tick)

    replay_json = {
        "meta": {
            "filename": os.path.basename(demo_path),
            "interval": TICK_INTERVAL,
            "start": start_tick,
            "end": end_tick
        },
        "players": player_lookup, # Static data here
        "timeline": ticks_data,   # Dynamic data here
        "events": events_data
    }

    save_json(replay_json, f"{os.path.basename(demo_path)}.json")
    os.remove(demo_path)

if __name__ == "__main__":
    main()