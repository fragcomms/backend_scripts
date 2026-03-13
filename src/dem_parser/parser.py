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


def get_absolute_path(filename):
  if os.path.isabs(OUTPUT_FOLDER):
    output_dir = OUTPUT_FOLDER
  else:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, OUTPUT_FOLDER)
  return os.path.join(output_dir, filename)


def save_json(data, filepath):
  os.makedirs(os.path.dirname(filepath), exist_ok=True)

  print(f"Saving to {filepath}...")
  with open(filepath, "w") as f:
    # Use separators to strip whitespace completely
    json.dump(data, f, separators=(",", ":"))
    # json.dump(data, f, indent=1) # use if you need to understand how the structure is
  print("Done.")
  return filepath


def parse_game_events(parser, match_start_tick):
  event_names = [
    "weapon_fire",
    "player_death",
    "round_start",
    "round_end",
    "bomb_planted",
  ]
  events_df = parser.parse_events(event_names, other=["game_time", "team_num"])

  processed_events = {}
  for event_name, df in events_df:
    if df is None or df.empty:
      continue

    if event_name in ["round_start", "round_end"]:
      buffer = 2000  # required to capture the first round tick start and ends
      df = df[df["tick"] >= (match_start_tick - buffer)]
    else:
      df = df[df["tick"] >= match_start_tick]

    wanted_cols = ["tick"]  # tick always required
    rename_map = {}

    if event_name == "player_death":
      rename_map = {
        "user_steamid": "vic",  # Victim
        "attacker_steamid": "att",  # Attacker
        "assister_steamid": "ass",  # assister, NaN if none
        "weapon": "weapon",
        "headshot": "hs",
      }
      wanted_cols += ["vic", "att", "ass", "weapon", "hs"]

    elif event_name == "weapon_fire":
      rename_map = {
        "user_steamid": "sid",  # shooter
        "weapon": "weapon",
      }
      wanted_cols += ["sid", "weapon"]

    elif event_name == "bomb_planted":
      rename_map = {
        "user_steamid": "sid",  # planter
        "site": "site",
      }
      wanted_cols += ["sid", "site"]

    elif event_name == "round_end":
      rename_map = {
        "winner": "winner",  # which side won the round
        "reason": "reason",
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

    df = df.astype(object).where(pd.notnull(df), None)

    processed_events[event_name] = df.to_dict(orient="records")

  return processed_events


def get_match_metadata(parser):
  header = parser.parse_header()
  map_name = header.get("map_name", "unknown")

  max_tick_df = parser.parse_ticks(["tick"])
  end_tick = int(max_tick_df["tick"].max())

  start_tick = 0
  try:
    match_start_df = parser.parse_event("begin_new_match")  # find warmup phase
    if not match_start_df.empty:
      start_tick = int(match_start_df["tick"].iloc[0])
    else:
      round_start_df = parser.parse_event("round_start")  # actual round start
      if not round_start_df.empty:
        start_tick = int(round_start_df["tick"].iloc[0])
  except Exception:
    pass

  winner_team = 0  # 2 = T, 3 = CT
  score_t = 0
  score_ct = 0

  try:
    # Get score state at the final tick
    df_score = parser.parse_ticks(["team_num", "team_rounds_total"], ticks=[end_tick])

    t_data = df_score[df_score["team_num"] == 2]
    ct_data = df_score[df_score["team_num"] == 3]

    if not t_data.empty:
      score_t = int(t_data.iloc[0]["team_rounds_total"])
    if not ct_data.empty:
      score_ct = int(ct_data.iloc[0]["team_rounds_total"])

    if score_t > score_ct:
      winner_team = 2
    elif score_ct > score_t:
      winner_team = 3
  except Exception as e:
    print(f"Warning: Could not fetch final scores: {e}")

  winning_side_str = "Draw"  # only at 15:15

  if winner_team != 0:
    try:
      # We grab player teams at START and END
      # We filter for alive/connected players to ensure we get valid data
      props = ["player_steamid", "team_num"]

      # Fetch data for start and end
      df_players = parser.parse_ticks(props, ticks=[start_tick, end_tick])

      # Pick a "Reference Player" (The first steamid found at the start)
      start_data = df_players[df_players["tick"] == start_tick]

      if not start_data.empty:
        ref_player = start_data.iloc[0]  # Pick the first player found
        ref_steamid = ref_player["player_steamid"]
        ref_start_team = int(ref_player["team_num"])  # 2 or 3

        # Find that SAME player at the end
        end_data = df_players[
          (df_players["tick"] == end_tick)
          & (df_players["player_steamid"] == ref_steamid)
        ]

        if not end_data.empty:
          ref_end_team = int(end_data.iloc[0]["team_num"])

          ref_player_won = ref_end_team == winner_team

          if ref_player_won:
            if ref_start_team == 2:
              winning_side_str = "TeamStartedT"
            elif ref_start_team == 3:
              winning_side_str = "TeamStartedCT"
          else:  # other team
            if ref_start_team == 2:
              winning_side_str = "TeamStartedCT"  # Started T lost, so Started CT won
            elif ref_start_team == 3:
              winning_side_str = "TeamStartedT"

    except Exception as e:
      print(f"Warning: Could not track side switch: {e}")

  return (
    start_tick,
    end_tick,
    map_name,
    winner_team,
    score_t,
    score_ct,
    winning_side_str,
  )


def process_ticks(parser, start_tick, end_tick):
  wanted_ticks = np.arange(start_tick, end_tick + 1, TICK_INTERVAL)
  if wanted_ticks[-1] != end_tick:
    wanted_ticks = np.append(wanted_ticks, end_tick)

  # we need Name/Team for lookup, but won't save them in timeline
  props = [
    "player_steamid",
    "player_name",
    "team_num",
    "is_alive",
    "health",
    "X",
    "Y",
    "Z",
    "pitch",
    "yaw",
  ]

  print(f"Fetching {len(wanted_ticks)} ticks...")
  df = parser.parse_ticks(props, ticks=list(wanted_ticks))

  # Map SteamID -> Name/Team
  # this runs BEFORE we strip columns, so we can capture the names
  player_info = (
    df.groupby("player_steamid").first()[["player_name", "team_num"]].reset_index()
  )
  player_lookup = {}
  if (
    "player_steamid" in df.columns
    and "player_name" in df.columns
    and "team_num" in df.columns
  ):
    player_info = (
      df.groupby("player_steamid").first()[["player_name", "team_num"]].reset_index()
    )
    for _, row in player_info.iterrows():
      player_lookup[str(row["player_steamid"])] = {
        "name": row["player_name"],
        "team": int(row["team_num"]) if pd.notnull(row["team_num"]) else 0,
      }

  # to make sure we only get alive players during the ticks
  # df = df[df['is_alive'] == True]

  col_map = {
    "player_steamid": "sid",
    "health": "hp",
    "X": "x",
    "Y": "y",
    "Z": "z",
    "pitch": "p",
    "yaw": "rot",  # 'rot' for rotation
  }
  df = df.rename(columns=col_map)

  df = df.dropna(subset=["x", "y", "z", "p", "rot", "hp"])

  # rounding floats to make sure no crazy value (0.032193120310) happens
  df["x"] = df["x"].astype(float).round(2)
  df["y"] = df["y"].astype(float).round(2)
  df["z"] = df["z"].astype(float).round(2)
  # df["p"] = df["p"].round(0).astype(int)
  df["rot"] = df["rot"].round(0).astype(int)

  # rename columns to short keys

  # keep ONLY these columns + tick
  # we drop name, team, is_alive, and any other misc
  # keep_cols = ["tick", "sid", "hp", "x", "y", "z", "p", "rot"]
  keep_cols = ["tick", "sid", "hp", "x", "y", "z", "rot"]

  # Filter to ensure we only have existing columns (avoids errors if a tick is empty)
  existing_cols = [c for c in keep_cols if c in df.columns]
  df = df[existing_cols]

  # dead player comparison
  df = df.sort_values(by=["sid", "tick"])
  is_dead = df["hp"] <= 0
  was_dead_prev = is_dead.groupby(df["sid"]).shift(1, fill_value=False)
  df = df[~(is_dead & was_dead_prev)]
  df = df.sort_values(by=["tick"])

  # convert to timeline list
  timeline = []
  grouped = df.groupby("tick")

  for tick, group in grouped:
    # Drop 'tick' from the inner records
    group_data = group.drop(columns=["tick"])
    players_data = group_data.values.tolist()
    # tick to t
    timeline.append({"t": int(tick), "p": players_data})

  return timeline, player_lookup


def main():
  demo_path = get_demo_path()
  base_filename = os.path.basename(demo_path)
  absolute_file_path = get_absolute_path(f"{base_filename}.json")
  parser = DemoParser(demo_path)

  print("Parsing Metadata...")
  start_tick, end_tick, map_name, winner, t_score, ct_score, winning_start_side = (
    get_match_metadata(parser)
  )

  winner_name = "D"
  if winner == 2:
    winner_name = "2"  # T
  if winner == 3:
    winner_name = "3"  # CT

  duration = end_tick - start_tick

  event = {
    "type": "parse_meta_complete",
    "payload": {
      "outcome": winner_name,
      "file_path": absolute_file_path,
      "length_ticks": duration,
      # fetch time server already has
      # match code server already has
      "map": map_name,
      "tick_interval": TICK_INTERVAL,
      "score_t": t_score,
      "score_ct": ct_score,
    },
  }

  print(f"Final Score: T {t_score} - {ct_score} CT")
  print(f"Winner Faction: {winner_name}")
  print(f"Actual Team Winner: {winning_start_side}")  # e.g. "TeamStartedCT"

  print(f"DATA_OUTPUT:{json.dumps(event)}", flush=True)

  meta_payload = {
    "filename": base_filename,
    "map": map_name,
    "interval": TICK_INTERVAL,
    "length_ticks": duration,
    "winner_team": winner,
    "winner_name": winner_name,
    "won_by_team_that_started_as": winning_start_side,
    "score_t": t_score,
    "score_ct": ct_score,
    "final_score": f"{t_score}:{ct_score}",
  }

  # print("Writing metadata file...")
  # save_json({"meta": meta_payload}, f"{base_filename}_meta.json")

  print("Processing Ticks & Events (this may take a while)...")
  ticks_data, player_lookup = process_ticks(parser, start_tick, end_tick)
  events_data = parse_game_events(parser, start_tick)

  replay_json = {
    "meta": meta_payload,
    "players": player_lookup,
    "timeline": ticks_data,
    "events": events_data,
  }

  # Save the full replay (overwriting or creating a new file)
  save_json(replay_json, absolute_file_path)

  # delete meta file
  # meta_path = os.path.join(OUTPUT_FOLDER, f"{base_filename}_meta.json")
  # if os.path.exists(meta_path):
  #     os.remove(meta_path)

  # Cleanup demo file
  os.remove(demo_path)


if __name__ == "__main__":
  main()
