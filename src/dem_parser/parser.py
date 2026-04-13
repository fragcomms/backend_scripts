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


def parse_game_events(parser, match_start_tick, steamid_map):
  event_names = [
    "weapon_fire",
    "player_death",
    "round_start",
    "round_end",
    "bomb_planted",
    # grenades
    "hegrenade_detonate",
    "flashbang_detonate",
    "smokegrenade_detonate",
    "decoy_detonate",
    "inferno_startburn",
    "inferno_expire",
    "inferno_extinguish",
  ]
  events_df = parser.parse_events(event_names, other=["game_time", "team_num"])

  def safe_map_sid(val):
    if pd.isna(val):
      return None
    return steamid_map.get(str(val).split(".")[0])

  processed_events = {}
  for event_name, df in events_df:
    if df is None or df.empty:
      continue

    if event_name in ["round_start", "round_end"]:
      buffer = 2000  # required to capture the first round tick start and ends
      df = df[df["tick"] >= (match_start_tick - buffer)]
    else:
      df = df[df["tick"] >= match_start_tick]

    wanted_cols = ["t"]  # tick always required
    rename_map = {"tick": "t"}

    if event_name == "player_death":
      rename_map.update(
        {
          "user_steamid": "vic",  # Victim
          "attacker_steamid": "att",  # Attacker
          "assister_steamid": "ass",  # assister, null if none
          "weapon": "wep",
          "headshot": "hs",
        }
      )
      wanted_cols += ["vic", "att", "ass", "wep", "hs"]

    elif event_name == "weapon_fire":
      rename_map.update(
        {
          "user_steamid": "id",  # shooter
          "weapon": "wep",
        }
      )
      wanted_cols += ["id", "wep"]

    elif event_name == "bomb_planted":
      rename_map.update(
        {
          "user_steamid": "id",  # planter
          "site": "site",
        }
      )
      wanted_cols += ["id", "site"]

    elif event_name == "round_end":
      rename_map.update(
        {
          "winner": "winner",  # which side won the round
          "reason": "reason",
        }
      )
      wanted_cols += ["winner", "reason"]

    elif event_name == "round_start":
      rename_map.update(
        {
          "timelimit": "time",
        }
      )
      wanted_cols += ["time"]

    elif event_name in [
      "hegrenade_detonate",
      "flashbang_detonate",
      "smokegrenade_detonate",
      "decoy_detonate",
      "inferno_startburn",
      "inferno_expire",
      "inferno_extinguish",
    ]:
      rename_map.update(
        {"user_steamid": "id", "entityid": "eid", "x": "x", "y": "y", "z": "z"}
      )
      wanted_cols += ["id", "eid", "x", "y", "z"]

    # Apply Rename
    df = df.rename(columns=rename_map)

    # convert steamid to tiny ints
    for col in ["vic", "att", "ass", "id"]:
      if col in df.columns:
        df[col] = df[col].apply(safe_map_sid)

    # keep columns that are wanted and discard rest
    existing_cols = [c for c in wanted_cols if c in df.columns]
    df = df[existing_cols]

    for col in df.columns:
      if col in ["t", "vic", "att", "ass", "id", "eid"]:
        df[col] = df[col].fillna(-1).astype(int)
      elif col in ["x", "y", "z"]:
        df[col] = df[col].astype(float).round(2)
      elif col == "hs":
        df[col] = df[col].fillna(False).astype(bool)
      elif col in ["wep", "winner", "reason", "site", "time"]:
        df[col] = df[col].fillna("").astype(str)

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
  ############### PLAYER PROCESSING
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
    # "pitch",
    "yaw",
  ]

  print(f"Fetching {len(wanted_ticks)} ticks...")
  df = parser.parse_ticks(props, ticks=list(wanted_ticks))

  # Map SteamID -> Name/Team
  # this runs BEFORE we strip columns, so we can capture the names
  player_lookup = {}
  steamid_map = {}

  if (
    "player_steamid" in df.columns
    and "player_name" in df.columns
    and "team_num" in df.columns
  ):
    player_info = (
      df.groupby("player_steamid").first()[["player_name", "team_num"]].reset_index()
    )
    current_id = 0
    for _, row in player_info.iterrows():
      original_sid = str(row["player_steamid"]).split(".")[0]

      steamid_map[original_sid] = current_id
      player_lookup[current_id] = {
        "name": row["player_name"],
        "team": int(row["team_num"]) if pd.notnull(row["team_num"]) else 0,
        "sid": original_sid,
      }
      current_id += 1

  col_map = {
    "player_steamid": "sid",
    "health": "hp",
    "X": "x",
    "Y": "y",
    "Z": "z",
    # "pitch": "p",
    "yaw": "rot",  # 'rot' for rotation
  }
  df = df.rename(columns=col_map)

  # map steamids to tiny ints
  df["sid"] = df["sid"].apply(
    lambda x: steamid_map.get(str(x).split(".")[0]) if pd.notnull(x) else None
  )

  # df = df.dropna(subset=["x", "y", "z", "p", "rot", "hp"])
  df = df.dropna(subset=["x", "y", "z", "rot", "hp"])

  # rounding floats to make sure no crazy value (0.032193120310) happens
  df["sid"] = df["sid"].astype(int)
  df["hp"] = df["hp"].astype(int)
  df["x"] = df["x"].astype(float).round(2)
  df["y"] = df["y"].astype(float).round(2)
  df["z"] = df["z"].astype(float).round(2)
  # df["p"] = df["p"].round(0).astype(int)
  df["rot"] = df["rot"].astype(int)

  # rename columns to short keys

  # keep ONLY these columns + tick
  # we drop name, team, is_alive, and any other misc
  # keep_cols = ["tick", "sid", "hp", "x", "y", "z", "p", "rot"]
  keep_cols = ["tick", "sid", "hp", "x", "y", "z", "rot"]

  # Filter to ensure we only have existing columns (avoids errors if a tick is empty)
  existing_cols = [c for c in keep_cols if c in df.columns]
  df = df[existing_cols]

  # dead player compact
  df = df.sort_values(by=["sid", "tick"])
  is_dead = df["hp"] <= 0
  was_dead_prev = is_dead.groupby(df["sid"]).shift(1, fill_value=False)
  df = df[~(is_dead & was_dead_prev)]
  df = df.sort_values(by=["tick"])

  ################### GRENADE PROCESSING
  g_grouped = None
  try:
    print("Fetching grenade flight paths")
    g_df = parser.parse_grenades()

    if not g_df.empty:
      g_rename = {
        "X": "x",
        "Y": "y",
        "Z": "z",
        "grenade_entity_id": "eid",
        "steamid": "sid",
        "grenade_type": "gtype",
      }
      g_df = g_df.rename(columns=g_rename)
      g_df = g_df[g_df["tick"].isin(wanted_ticks)]

      if "sid" in g_df.columns:
        g_df["sid"] = g_df["sid"].apply(
          lambda x: steamid_map.get(str(x).split(".")[0]) if pd.notnull(x) else -1
        )
      else:
        g_df["sid"] = -1

      # 1=HE, 2=Smoke, 3=Flash, 4=Decoy, 5=Molly/Incendiary
      def map_grenade_class(name):
        name = str(name).lower()
        if "hegrenade" in name:
          return 1
        if "smoke" in name:
          return 2
        if "flash" in name:
          return 3
        if "decoy" in name:
          return 4
        if "molotov" in name:
          return 5
        if "incendiary" in name:
          return 5
        return 0

      if "gtype" in g_df.columns:
        g_df["wep"] = g_df["gtype"].apply(map_grenade_class)
      else:
        g_df["wep"] = 0

      # for col in ["eid", "sid", "wep", "x", "y", "z"]:
      #   if col not in g_df.columns:
      #     g_df[col] = 0 if col in ["eid", "wep"] else (-1 if col == "sid" else 0.0)

      # g_df = g_df.dropna(subset=["x", "y", "z"])

      # actually round coordinates
      g_df["eid"] = g_df["eid"].fillna(0).astype(int)
      g_df["sid"] = g_df["sid"].fillna(-1).astype(int)
      g_df["wep"] = g_df["wep"].fillna(0).astype(int)
      g_df["x"] = g_df["x"].astype(float).round(2)
      g_df["y"] = g_df["y"].astype(float).round(2)
      g_df["z"] = g_df["z"].astype(float).round(2)

      g_df = g_df.dropna(subset=["x", "y"])
      g_grouped = g_df.groupby("tick")
  except Exception as e:
    print(f"Error: Failed to parse grenade paths: {e}")

  # TIMELINE
  # convert to timeline list
  timeline = []
  grouped = df.groupby("tick")

  for tick, group in grouped:
    # Drop 'tick' from the inner records
    players_data = [
      [
        int(sid),
        int(hp),
        round(float(x), 2),
        round(float(y), 2),
        round(float(z), 2),
        int(rot),
      ]
      for sid, hp, x, y, z, rot in zip(
        group["sid"], group["hp"], group["x"], group["y"], group["z"], group["rot"]
      )
    ]

    tick_obj = {"t": int(tick), "p": players_data}
    # tick to t
    if g_grouped is not None and tick in g_grouped.groups:
      g_data = g_grouped.get_group(tick).drop(columns=["tick"])
      tick_obj["g"] = [
        [
          int(eid),
          int(sid),
          int(wep),
          round(float(x), 2),
          round(float(y), 2),
          round(float(z), 2),
        ]
        for eid, sid, wep, x, y, z in zip(
          g_data["eid"],
          g_data["sid"],
          g_data["wep"],
          g_data["x"],
          g_data["y"],
          g_data["z"],
        )
      ]

    timeline.append(tick_obj)

  return timeline, player_lookup, steamid_map


def calculate_advanced_stats(parser):
  events_df = parser.parse_events(
    ["player_hurt", "player_death", "round_end"],
    other=["total_rounds_played", "team_num", "game_time"],
  )

  # Extract the dataframes
  hurt_df = pd.DataFrame()
  death_df = pd.DataFrame()
  round_end_df = pd.DataFrame()

  for event_name, df in events_df:
    if df is None or df.empty:
      continue
    if event_name == "player_hurt":
      hurt_df = df
    elif event_name == "player_death":
      death_df = df
    elif event_name == "round_end":
      round_end_df = df

  if hurt_df.empty or death_df.empty or round_end_df.empty:
    return {}

  # Standardize round numbers (total_rounds_played increments when round ends)
  # We add 1 so round 0 becomes round 1
  hurt_df["round"] = hurt_df["total_rounds_played"] + 1
  death_df["round"] = death_df["total_rounds_played"] + 1
  round_end_df["round"] = round_end_df["total_rounds_played"] + 1

  stats_per_player = {}

  def init_player(sid):
    if sid not in stats_per_player and pd.notna(sid):
      stats_per_player[sid] = {
        "rounds_played": 0,
        "kills": 0,
        "assists": 0,
        "deaths": 0,
        "hs_kills": 0,
        "damage": 0,
        "util_damage": 0,
        "first_kills": 0,
        "first_deaths": 0,
        "kast_rounds": 0,
        "1v1_won": 0,
        "1v2_won": 0,
        "1v3_won": 0,
        "1v4_won": 0,
        "1v5_won": 0,
        "per_round_data": {},
      }

  # Group by rounds
  rounds = round_end_df["round"].unique()

  for r in rounds:
    r_hurts = hurt_df[hurt_df["round"] == r]
    r_deaths = death_df[death_df["round"] == r].sort_values("tick")

    # Get the winner of the round
    r_end = round_end_df[round_end_df["round"] == r]
    winner_team = r_end.iloc[0]["winner"] if not r_end.empty else None

    # Track first kill/death
    if not r_deaths.empty:
      fk_event = r_deaths.iloc[0]
      att_fk = fk_event.get("attacker_steamid")
      vic_fd = fk_event.get("user_steamid")

      if pd.notna(att_fk):
        init_player(att_fk)
        stats_per_player[att_fk]["first_kills"] += 1
      if pd.notna(vic_fd):
        init_player(vic_fd)
        stats_per_player[vic_fd]["first_deaths"] += 1

    # Track Kills, Deaths, Assists, HS, KAST Trades, and 1vX
    alive_players = {2: set(), 3: set()}  # 2 = T, 3 = CT
    # Populate initial alive players for this round (Requires basic team mapping)
    # For simplicity, we add them to the set as they appear in the event logs
    for _, row in r_deaths.iterrows():
      if pd.notna(row["user_steamid"]):
        alive_players[row["team_num"]].add(row["user_steamid"])

    round_kast_achieved = set()

    for _, death in r_deaths.iterrows():
      vic = death.get("user_steamid")
      att = death.get("attacker_steamid")
      ass = death.get("assister_steamid")
      hs = death.get("headshot")
      tick = death.get("tick")

      # Remove victim from alive players
      vic_team = death.get("team_num")
      if vic in alive_players.get(vic_team, set()):
        alive_players[vic_team].remove(vic)

      # Check for 1vX Situation (Victim dies, leaving 1 guy alive on their team)
      # Actually, we want to check if the ATTACKER'S team is in a 1vX.
      if pd.notna(att):
        att_team = 2 if vic_team == 3 else 3  # Opposite team
        if len(alive_players.get(att_team, set())) == 1:
          last_alive = list(alive_players[att_team])[0]
          # if the intial team wins, it is a 1vX clutch
          if att_team == winner_team and r_deaths.iloc[-1]["user_steamid"] == vic:
            enemies_alive_at_start_of_clutch = (
              len(alive_players.get(vic_team, set())) + 1
            )
            clutch_key = f"1v{min(enemies_alive_at_start_of_clutch, 5)}_won"
            init_player(last_alive)
            stats_per_player[last_alive][clutch_key] += 1

      if pd.notna(att) and att != vic:  # Kill
        init_player(att)
        stats_per_player[att]["kills"] += 1
        if hs:
          stats_per_player[att]["hs_kills"] += 1
        round_kast_achieved.add(att)  # KAST: Kill

      if pd.notna(ass):  # Assist
        init_player(ass)
        stats_per_player[ass]["assists"] += 1
        round_kast_achieved.add(ass)  # KAST: Assist

      if pd.notna(vic):  # Death
        init_player(vic)
        stats_per_player[vic]["deaths"] += 1

        # KAST: Trade check (Did the killer die within ~5 seconds?)
        # 5 seconds * 64 ticks = 320 ticks
        killer_death = r_deaths[
          (r_deaths["user_steamid"] == att)
          & (r_deaths["tick"] > tick)
          & (r_deaths["tick"] <= tick + 320)
        ]
        if not killer_death.empty:
          round_kast_achieved.add(vic)  # KAST: Traded

    # Calculate Damage
    for _, hurt in r_hurts.iterrows():
      att = hurt.get("attacker_steamid")
      vic = hurt.get("user_steamid")
      dmg = hurt.get("dmg_health")
      wep = hurt.get("weapon")

      # Don't count self-damage or team damage
      if pd.notna(att) and pd.notna(vic) and att != vic:
        # NOTE: You should ideally check if att_team != vic_team here
        init_player(att)
        stats_per_player[att]["damage"] += dmg

        if str(wep) in ["hegrenade", "inferno", "molotov", "incgrenade"]:
          stats_per_player[att]["util_damage"] += dmg

    # Final KAST processing for Survived
    # Anyone who participated in the round but didn't die gets Survived
    all_players_in_round = set(r_hurts["user_steamid"].dropna()).union(
      set(r_hurts["attacker_steamid"].dropna())
    )

    for p in all_players_in_round:
      init_player(p)
      stats_per_player[p]["rounds_played"] += 1

      # KAST: Survived
      if p not in r_deaths["user_steamid"].values:
        round_kast_achieved.add(p)

      if p in round_kast_achieved:
        stats_per_player[p]["kast_rounds"] += 1

  # Post-process into final metrics
  for sid, stats in stats_per_player.items():
    rp = max(stats["rounds_played"], 1)  # avoid division by zero
    kills = max(stats["kills"], 1)

    stats["kast_pct"] = round((stats["kast_rounds"] / rp) * 100, 1)
    stats["adr"] = round(stats["damage"] / rp, 1)
    stats["hs_pct"] = round((stats["hs_kills"] / kills) * 100, 1)
    stats["util_adr"] = round(stats["util_damage"] / rp, 1)

  return stats_per_player


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

  print("Calculating Advanced Stats (ADR, KAST, 1vX)...")
  advanced_stats = calculate_advanced_stats(parser)

  print("Processing Ticks & Events (this may take a while)...")
  ticks_data, player_lookup, steamid_map = process_ticks(parser, start_tick, end_tick)
  events_data = parse_game_events(parser, start_tick, steamid_map)

  # Merge advanced stats into your player_lookup using the steamIDs
  for tiny_id, p_info in player_lookup.items():
    sid_str = p_info["sid"]
    # Convert to float/int to match the dataframe parsed IDs
    sid_numeric = int(sid_str) if sid_str.isdigit() else float(sid_str)

    if sid_numeric in advanced_stats:
      p_info["advanced_stats"] = advanced_stats[sid_numeric]

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
