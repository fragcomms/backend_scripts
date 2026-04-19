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

  warmup_start_tick = -1
  warmup_end_tick = -1
  try:
    warmup_start_df = parser.parse_event("warmup_period_start")
    if not warmup_start_df.empty:
      warmup_start_tick = int(warmup_start_df["tick"].iloc[0])

    warmup_end_df = parser.parse_event("warmup_period_end")
    if not warmup_end_df.empty:
      # Use .iloc[-1] to get the last warmup end, in case it was restarted
      warmup_end_tick = int(warmup_end_df["tick"].iloc[-1])
  except Exception as e:
    print(f"Warning: Could not fetch warmup events: {e}")

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
    warmup_start_tick,
    warmup_end_tick,
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


def calculate_advanced_stats(parser, start_tick, end_tick):
  # Notice we no longer need 'total_rounds_played'
  events_df = parser.parse_events(["player_hurt", "player_death", "round_start"])

  hurt_df = pd.DataFrame()
  death_df = pd.DataFrame()
  round_start_df = pd.DataFrame()

  for event_name, df in events_df:
    if df is None or df.empty:
      continue
    if event_name == "player_hurt":
      hurt_df = df
    elif event_name == "player_death":
      death_df = df
    elif event_name == "round_start":
      round_start_df = df

  if hurt_df.empty or death_df.empty or round_start_df.empty:
    return {}

  # 1. FILTER OUT WARMUP & POST-MATCH
  hurt_df = hurt_df[
    (hurt_df["tick"] >= start_tick) & (hurt_df["tick"] <= end_tick)
  ].copy()
  death_df = death_df[
    (death_df["tick"] >= start_tick) & (death_df["tick"] <= end_tick)
  ].copy()
  round_start_df = round_start_df[round_start_df["tick"] >= start_tick].copy()

  # 2. BUILD PROPER ROUND INTERVALS
  # A round spans from its start_tick to the NEXT round's start_tick (or match end)
  round_start_df = round_start_df.sort_values("tick")
  start_ticks = round_start_df["tick"].tolist()

  if not start_ticks:
    return {}

  round_intervals = []
  for i in range(len(start_ticks)):
    current_start = start_ticks[i]
    next_start = start_ticks[i + 1] if i + 1 < len(start_ticks) else end_tick + 1
    round_intervals.append((current_start, next_start))

  # Map every event to a specific round index
  def get_round_idx(tick):
    for idx, (start, end) in enumerate(round_intervals):
      if start <= tick < end:
        return idx
    return -1

  hurt_df["round"] = hurt_df["tick"].apply(get_round_idx)
  death_df["round"] = death_df["tick"].apply(get_round_idx)

  # Drop orphaned events that somehow didn't fit into a valid match round
  hurt_df = hurt_df[hurt_df["round"] != -1]
  death_df = death_df[death_df["round"] != -1]

  stats_per_player = {}

  def init_player(name):
    if pd.notna(name) and name not in stats_per_player:
      stats_per_player[name] = {
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
        "3k": 0,
        "4k": 0,
        "5k": 0,
      }

  start_states = parser.parse_ticks(["player_name", "team_num"], ticks=start_ticks)

  # 3. PROCESS EACH ROUND INDEPENDENTLY
  for r_idx in range(len(round_intervals)):
    r_hurts = hurt_df[hurt_df["round"] == r_idx]
    r_deaths = death_df[death_df["round"] == r_idx].sort_values("tick")
    r_start_tick = start_ticks[r_idx]

    round_players = set()
    player_teams = {}

    # Get team alignments at the exact start of the round
    if not start_states.empty:
      r_state = start_states[start_states["tick"] == r_start_tick]
      for _, row in r_state.iterrows():
        pname = row.get("player_name")
        team = row.get("team_num")
        if pd.notna(pname):
          round_players.add(pname)
          if pd.notna(team):
            player_teams[pname] = team

    # Add anyone who dealt damage or died, in case they reconnected mid-round
    active_in_round = (
      set(r_hurts["user_name"].dropna())
      .union(set(r_hurts["attacker_name"].dropna()))
      .union(set(r_deaths["user_name"].dropna()))
    )
    round_players.update(active_in_round)

    # --- First Kills / Deaths ---
    if not r_deaths.empty:
      for _, fk_event in r_deaths.iterrows():
        att_fk = fk_event.get("attacker_name")
        vic_fd = fk_event.get("user_name")

        att_team = player_teams.get(att_fk, "att_unknown")
        vic_team = player_teams.get(vic_fd, "vic_unknown")

        if (
          pd.notna(att_fk)
          and pd.notna(vic_fd)
          and att_fk != vic_fd
          and att_team != vic_team
        ):
          init_player(att_fk)
          stats_per_player[att_fk]["first_kills"] += 1
          init_player(vic_fd)
          stats_per_player[vic_fd]["first_deaths"] += 1
          break

    round_kast_achieved = set()
    round_kill_tally = {}

    # --- Deaths, Kills, Assists, Trades ---
    for _, death in r_deaths.iterrows():
      vic = death.get("user_name")
      att = death.get("attacker_name")
      ass = death.get("assister_name")
      hs = death.get("headshot")
      tick = int(death.get("tick"))

      vic_team = player_teams.get(vic, "vic_unknown")
      att_team = player_teams.get(att, "att_unknown")
      ass_team = player_teams.get(ass, "ass_unknown")

      # KILLS
      if pd.notna(att) and att != vic and att_team != vic_team:
        init_player(att)
        stats_per_player[att]["kills"] += 1
        if hs:
          stats_per_player[att]["hs_kills"] += 1
        round_kast_achieved.add(att)

        round_kill_tally[att] = round_kill_tally.get(att, 0) + 1

      # ASSISTS
      if pd.notna(ass) and ass_team != vic_team:
        init_player(ass)
        stats_per_player[ass]["assists"] += 1
        round_kast_achieved.add(ass)

      # DEATHS & TRADES
      if pd.notna(vic):
        init_player(vic)
        stats_per_player[vic]["deaths"] += 1

        # Trade check (Killer is avenged within ~5 seconds)
        killer_death = r_deaths[
          (r_deaths["user_name"] == att)
          & (r_deaths["tick"] > tick)
          & (r_deaths["tick"] <= tick + 320)
        ]
        if not killer_death.empty:
          round_kast_achieved.add(vic)

    # --- Multi-kills ---
    for player_name, round_kills in round_kill_tally.items():
      if round_kills == 3:
        stats_per_player[player_name]["3k"] += 1
      elif round_kills == 4:
        stats_per_player[player_name]["4k"] += 1
      elif round_kills >= 5:
        stats_per_player[player_name]["5k"] += 1

    # --- Damage ---
    # Sort hurts chronologically so damage applies in the correct order
    r_hurts = r_hurts.sort_values("tick")

    # Every player enters the round with a strict maximum of 100 HP to "give"
    victim_health_pool = {p: 100 for p in round_players}

    for _, hurt in r_hurts.iterrows():
      att = hurt.get("attacker_name")
      vic = hurt.get("user_name")

      try:
        dmg = int(hurt.get("dmg_health", 0))
      except (ValueError, TypeError):
        dmg = 0

      wep = hurt.get("weapon")

      if pd.isna(vic):
        continue

      vic_team = player_teams.get(vic, "vic_unknown")
      att_team = player_teams.get(att, "att_unknown")

      # 1. Cap the damage at whatever health the victim actually has left
      available_hp = victim_health_pool.get(vic, 100)
      actual_dmg = min(dmg, available_hp)

      # 2. Subtract this damage from the victim's pool so overkill/corpse-hitting is nullified
      victim_health_pool[vic] = available_hp - actual_dmg

      # 3. Only award damage if it's a valid enemy attacker
      if pd.notna(att) and att != vic and att_team != vic_team and actual_dmg > 0:
        init_player(att)
        stats_per_player[att]["damage"] += actual_dmg
        if str(wep) in ["hegrenade", "inferno", "molotov", "incgrenade"]:
          stats_per_player[att]["util_damage"] += actual_dmg

    # --- Rounds Played & Survived (KAST) ---
    r_death_names = set(r_deaths["user_name"].dropna())

    for p in round_players:
      init_player(p)
      stats_per_player[p]["rounds_played"] += 1

      if p not in r_death_names:
        round_kast_achieved.add(p)

      if p in round_kast_achieved:
        stats_per_player[p]["kast_rounds"] += 1

  # Final Percentage Math
  for name, stats in stats_per_player.items():
    rp = max(stats["rounds_played"], 1)
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
  advanced_stats = calculate_advanced_stats(parser, start_tick, end_tick)

  print("Processing Ticks & Events (this may take a while)...")
  ticks_data, player_lookup, steamid_map = process_ticks(parser, start_tick, end_tick)
  events_data = parse_game_events(parser, start_tick, steamid_map)

  # Merge advanced stats into your player_lookup using the steamIDs
  for tiny_id, p_info in player_lookup.items():
    p_name = p_info["name"]

    if p_name in advanced_stats:
      p_info["advanced_stats"] = advanced_stats[p_name]
    else:
      print(f"Warning: No advanced stats found for {p_name}")

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
