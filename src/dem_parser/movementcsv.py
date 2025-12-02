from demoparser2 import DemoParser
import pandas as pd 

parser = DemoParser("furia-vs-falcons-m1-inferno-11-16.dem")

event_names = [
    "begin_new_match", "round_start", "round_end", "round_mvp", 
    "player_death", "bomb_planted", "bomb_defused", "hostage_rescued", 
    "weapon_fire", "flashbang_detonate", "hegrenade_detonate", 
    "molotov_detonate", "smokegrenade_detonate", "player_hurt", 
    "player_blind"
]

#Parse Events
all_events = [
    (event_name, parser.parse_event(event_name))
    for event_name in event_names
]

# Find match start tick
begin_new_match_df = next((df for event_name, df in all_events if event_name == 'begin_new_match'), None)
match_start_tick = begin_new_match_df['tick'].iloc[0] if begin_new_match_df is not None else 0

# Filter out events before the match start
filtered_events = [(event_name, df[df['tick'] >= match_start_tick]) for event_name, df in all_events]

wanted_props = ["equipment_value_this_round", "cash_spent_this_round", "is_alive", "team_num", "player_name", "score", "player_steamid", "X", "Y"]
tick_values = set()
for _, df in filtered_events:
    if df is not None and not df.empty:
     tick_values.update(df['tick'].unique())

tick_values = sorted(tick_values)
all_ticks = parser.parse_ticks(wanted_props, ticks=tick_values)

# Convert ticks to a map
all_ticks_map = {}
for row in all_ticks.itertuples():
    all_ticks_map.setdefault(row.tick, []).append(row)

#End of game tick
if all_ticks.empty:
    scoreboard_df = pd.DataFrame()
else:
    final_tick = all_ticks["tick"].max()
    scoreboard_rows = all_ticks_map.get(final_tick, [])
    scoreboard_df = pd.DataFrame([r._asdict() for r in scoreboard_rows])

# Access ticks
game_end_events = next((df for event_name, df in filtered_events if event_name == 'round_end'), None)
shot_events = next((df for event_name, df in filtered_events if event_name == 'weapon_fire'), None)
hit_events = next((df for event_name, df in filtered_events if event_name == 'player_hurt'), None)
flash_events = next((df for event_name, df in filtered_events if event_name == 'player_blind'), None)

print("Scoreboard at final tick:")
print(scoreboard_df[["player_name", "team_num", "score", "cash_spent_this_round"]])

print("\nHit events (head):")
print(hit_events.head() if hit_events is not None else "No hit events")
