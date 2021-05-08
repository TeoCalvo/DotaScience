select match_id,
        max(case when team = 0 then team_name end) as radiant_team,
        max(case when team = 1 then team_name end) as dire_team
from raw_tb_live_games

group by match_id