## QUERIES FOR SONGPLAY ANALYSIS

# Most frequently played song within the app
frequent_song_query = """SELECT s.title,a.artist_name,COUNT(*) \
FROM songplay sp \
JOIN songs s ON s.song_id = sp.song_id \
JOIN artists a ON a.artist_id = sp.artist_id \
GROUP BY s.title, a.artist_name \
ORDER BY count desc"""

# Weekday that had the most song plays
weekday_song_query = """SELECT weekday,COUNT(*) \
FROM "time" t \
GROUP BY weekday \
ORDER BY count desc
"""

# 5 top active users on the app the most
five_most_active_user_query = """SELECT u.first_name, u.last_name,u.level, COUNT(*) \
FROM songplay sp \
JOIN users u ON u.user_id = sp.user_id \
GROUP BY u.first_name , u.last_name,u.level \
ORDER BY count desc \
LIMIT 5
"""

# Male/Female Free/Paid usage rate
male_female_query = """SELECT u.gender,u.level,COUNT(*)
FROM songplay sp \
JOIN users u ON u.user_id = sp.user_id \
GROUP BY u.gender,u.level"""

# Songplay location query
location_play_query = """SELECT location,COUNT(*) \
FROM songplay \
GROUP BY location \
ORDER BY count DESC"""

# Platform query
platform_access_query = """SELECT user_agent, COUNT(*) \
FROM songplay \
GROUP BY user_agent \
ORDER BY count DESC"""
