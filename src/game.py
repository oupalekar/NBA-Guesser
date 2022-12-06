from update import create_db_connection, execute_query_get
from queue import Queue
import pandas as pd
import random

def chooseRandomPlayer(connection):
    MAX_PREVIOUS_PLAYERS = 30
    previous_players = []
    try:
        with open("previous_players.txt", 'r') as file:
            string_of_player_ids = file.read()
            previous_players = string_of_player_ids.split(',')
    except:
        print("File does not exist.")
    
    # print(previous_players)
    player_id_query = "SELECT PlayerID FROM Player;"
    player_ids = pd.read_sql_query(player_id_query, connection)['PlayerID'].tolist()
    # print(player_ids['PlayerID'].tolist())
    while True:
        player_id_random = random.choice(player_ids)
        if str(player_id_random) not in previous_players:
            try:
                player_years = pd.read_sql_query(f"SELECT * FROM Player JOIN Sign USING(PlayerID) WHERE PlayerID = {player_id_random}", connection)
                player_years.sample().values[0]
                break
            except:
                pass
        else:
            print("Found repeated id")
    
    previous_players.append(str(player_id_random))
    if len(previous_players) > MAX_PREVIOUS_PLAYERS:
        previous_players.pop(0)
    
    # print(previous_players)
    with open("previous_players.txt", "w") as file:
        string_of_player_id = ""
        for id in previous_players:
            string_of_player_id += str(id) + ","
        file.write(string_of_player_id[:-1])
    
    return player_id_random


def getStatistics(connection, player_id):
    player_years = pd.read_sql_query(f"SELECT * FROM Player JOIN Sign USING(PlayerID) WHERE PlayerID = {player_id_random}", connection) 
    team_id, year = player_years.sample().values[0][-2:]
    team_wins_query = f'SELECT COUNT(*) FROM (SELECT g1.GameId FROM Team t1 JOIN Game g1 ON(t1.TeamId = g1.HomeTeamId) WHERE (g1.HomeScore > g1.AwayScore AND g1.HomeTeamId = {team_id} AND t1.Year = {year}) UNION SELECT g2.GameId FROM Team t2 JOIN Game g2 ON(t2.TeamId = g2.AwayTeamId) WHERE (g2.AwayScore > g2.HomeScore AND g2.AwayTeamId = {team_id} AND t2.Year = {year})) AS winning_games;'
    team_wins = execute_query_get(connection, team_wins_query)[0][0]

    max_pts_query = f'SELECT g1.Date, g1.HomeScore, g1.AwayScore, g1.HomeTeamId, g1.AwayTeamId, b1.Pts FROM Player p1 JOIN BoxScore b1 USING(PlayerId) JOIN Game g1 USING(GameId) JOIN Team t1 ON(t1.TeamId = g1.HomeTeamId) WHERE t1.Year = {year} AND p1.PlayerId = {player_id} AND b1.Pts = (SELECT MAX(b2.Pts) FROM Player p2 JOIN BoxScore b2 USING(PlayerId) JOIN Game g2 USING(GameId) JOIN Team t2 ON (t2.TeamId = g2.HomeTeamId) WHERE p2.PlayerId = {player_id} AND t2.Year = {year});'

    max_pts = execute_query_get(connection, max_pts_query)[0][5]

    # print(team_wins, team_id, year)
    average_stat = aggregateStats(connection, player_id, year)
    print(average_stat)
    print(year)
    print(team_wins)
    print(max_pts)


def aggregateStats(connection, player_id, year):
    stats = ["Pts","Asts","Rebs", "Blks","Stls", "TOs","FGM", "FGA","TPM","TPA", "FTM" ,"FTA"]
    yearCodes = ("2" + str(year % 2000) + "00000", "2" + str(year % 2000 + 1) + "00000")

    stat = random.choice(stats)
    print(stat)

    average_stat_query = f'SELECT avg({stat}) FROM Player NATURAL JOIN BoxScore WHERE PlayerID = {player_id} AND GameID >= {yearCodes[0]} AND GameID < {yearCodes[1]};'
    average_stat = execute_query_get(connection, average_stat_query)
    # print(player_id, year, stat, average_stat)
    # if stat in ["FGM", "FGA","TPM","TPA", "FTM" ,"FTA"]:
    #     average_stat = str(float(average_stat[0][0])) + "%"
    #     return average_stat
    return round(float(average_stat[0][0]),2)

# def get_team_wins(connection, player_id):
#     team_wins_query = "SELECT COUNT(*) FROM (SELECT g1.GameId FROM Team t1 JOIN Game g1 ON(t1.TeamId = g1.HomeTeamId) WHERE (g1.HomeScore > g1.AwayScore AND g1.HomeTeamId = 1 AND t1.Year = 2020) UNION SELECT g2.GameId FROM Team t2 JOIN Game g2 ON(t2.TeamId = g2.AwayTeamId) WHERE (g2.AwayScore > g2.HomeScore AND g2.AwayTeamId = 1 AND t2.Year = 2020));"


if __name__ == '__main__':
    connection = create_db_connection("34.136.27.137", "root", "PickAndRoll")
    player_id_random = chooseRandomPlayer(connection)
    print(player_id_random)
    getStatistics(connection, player_id_random)

    # aggregateStats(None, None, 2020)