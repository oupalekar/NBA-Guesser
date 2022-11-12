from nba_api.stats.endpoints.commonteamroster import CommonTeamRoster
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.leaguegamelog import LeagueGameLog
from nba_api.stats.endpoints.boxscoretraditionalv2 import BoxScoreTraditionalV2
from nba_api.stats.static import teams
import mysql.connector
from mysql.connector import Error
import pandas as pd
import getpass
import time
from tqdm import tqdm
import time
import warnings
import sys
import logging as log

def create_db_connection(host_name, user_name, db_name):
    """
    From: https://www.freecodecamp.org/news/connect-python-with-sql/
    """
    # user_password = getpass.getpass("Password")
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd="apple123",
            database=db_name
        )
        log.info("MySQL Database connection successful")
    except Error as err:
        log.info(f"Error: '{err}'")

    return connection

def execute_query_get(connection, query):
    """
    From: https://www.freecodecamp.org/news/connect-python-with-sql/
    """
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute(query)
        connection.commit()
        result = cursor.fetchall()
        # print("Query successful")
        return result

        # return result
    except Error as err:
        log.info(f"Error: '{err}'")

def execute_query_post(connection, query):
    """
    From: https://www.freecodecamp.org/news/connect-python-with-sql/
    """
    cursor = connection.cursor(buffered=True)
    try:
        cursor.execute(query)
        connection.commit()
        # print("Query successful")
        # return result
    except Error as err:
        log.info(f"Error: '{err}'")

def CheckPlayers(df, connection):
    items = df['PLAYER_ID']
    df_items = pd.DataFrame(items, columns=['PlayerId'])

    sql = "select * from Player;"
    df_db = pd.read_sql_query(sql, connection)
    df_db.PlayerId = df_db.PlayerId.astype(str)

    df_final = df_items.merge(df_db,on='PlayerId',how='left')
    df_final = df_final.loc[lambda x:x.FirstName.isnull()]
    new_players = df_final['PlayerId'].tolist()
    return new_players

def InsertPlayers(new_players, connection):
    data = pd.DataFrame(columns=['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'HEIGHT', 'WEIGHT'])

    for i in tqdm(range(len(new_players))):
        player = CommonPlayerInfo(player_id=new_players[i]).common_player_info.get_data_frame()
        player = player[['PERSON_ID', 'FIRST_NAME', 'LAST_NAME', 'HEIGHT', 'WEIGHT']]
        data = pd.concat([data,player], axis=0)

    for player in list(data.itertuples(index=False, name=None)):
        player_query = f"""INSERT INTO Player(PlayerId, FirstName, LastName, Height, Weight)
        VALUES{player}"""
        execute_query_post(connection, player_query)

def SignPlayers(df = None):
    currentSeason = int(open("current_season.txt", 'r').readline())
    team_id_start = (currentSeason % 2020) * 30 + 1

    all_teams = teams.get_teams()
    if (df is None):
        df = pd.DataFrame(columns=['TeamID', 'SEASON', 'LeagueID', 'PLAYER', 'PLAYER_SLUG', 'NUM', 'POSITION', 'HEIGHT', 'WEIGHT', 'BIRTH_DATE', 'AGE', 'EXP', 'SCHOOL', 'PLAYER_ID'])
        for l in tqdm(range(len(all_teams))):
            rosters = CommonTeamRoster(season = "2022", team_id= all_teams[l]['id'])
            df = pd.concat([df, rosters.common_team_roster.get_data_frame()], axis = 0)
            time.sleep(.600)

        # df.to_csv("csv/sign2022.csv")    
    # return df
    df = df.rename(columns = {'SEASON': 'Year', 'TEAM_ID': 'TeamId', 'PLAYER_ID': 'PlayerId'})
    # print(df)
    df = df.drop(df.columns.difference(['Year', "TeamId",'PlayerId']), 1)
    # print(df)
    # df.drop(['LeagueID', 'PLAYER', 'PLAYER_SLUG', 'NUM', 'POSITION', 'HEIGHT', 'WEIGHT', 'BIRTH_DATE', 'AGE', 'EXP', 'SCHOOL', 'NICKNAME', 'HOW_ACQUIRED'], axis = 1, inplace=True)
    converter = {}
    for l in range(len(all_teams)):
        converter[all_teams[l]['id']] = team_id_start + l
    df['TeamId'] = df['TeamId'].apply(lambda x: converter[x])
    df = df.drop_duplicates()
    for signings in list(df.itertuples(index=False, name=None)):
        # print(signings)
        signing = list(signings)
        signing.append(currentSeason) 
        # print(signing)
        sign_query = f"""INSERT INTO Sign(TeamId, PlayerId, Year)
        VALUES{tuple(signing)}"""
        execute_query_post(connection, sign_query)

def InputBoxScoreAndPlayerPlay(boxscores, connection):
    get_last_boxscore_value_sql = "SELECT * FROM BoxScore ORDER BY BoxScoreId DESC LIMIT 1"
    last_game_accessed = execute_query_get(connection, get_last_boxscore_value_sql)[0][0] + 1

    box_scores = boxscores.drop(['TEAM_ID', 'TEAM_CITY', "TEAM_ABBREVIATION" , 'PLAYER_NAME' ,'NICKNAME',
        'START_POSITION', 'COMMENT', 'MIN', 'FG_PCT' ,
        'FG3_PCT', 'FT_PCT', 'OREB' ,'DREB' ,
        'PF' , 'PLUS_MINUS'], axis=1)
    box_scores.fillna(0, inplace = True)
    
    players_play = boxscores.drop(boxscores.columns.difference(['GAME_ID','PLAYER_ID']), 1)
    players_play.fillna(0, inplace = True)

    box_table = pd.read_sql_query("SELECT * FROM BoxScore;", connection)

    log.info("Adding BoxScore and PlayerPlay")
    pp = list(players_play.itertuples(index=False, name=None))
    boxscore = list(box_scores.itertuples(index=False, name=None))
    for index in tqdm(range(len(pp))):
        # print(pp[index])
        pp_query = f"""INSERT INTO PlayerPlay(GameId, PlayerId)
        VALUES{pp[index]};
        """
        execute_query_post(connection, pp_query)

        bs = list(boxscore[index])
        bs.append(last_game_accessed)
        bs = tuple(bs)
        last_game_accessed += 1
        
        if bs[0] in box_table["BoxScoreId"]:
            boxScoreQuery = f"""REPLACE INTO BoxScore(GameId, PlayerId, FGM, FGA, TPM, TPA, FTM, FTA, Rebs, Asts, Stls, Blks, TOs, Pts, BoxScoreId)
        VALUES{bs}"""
        else:
            boxScoreQuery = f"""INSERT INTO BoxScore(GameId, PlayerId, FGM, FGA, TPM, TPA, FTM, FTA, Rebs, Asts, Stls, Blks, TOs, Pts, BoxScoreId)
            VALUES{bs}"""
        execute_query_post(connection, boxScoreQuery)
        # break

def UpdateGames(connection):
    """Returns Dataframe with games that are not in the database
    """
    log.info("Finding new games...")
    last_game_accessed_sql =  "SELECT Date FROM Game WHERE Date = (SELECT DISTINCT MAX(Date) FROM Game) LIMIT 1;" 
    last_game_accessed = execute_query_get(connection, last_game_accessed_sql)[0][0]
    # game_table = execute_query_get(connection, "SELECT * FROM Game;")
    game_table = pd.read_sql_query("SELECT * FROM Game;", connection)
    
    all_games = LeagueGameLog(league_id='00', season_type_all_star="Regular Season", date_from_nullable=last_game_accessed)
    df = all_games.league_game_log.get_data_frame()
    df.drop_duplicates().sort_values(by=['GAME_ID'])
    log.info("Acquired Games")

    data = pd.DataFrame(columns=['GAME_ID', 'GAME_DATE', 'HOME_SCORE', 'AWAY_SCORE', 'HOME_TEAM_ID', 'AWAY_TEAM_ID'])
    for i in range(0, len(df), 2):
        game = {'GAME_ID': df.loc[i]['GAME_ID'], 'GAME_DATE': df.loc[i]['GAME_DATE'], 'HOME_SCORE': df.loc[i]['PTS'], 'AWAY_SCORE': df.loc[i + 1]['PTS'], 'HOME_TEAM_ID': df.loc[i]['TEAM_ID'], 'AWAY_TEAM_ID': df.loc[i + 1]['TEAM_ID']}
        game = pd.Series(game)

        data = data.append(game, ignore_index = True) 
    
    for game in list(data.itertuples(index=False, name=None)):
        game = list(game)
        if game[0] in game_table["GameId"]:
            # game_query = f"""REPLACE INTO Game(GameId, Date, HomeScore, AwayScore, HomeTeamId, AwayTeamId)
            # VALUES{tuple(game)}"""
            game_query = f"""UPDATE Game 
            SET GameId = {game[0]}, Date = {game[1]}, HomeScore = {game[2]}, AwayScore = {game[3]}, HomeTeamId = {game[4]}, AwayTeamId = {game[5]}
            WHERE GameId = {game[0]};"""
        else:   
            game_query = f"""INSERT INTO Game(GameId, Date, HomeScore, AwayScore, HomeTeamId, AwayTeamId)
            VALUES{tuple(game)}"""
        execute_query_post(connection, game_query)
    return data

def GetBoxScores(df):
    log.info("Getting Box Scores...")
    game_ids = df['GAME_ID']
    box_scores = pd.DataFrame()
    for game_id_index in tqdm(range(len(game_ids))):
        try:
            response = BoxScoreTraditionalV2(game_id=str(game_ids.loc[game_id_index]))
            boxscore_df = response.player_stats.get_data_frame()
            box_scores = box_scores.append(boxscore_df)
            time.sleep(0.6)
        except Exception as e:
            raise e
    return box_scores

if __name__ == "__main__":
    warnings.filterwarnings("ignore")
    try:
        log.info("Starting Update")

        connection = create_db_connection("34.136.27.137", "root", "PickAndRoll")
        data = UpdateGames(connection)
        boxscores = GetBoxScores(data)
        # boxscores = pd.read_csv("csv/boxscoresv3.csv", index_col = 0)
        newplayers = CheckPlayers(boxscores, connection)
        InsertPlayers(newplayers, connection)
        SignPlayers(boxscores)
        InputBoxScoreAndPlayerPlay(boxscores, connection)
        sys.stdout.flush()
        print(f'Update finished: {len(data)} games added!')

        
    except Exception as e:
        raise Exception
        print("An Error occured")