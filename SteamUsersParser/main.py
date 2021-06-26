from steam import steamid
import requests
import concurrent.futures
import psycopg2

DEVELOPER_KEY = "YOUR_STEAM_WEB_API_KEY"
STEAM_USERS_COUNT = 110000  # 1225000000  # there are less than 1 225 000 000 accounts registered in steam
START_USER_INDEX = 0  # index which is going to be the first in the loop
USERS_PER_REQUEST = 100  # users ids per one request
CONNECTIONS = 10

DATABASE = "steam_users"
USER = "postgres"
PASSWORD = "YOUR_DATABASE_PASSWORD"
HOST = "localhost"
PORT = "5432"


def do_request(i, session):
    steam_ids = get_steam_ids(i)
    # r = requests.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/"
    #                  "v2/?key=" + DEVELOPER_KEY + "&steamids=" + ",".join(steam_ids))
    r = session.get("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/"
                     "v2/?key=" + DEVELOPER_KEY + "&steamids=" + ",".join(steam_ids))
    return r.json(), i


def get_steam_users_info(request) -> list:
    r = request[0]
    iterator = request[1]
    users_list = []
    for i in range(len(r["response"]["players"])):
        users_list.append((r["response"]["players"][i]["steamid"] + ";" +
                           r["response"]["players"][i]["avatarhash"][:2] + "/" +
                           r["response"]["players"][i]["avatarhash"]).split(";"))
    return users_list, iterator


def get_steam_ids(i):
    steam_ids = []
    for j in range(i, i + USERS_PER_REQUEST):
        steam_ids.append(str(steamid.SteamID(j)))
    return steam_ids


if __name__ == '__main__':
    # DEVELOPER_KEY = input("Enter Steam Api Key: ")
    # START_USER_INDEX = int(input("Enter start user index for parsing (must be divisible by 100 without a remainder): "))
    # STEAM_USERS_COUNT = int(input("Enter last user index for parsing (must be divisible by 100 without a remainder): "))
    # DATABASE = input("Enter DataBase name: ")
    # USER = input("Enter user name from Postgres: ")
    # PASSWORD = input("Enter user password from Postgres: ")
    # HOST = input("Enter Postgres host: ")
    # PORT = input("Enter Postgres port: ")
    try:
        connection = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
        print("Connected to DataBase successful")
        cursor = connection.cursor()
        session = requests.Session()
        with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
            r = {executor.submit(do_request, i, session): i for i in
                 range(START_USER_INDEX, STEAM_USERS_COUNT, USERS_PER_REQUEST)}
            for rs in concurrent.futures.as_completed(r):
                response = get_steam_users_info(rs.result())
                users_list = response[0]
                current_iterator = response[1]
                for user in users_list:
                    insert_query = f"INSERT INTO user_info (id, avatar) VALUES " \
                                   f"('{user[0]}', '{user[1]}')"
                    cursor.execute(insert_query)
                connection.commit()
                print(f"Users {current_iterator}-{current_iterator + USERS_PER_REQUEST - 1} added to DataBase")
    except (Exception, psycopg2.Error) as error:
        print("An error has occurred:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection with DataBase has been closed")
