import requests
import concurrent.futures
import psycopg2


CONNECTIONS = 10

DATABASE = "steam_users"
USER = "postgres"
PASSWORD = "Av99052824"
HOST = "localhost"
PORT = "5432"
LIMIT = 1000
START_USER_INDEX = 0
END_USER_INDEX = 110000


def do_request(i, session):
    rows = get_records(i)
    avatar_update_dates = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        r = {executor.submit(send_request, row, session): row for row in rows}
        # range(START_USER_INDEX, END_USER_INDEX, LIMIT)}
        for rs in concurrent.futures.as_completed(r):
            avatar_update_dates.append(rs.result())
    return avatar_update_dates


def send_request(row, session):
    try:
        r = requests.get(
            f"https://cdn.cloudflare.steamstatic.com/steamcommunity/public/images/avatars/{row[1]}.jpg")
        avatar_update_date = ([row[0], str(r.headers["Last-Modified"])])
    except:
        avatar_update_date = ([row[0], "Null"])
    return avatar_update_date


def get_records(i):
    insert_query = "SELECT * FROM user_info " \
                   " ORDER BY id" \
                   f" LIMIT {LIMIT} OFFSET {i}"
    cursor.execute(insert_query)
    rows = cursor.fetchall()
    return rows


def add_avatar_update_dates_to_db(avatars_update_dates):
    for data in avatars_update_dates:
        insert_query = f"UPDATE user_info " \
                       f" SET avatar_update_date='{data[1]}' " \
                       f" WHERE id='{data[0]}'"
        cursor.execute(insert_query)
    connection.commit()


if __name__ == '__main__':
    try:
        connection = psycopg2.connect(database=DATABASE, user=USER, password=PASSWORD, host=HOST, port=PORT)
        print("Connected to DataBase successful")
        cursor = connection.cursor()
        session = requests.Session()
        for i in range(START_USER_INDEX, END_USER_INDEX, LIMIT):
            response = do_request(i, session)
            add_avatar_update_dates_to_db(response)
            print(f"Avatar update date of users {i}-{i + LIMIT - 1} added to DataBase")
    except (Exception, psycopg2.Error) as error:
        print("An error has occurred:", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection with DataBase has been closed")
