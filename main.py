import os
from datetime import datetime

import psycopg2
from credentials import db, user, password, host, port, COLUMNS


def fetch_db_events(conn):
    query = """select distinct file from events_raw"""

    cur = conn.cursor()
    cur.execute(query)
    events = cur.fetchall()
    print(str(len(events)) + ' events in db!')
    cur.close()

    return events


def db_conn():
    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host=host,
        port=port
    )

    return conn


def db_insert(rows, conn):
    chunksize = 50
    cur = conn.cursor()
    all_columns = sorted(COLUMNS)

    columns_str = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))

    for i in range(0, len(rows), chunksize):
        print('Inserting batch ' + str(i) + ' to db')
        batch = rows[i:i + chunksize]

        values = [
            tuple(row_.get(col_, None) for col_ in all_columns)
            for row_ in batch
        ]

        query = f"INSERT INTO events_raw ({columns_str}) VALUES " + ",".join(
            cur.mogrify(f"({placeholders})", val).decode("utf-8") for val in values
        )

        cur.execute(query)
        conn.commit()

    cur.close()

    return


def check_table(conn):
    cur = conn.cursor()

    # Create table query
    create_table_query = """
    CREATE TABLE IF NOT EXISTS events_raw (
        file VARCHAR(255),
        sessionId VARCHAR(255), 
        time TIMESTAMP, 
        logId VARCHAR(255),
        type VARCHAR(255),
        logType VARCHAR(255),
        name VARCHAR(255),
        schoolId VARCHAR(255)
    );
    """
    cur.execute(create_table_query)
    conn.commit()

    print("Table connection established!")

    cur.close()
    return


if __name__ == '__main__':
    """
    1. read file names
    2. read if file name is in db
    3. if not read file
    4. increment read counter
    5. store file name in db
    6. at end show read events
    """

    current_directory = os.getcwd()
    folder_path = os.path.join(current_directory, 'events.jsonl')
    new_count = 0
    old_count = 0
    conn = db_conn()
    # check if table exists
    check_table(conn)
    db_events = fetch_db_events(conn)
    db_events = [row[0] for row in db_events]
    rows = []
    cols = []
    # simulate processing events as batch when device is online
    for filename in os.listdir(folder_path):
        # skip existing logs;
        # todo; replace with time specific log reads
        if filename not in db_events:
            file_name_ = filename.split('{')[1].split('}')[0]
            data = file_name_.split(',')
            row = {}
            col = ''
            row['file'] = filename
            for item in data:
                col = item.split('___')[0].strip('_')
                val = item.split('___')[1].strip('_')

                if col == 'time':
                    val = val.replace('_', ':')
                    # remove inconsistency 2021-06-13T11_46_47Z vs 2021-06-13T11_46_47.371Z
                    val = datetime.strptime(val[:19], "%Y-%m-%dT%H:%M:%S")

                row[col] = val
            rows.append(row)
            cols.append(col)

            new_count += 1

        else:
            old_count += 1

    if len(rows) > 0:
        db_insert(rows, conn)

    if conn:
        conn.close()

    # todo; replace print statements with logging for monitoring
    print('Read: ' + str(new_count) + ', Skipped: ' + str(old_count) + ' events')
