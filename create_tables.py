# Fact Table
# songplays - records in log data associated with song plays i.e. records with page NextSong
	# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dimension Tables
# users - users in the app
	# user_id, first_name, last_name, gender, level
# songs - songs in music database
	# song_id, title, artist_id, year, duration
# artists - artists in music database
	# artist_id, name, location, latitude, longitude
# time - timestamps of records in songplays broken down into specific units
	# start_time, hour, day, week, month, year, weekday


import psycopg2

#remove this later 
print("""


START:
""")

from sql_queries import create_table_queries, drop_table_queries

def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()