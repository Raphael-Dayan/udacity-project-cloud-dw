import configparser
import psycopg2
from sql_queries import count_number_rows_queries


def get_tables_rows(cur, conn):
    """
    Gets the number of rows stored into each table
    """
    print("Simple query to validate that all tables have >0 rows.")
    for query in count_number_rows_queries:
        print("\n".join(("", "Running:", query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)

analytical_queries = {"at what time does people listen to music the most?": """
                        SELECT hour, COUNT(hour) AS count
                        FROM songplays s
                        JOIN time t on (s.start_time = t.start_time)
                        GROUP BY hour
                        ORDER BY count DESC
                        LIMIT 1;""",
                        "how often do users listen to music?": """
                        WITH user_listening_frequency AS (
                            SELECT 
                                user_id,
                                start_time,
                                LAG(start_time, 1) OVER (PARTITION BY user_id ORDER BY start_time) AS previous_start_time,
                                start_time - previous_start_time AS time_since_last_song_played
                            FROM
                                songplays)
                        SELECT
                            AVG(time_since_last_song_played) AS average_time_for_user_to_hear_another_song
                        FROM 
                            user_listening_frequency""",
                            
                        "how often do users try new songs?": """
                        WITH new_songs AS (
                            SELECT 
                                user_id,
                                start_time,
                                song_id,
                                ROW_NUMBER() OVER (PARTITION BY user_id, song_id ORDER BY start_time) AS play_number,
                                CASE 
                                    WHEN play_number = 1 THEN 1
                                    WHEN play_number > 1 THEN 0
                                END AS is_new_song
                            FROM
                                songplays),
                        percentage_of_new_songs AS(
                            SELECT
                                user_id,
                                song_id,
                                SUM(is_new_song) OVER(PARTITION BY user_id) AS new_song_count,
                                COUNT(*) OVER(PARTITION BY user_id) AS total_song_count,
                                CAST(new_song_count AS DECIMAL) / total_song_count AS new_song_percentage
                            FROM
                                new_songs),
                        percentage_of_new_songs_per_customer AS(
                            SELECT 
                                DISTINCT user_id,
                                new_song_percentage
                            FROM
                                percentage_of_new_songs)
                        SELECT
                            AVG(new_song_percentage) * 100 AS average_percentage_of_new_songs
                        FROM
                            percentage_of_new_songs_per_customer"""}

def answer_analytical_questions(cur, conn):
    """
    Returns queries to answer predefined analytical questions
    """
    print("\n\n\n\nAnswering analytical questions:\n\n\n")

    print(f"\n\nBusiness Question: {list(analytical_queries.items())[0][0]}")
    print(f"Translating this question to a technical question: what is the most popular hour people listen to music?")
    print("Executing query. Answer:")
    cur.execute(list(analytical_queries.items())[0][1])
    results = cur.fetchone()
    for row in results:
        print(row)

    print(f"\n\nBusiness Question: {list(analytical_queries.items())[1][0]}")
    print(f"Translating this question to a technical question: what is the average time between sessions for the same user?")
    print("Executing query. Answer:")
    cur.execute(list(analytical_queries.items())[1][1])
    results = cur.fetchone()
    for row in results:
        print(row)

    print(f"\n\nBusiness Question: {list(analytical_queries.items())[2][0]}")
    print(f"Translating this question to a technical question: what is the percentage of new songs per customer?")
    print("Executing query. Answer:")
    cur.execute(list(analytical_queries.items())[2][1])
    results = cur.fetchone()
    for row in results:
        print(row)        

def main():
    """
    Runs analytical queries
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    print("Connected to AWS Redshift")
    cur = conn.cursor()

    # Analytical queries
    get_tables_rows(cur, conn)
    answer_analytical_questions(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()