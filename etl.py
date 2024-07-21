import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Copies data (log_data and song_data) from S3 to Redshift.

    Arguments:
        - cur: cursor to connect to the db.
        - conn: conection to the db.

    Output:
    - None: loads data from S3 to Redshift on staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Loops through insert statements, executing them. This will insert data into the final fact/dimension tables.
    
    Arguments:
        - cur: cursor to connect to the db.
        - conn: conection to the db.

    Outputs:
        - None: Inserts data into the tables at the Data Warehouse on AWS.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

