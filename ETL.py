import configparser
import psycopg2
import pandas as pd

# Define SQL queries for COPY and INSERT operations
from sql_queries import insert_table_queries

def load_staging_tables(cur, conn, config):
    """
    Load data into staging tables from S3.
    """
    DWH_ROLE_ARN = config['IAM_ROLE']['ARN']
    S3_EVENT_CSV = config['S3']['EVENT_CSV']
    S3_SONGS_CSV = config['S3']['SONGS_CSV']

    queries = [
        f"""
            COPY staging_events
            FROM {S3_EVENT_CSV}
            IAM_ROLE {DWH_ROLE_ARN}
            CSV
            IGNOREHEADER 1
            FILLRECORD
            TRUNCATECOLUMNS
            MAXERROR 10;
        """,
        f"""
            COPY staging_songs
            FROM {S3_SONGS_CSV}
            IAM_ROLE {DWH_ROLE_ARN}
            CSV
            IGNOREHEADER 1
            FILLRECORD
            TRUNCATECOLUMNS
            MAXERROR 10;
        """
    ]

    for query in queries:
        try:
            print(f"Executing query: {query}")
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f"Error executing query: {e}")


def insert_tables(cur, conn):
    """
    Insert data into analytics tables from staging tables.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print(f"Query executed successfully: {query}")
            print(f"Number of rows inserted: {cur.rowcount}")
        except Exception as e:
            print(f"Error executing query: {e}")


def main():
    """
    Main function to manage the ETL pipeline.
    """
    # Load configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish database connection
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    print("Database connection established.")

    # Load data into staging tables
    print("Loading data into staging tables...")
    load_staging_tables(cur, conn, config)

    # Insert data into final tables
    print("Inserting data into analytics tables...")
    insert_tables(cur, conn)

    # Close the connection
    conn.close()
    print("Database connection closed.")


if __name__ == "__main__":
    main()
