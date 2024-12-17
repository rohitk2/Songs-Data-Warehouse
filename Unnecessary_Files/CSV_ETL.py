import configparser
import psycopg2
import pandas as pd
from sql_queries import insert_table_queries


config = configparser.ConfigParser()
config.read('dwh.cfg')
conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
cur = conn.cursor()

print(conn)
print(cur)

def load_csv_to_staging(table_name, csv_file, cur, conn):
    """
    Load data from a local CSV file into a Redshift staging table.
    """
    import numpy as np

    # Read CSV
    df = pd.read_csv(csv_file)

    # Log the column names for verification
    print("Columns in CSV:", df.columns)

    # Prepare INSERT query
    columns = ", ".join(df.columns)
    values = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"

    rows_dropped = 0
    # Load data into the table
    for _, row in df[:10].iterrows() :
        # Convert row to a list for validation
        validated_row = []
        for val in row:
            # Check if value is a number and exceeds BIGINT range
            if isinstance(val, (int, float)) and val > 9223372036854775807:
                validated_row.append(None)  # Replace out-of-range values with NULL
            else:
                validated_row.append(val)

        try:
            # Insert the validated row into the database
            cur.execute(insert_query, tuple(validated_row))
            conn.commit()  # Commit after each successful row
            #print(f"Added: {validated_row}")
        except Exception as e:
            # Log the problematic row for debugging
            print("Failed to insert row:", validated_row)
            print("Error:", e)
            rows_dropped += 1
            conn.rollback()  # Roll back the transaction to continue with the next row    
    print(f"DROPPED {rows_dropped}/{len(df)} rows")


# Load staging_events
load_csv_to_staging("staging_events", "events.csv", cur, conn)

# Load staging_songs
load_csv_to_staging("staging_songs", "songs.csv", cur, conn)


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


#load_staging_tables(cur, conn)
#insert_tables(cur, conn)
conn.close()