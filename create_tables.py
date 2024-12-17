import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in the database.
    """
    print("Dropping tables...")
    for query in drop_table_queries:
        try:
            print(f"Executing drop query: {query}")
            cur.execute(query)
            conn.commit()
            print("Table dropped successfully.")
        except Exception as e:
            print(f"Error dropping table: {e}")
    print("Finished dropping tables.\n")


def create_tables(cur, conn):
    """
    Create tables in the database.
    """
    print("Creating tables...")
    for query in create_table_queries:
        try:
            print(f"Executing create query: {query}")
            cur.execute(query)
            conn.commit()
            print("Table created successfully.")
        except Exception as e:
            print(f"Error creating table: {e}")
    print("Finished creating tables.\n")



def main():
    """
    Main function to set up the database by dropping existing tables
    and creating new ones.
    """
    # Load configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Establish connection to the database
    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
    )
    cur = conn.cursor()

    # Print connection details for debugging
    print("Connection established:", conn)
    print("Cursor created:", cur)

    # Drop and recreate tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the connection
    conn.close()
    print("Connection closed.")


if __name__ == "__main__":
    main()
