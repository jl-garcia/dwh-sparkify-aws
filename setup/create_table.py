import configparser

import psycopg2

from sql_queries import delete_schema, create_and_set_schema, drop_staging_tables, create_staging_tables, \
    drop_table_queries, create_table_queries


def set_up_schema(cur, conn):
    """
        Deletes and create Sparkify schema. Sets this new schema as a default one
    :param cur: Cursor
    :param conn: Redshift connection
    :return: void
    """
    cur.execute(delete_schema)
    cur.execute(create_and_set_schema)
    conn.commit()


def create_staging(cur, conn):
    """
        Drop and create staging tables
    :param cur: Cursor
    :param conn: Connection
    :return: void
    """
    # deleting staging tables
    for query in drop_staging_tables:
        cur.execute(query)
        conn.commit()

    # creating staging tables
    for query in create_staging_tables:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
        Drops start model tables (Fact and dimension tables)
    :param cur: Cursor
    :param conn: Redshift connection
    :return: void
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Creates business tables (star model tables)
    :param cur: Cursor
    :param conn: Redshift connection
    :return: void
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
       It creates fact and dimension tables for the star schema in Redshift
    :return: void
    """
    conn = get_connection()
    print("Connected!!")

    cur = conn.cursor()

    # 1 Create Sparkify schema
    print("Setting Sparkify schema")
    set_up_schema(cur, conn)
    print("Done!")

    # 2 Create staging tables
    print("Creating stagin tables")
    create_staging(cur, conn)
    print("Done!")

    # 3. Drops and creates fact and dimension tables
    print("Setting fact and dimension tables")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Done!")

    print("Closing connection")
    conn.close()
    print("Done!")


def get_connection():
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(*config['CLUSTER'].values())
    # Getting a Redshift connection
    print("Connection to " + conn_string)
    conn = psycopg2.connect(conn_string)
    return conn


if __name__ == "__main__":
    main()
