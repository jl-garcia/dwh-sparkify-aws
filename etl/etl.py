import configparser

import pandas as pd
import boto3
import json

import psycopg2

from queries.sql_queries import copy_table_queries, \
    insert_table_queries, create_staging_tables, \
    staging_events_table_drop, staging_songs_table_drop, \
    drop_staging_tables, delete_schema, create_and_set_schema, \
    create_table_queries, drop_table_queries


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


def load_staging_tables(cur, conn, config):
    """
        Load staging tables from a S3 bucket
    :param cur: Cursor
    :param conn: Redshift connection
    :param config: Config object
    :return: void
    """
    for query in copy_table_queries:
        cur.execute(query.format(config['IAM_ROLE']['ARN']))
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


def insert_tables(cur, conn):
    """
        Loads fact and dimensional tables from stagin tables
    :param cur: Cursor
    :param conn: Redshift connection
    :return: void
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


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


def main():
    """
        It executes all the steps for the entire ETL process
    :return: void
    """
    config = configparser.ConfigParser()
    config.read('config/dwh.cfg')

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(*config['CLUSTER'].values())

    #Getting a Redshift connection
    print("Connection to " + conn_string)
    conn = psycopg2.connect(conn_string)
    print("Connected!!")
    cur = conn.cursor()

    #1 Create Sparkify schema
    print("Setting Sparkify schema")
    set_up_schema(cur, conn)
    print("Done!")

    #2 Create stagin tables
    print("Creating stagin tables")
    create_staging(cur, conn)
    print("Done!")
    print("Loading stagin tables")
    load_staging_tables(cur, conn, config)
    print("Done!")

    #3. Transform and Load into fact and dimensional tables
    print("Setting fact and dimension tables")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Done!")
    print("Inserting in fact and dimension tables")
    insert_tables(cur, conn)
    print("Done!")

    print("Your Sparkify star model was succesfully loades into a Redshift cluster. \n"
          "You can get fun query it from your AWS Redshift query editor.")
    print("Closing connection")
    conn.close()
    print("Done!")

if __name__ == "__main__":
    main()
