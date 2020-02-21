import configparser

import psycopg2

from sql_queries import copy_table_queries, \
    insert_table_queries

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


def main():
    """
        It executes all the steps for the entire ETL process
    :return: void
    """
    config = configparser.ConfigParser()
    config.read('../config/dwh.cfg')

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(*config['CLUSTER'].values())

    # Getting a Redshift connection
    print("Connection to " + conn_string)
    conn = psycopg2.connect(conn_string)
    print("Connected!!")
    cur = conn.cursor()

    print("Loading stagin tables")
    load_staging_tables(cur, conn, config)
    print("Done!")

    print("Inserting into fact and dimension tables")
    insert_tables(cur, conn)
    print("Done!")

    print("Your Sparkify star model was succesfully loades into a Redshift cluster. \n"
          "You can get fun query it from your AWS Redshift query editor.")
    print("Closing connection")
    conn.close()
    print("Done!")


if __name__ == "__main__":
    main()
