from config import config
from connector import connector


def setup_database(username, password, schema):
    conn = connector.Connector(username=username, password=password)
    try:
        # conn.execute(
        #     "CREATE WAREHOUSE IF NOT EXISTS {} WAREHOUSE_SIZE='{}' AUTO_SUSPEND=1 AUTO_RESUME=true".format(
        #         config["WAREHOUSE_NAME"], config["WAREHOUSE_SIZE"])
        # )

        conn.execute("USE WAREHOUSE {}".format(config["WAREHOUSE_NAME"]))
        # conn.execute("CREATE DATABASE IF NOT EXISTS {}".format(config["DB_NAME"]))
        conn.execute("USE DATABASE {}".format(config["DB_NAME"]))

        conn.execute("create schema if not exists {0}".format(schema))
        conn.execute("use {}.{}".format(config["DB_NAME"], schema))

        # Setup file format CSV and JSON
        conn.execute("CREATE OR REPLACE FILE FORMAT postgres_csv type = 'csv' field_delimiter = ',' record_delimiter "
                     "= '\\n' skip_header = 1 trim_space = true FIELD_OPTIONALLY_ENCLOSED_BY = '\"' "
                     "TIMESTAMP_FORMAT='MON DD YYYY HH12:MI:SS:FFAM'")
        conn.execute("CREATE OR REPLACE FILE FORMAT JSON TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE")

        return conn
    except Exception as e:
        print("\nERROR OCCURRED : {}\n".format(e))
        conn.disconnect()
        exit()
