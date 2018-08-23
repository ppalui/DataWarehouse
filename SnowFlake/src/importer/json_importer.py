import json
import os
from config import config
from importer import setup


def insert_data(conn, table_name, data):
    data = ','.join(data)
    conn.execute("INSERT INTO {} SELECT PARSE_JSON(column1) FROM VALUES{}".format(table_name, data))


def get_chunks(data, n):
    for i in range(0, len(data), n):
        yield data[i:i + n]


def transform_data(data):
    for key, value in data.items():
        if type(value) is str:
            data[key] = value.replace('"', "")
    return "('{}')".format(str(data))


def main():
    conn = setup.setup_database(username="YOUR USERNAME", password="YOUR PASSWORD", schema="JSON_"+config["SCHEMA_NAME"])

    print("\nImporting JSON to database...\n")
    try:
        file_path = "{}/resource/json".format(config["ROOT_DIR"])
        json_files = [x for x in os.listdir(file_path) if x.endswith("json")]

        for file_name in json_files:
            print('\nReading file... {}\n'.format(file_name))

            with open("{}/{}".format(file_path, file_name)) as datafile:
                data = json.load(datafile)

            source = list()
            for item in data:
                for key, value in item.items():
                    if type(value) is str:
                        item[key] = value.replace('"', "")
                json_data = json.dumps(item).replace("'", "\\'")
                source.append("('{}')".format(json_data))

            table_name = file_name[:-5]
            conn.execute("CREATE OR REPLACE TABLE {0}(source variant)".format(table_name))

            if len(source) > config["SQL_INSERT_LIMIT"]:
                chunks = list(get_chunks(source, config["SQL_INSERT_LIMIT"]))
                print("\nPartitioning data in to {} parts...\n".format(str(len(chunks))))
                for index, item in enumerate(chunks):
                    insert_data(conn, table_name, item)
            else:
                insert_data(conn, table_name, source)
    except Exception as e:
        print("ERROR OCCURRED : {}".format(e))
    finally:
        conn.disconnect()
        exit()


if __name__ == "__main__" or __name__ == "builtins":
    main()
