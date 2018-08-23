import snowflake.connector as sf
from config import config


class Connector:
    def __init__(self, username, password):
        print("Connecting to snowflake...")
        self.cnx = sf.connect(
            user=username,
            password=password,
            account=config["ACCOUNT"])

        self.cursor = self.cnx.cursor()

    def execute(self, sql):
        try:
            self.cursor.execute(sql)
        finally:
            print('$ {} executed.'.format(sql))
        return self

    def disconnect(self):
        self.cnx.close()
        print('Snowflake connector disconnected.')
