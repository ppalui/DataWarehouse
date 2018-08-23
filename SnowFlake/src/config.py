import os

config = {
    "ACCOUNT": 'kingpower.ap-southeast-2',
    "SQL_INSERT_LIMIT": 16384,
    "WAREHOUSE_NAME": "DEV_PLAYGROUND",
    "WAREHOUSE_SIZE": "MEDIUM",
    "DB_NAME": "DEV_PG",
    "SCHEMA_NAME": "KPC_REPORT",
    "ROOT_DIR": os.path.dirname(os.path.abspath(__file__))
}