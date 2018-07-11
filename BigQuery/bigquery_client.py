from google.cloud import bigquery
from BigQuery.schema_generator import SchemaGenerator
import os

class BigQueryClient:
    def import_data(self, data):
        os.getcwd()
        schema_generator = SchemaGenerator()
        client = bigquery.Client.from_service_account_json(''.join([os.getcwd(), '/BigQuery/service_account.json']))
        result = schema_generator.execute(client, "kaggle_coinbase")

        if result['success']:
            table = result['table']
            errors = client.insert_rows(table, data)

            if len(errors) == 0: #which means it's success.
                print("Upload success")
        else:
            print("Upload failed : {}".format(result["errors"]))
