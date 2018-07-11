from google.cloud import bigquery
from BigQuery.schema_generator import SchemaGenerator
import os

class BigQueryClient:
    def import_data(self, data, table_name):
        schema_generator = SchemaGenerator()
        client = bigquery.Client.from_service_account_json(''.join([os.getcwd(), '/BigQuery/service_account.json']))
        result = schema_generator.execute(client, table_name)

        if result['success']: #Create table and schema success.
            table = result['table']
            print("\n{}Importing Data to Bigquery{}\n".format('*'*15, '*'*15))
            errors = client.insert_rows(table, data)

            if len(errors) == 0: #which means it's success.
                print("\nUpload success\n")
            else:
                print("\nUpload failed\n")
        else:
            print("Upload failed : {}".format(result["errors"]))

        return
