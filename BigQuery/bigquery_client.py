from google.cloud import bigquery
from schema_generator import SchemaGenerator

data = ""

class BigQueryClient:
    def import_data(self, data):
        schema_generator = SchemaGenerator()
        client = bigquery.Client.from_service_account_json('./service_account.json')
        result = schema_generator.execute(client, "kaggle_coinbase")

        table = if result['success']: result['table'] else: None
        errors = client.insert_rows(table, data)

        if len(errors) == 0: #which means it's success.
            print("Upload success")
