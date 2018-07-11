from google.cloud import bigquery

class SchemaGenerator:
    def execute(self, client, table_name):
        print("\n{}Generating Schema{}\n".format('*'*15, '*'*15))

        schema = [
            bigquery.SchemaField('Currency', 'STRING'),
            bigquery.SchemaField('Date', 'DATETIME'),
            bigquery.SchemaField('Open', 'FLOAT'),
            bigquery.SchemaField('High', 'FLOAT'),
            bigquery.SchemaField('Low', 'FLOAT'),
            bigquery.SchemaField('Close', 'FLOAT'),
            bigquery.SchemaField('Volume', 'FLOAT'),
            bigquery.SchemaField('Market_Cap', 'FLOAT'),
        ]

        project_id = 'able-involution-209812'
        dataset_id = 'kaggle_coinbase_data'
        table_id = 'kaggle_coinbase'

        dataset_ref = client.dataset(dataset_id, project_id)
        table_ref = dataset_ref.table(table_id)
        table = bigquery.Table(table_ref, schema=schema)

        try:
            print("\n{}Creating Table{}\n".format('*'*15, '*'*15))
            result = client.create_table(table)
            return {'success': result.table_id == table_name, 'table': table}
        except Exception as e:
            return {'success': False, 'errors': e.args}
