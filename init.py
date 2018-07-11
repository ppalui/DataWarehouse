from kaggle_request import Request
import pandas as pd
from datetime import datetime
from BigQuery.bigquery_client import BigQueryClient

def load_raw_data(owner, repository, filename):
    """
    Call kaggle_request to get data from Kaggle through Kaggle API
    """
    requests = Request()
    return requests.get_csv_data(
    owner,
    repository,
    filename, "./")

def transform_data(df):
    # Convert Timestamp to Datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit='s')
    return df

def main():
    # Get data from kaggle using specific owner, repository and filename.
    df = load_raw_data("philmohun", "cryptocurrency-financial-data", "consolidated_coin_data.csv")

    # Drop duplicated rows
    df = df.drop_duplicates()
    df = df.iloc[1:,:]

    # Transform dataset
    df = transform_data(df)

    # Convert DataFrame to tuples before import to bigQuery
    tuples = list(df.itertuples(index=False, name=None))

    bigQueryClient = BigQueryClient()
    bigQueryClient.import_data(tuples)


if __name__ == "__main__":
	main()
