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
    # Sample data transformation : Convert String to Datetime using Pandas
    df["Date"] = pd.to_datetime(df["Date"])

    # Sample data transformation : Handle numeric data that contains comma
    df["Volume"] = pd.to_numeric(df["Volume"].str.replace(',',''), errors='coerce')
    df["Market Cap"] = pd.to_numeric(df["Market Cap"].str.replace(',',''), errors='coerce')

    # Sample data transformation : Handle NaN data
    df["Volume"] = df["Volume"].fillna(df["Volume"].mode()[0])
    df["Market Cap"] = df["Market Cap"].fillna(df["Market Cap"].mode()[0])
    return df


def import_data_to_bigquery(df):
    # Convert DataFrame to tuples before import to bigQuery
    tuples = list(df.itertuples(index=False, name=None))

    bigQueryClient = BigQueryClient()
    bigQueryClient.import_data(tuples, "kaggle_coinbase")
    print("BigQuery Task Completed.")


def main():
    # Get data from kaggle using specific owner, repository and filename.
    print("\n{}Loading Raw Data{}\n".format('*'*15, '*'*15))
    df = load_raw_data("philmohun", "cryptocurrency-financial-data", "consolidated_coin_data.csv")

    # Transform dataset
    print("\n{}Transforming Data{}\n".format('*'*15, '*'*15))
    df = transform_data(df)

    import_data_to_bigquery(df)


if __name__ == "__main__":
	main()
