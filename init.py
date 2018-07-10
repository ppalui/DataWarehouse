from kaggle_request import Request

def load_raw_data(owner, repository, filename):
    """
    Call kaggle_request to get data from Kaggle through Kaggle API
    """
    requests = Request()
    return requests.get_csv_data(
    owner,
    repository,
    filename, "./")


def main():
    # Get data from kaggle using specific owner, repository and filename.
    # df = load_raw_data("philmohun", "cryptocurrency-financial-data", "consolidated_coin_data.csv")
    df = load_raw_data("mczielinski", "bitcoin-historical-data", "coinbaseUSD_1-min_data_2014-12-01_to_2018-06-27.csv")
    print(df.head())


if __name__ == "__main__":
	main()
