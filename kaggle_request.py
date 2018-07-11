import pandas as pd
import os

class Request:
    def get_csv_data(self, owner, repository, file, path):
        os.system("kaggle datasets download -d {}/{} -f {} -p {}".format(owner, repository, file, path))
        fileName = "{}{}.zip".format(path, file)
        df = pd.read_csv(fileName, compression='zip', low_memory=False)
        # Drop duplicated rows
        df = df.drop_duplicates()
        df = df.iloc[1:,:]
        os.remove(fileName)
        return df.head(1000)
