class kaggle:
    @staticmethod
    def downloadDataset():
        from kaggle.api.kaggle_api_extended import KaggleApi
        import pandas as pd
        import os

        api = KaggleApi()
        api.authenticate()

        dataset_url = 'dgomonov/new-york-city-airbnb-open-data'
        api.dataset_download_files(dataset_url, path='./kaggle', unzip=True)

        if os.path.exists("kaggle/New_York_City.png"):
            os.remove("kaggle\New_York_City_.png")

        file_name = 'kaggle/AB_NYC_2019.csv'
        data = pd.read_csv(file_name)

        print(data.head())
        cols_to_use = pd.read_csv(file_name, nrows=0).columns.tolist()
        return dataset_url.split('/')[-1], cols_to_use