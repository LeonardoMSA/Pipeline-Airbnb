class kaggle:
  def downloadDataset(): 
    import pandas as pd
    from kaggle.api.kaggle_api_extended import KaggleApi
    import os
    
    api = KaggleApi()
    api.authenticate()

    dataset_url = 'dgomonov/new-york-city-airbnb-open-data'
    api.dataset_download_files(dataset_url, path='./kaggle', unzip=True)

    if os.path.exists("kaggle/New_York_City_.png"):
      os.remove("kaggle/New_York_City_.png")

    file_name = 'kaggle/AB_NYC_2019.csv'

    cols_to_use = ['id', 'name', 'host_id', 'neighbourhood', 'neighbourhood_group', 'latitude', 'longitude', 'room_type', 'price', 'minimum_nights', 'number_of_reviews', 'availability_365']
    data = pd.read_csv(file_name, usecols=cols_to_use)

    print(data.head())
