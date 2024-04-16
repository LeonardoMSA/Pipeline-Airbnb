from kaggle.api.kaggle_api_extended import KaggleApi
import pandas as pd
import os

class kaggle:
    @staticmethod
    def downloadDataset():
        # Autenticar na API do Kaggle
        api = KaggleApi()
        api.authenticate()

        # Download do dataset
        dataset_url = 'dgomonov/new-york-city-airbnb-open-data'
        api.dataset_download_files(dataset_url, path='./kaggle', unzip=True)

        diretorio = "kaggle/"
        arquivos_png = [arquivo for arquivo in os.listdir(diretorio) if arquivo.endswith(".png")]
        for arquivo in arquivos_png:
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)

        # Pegar o nome do arquivo .csv restante
        arquivos_csv = [arquivo for arquivo in os.listdir(diretorio) if arquivo.endswith(".csv")]
        if len(arquivos_csv) == 0:
            raise FileNotFoundError("Nenhum arquivo CSV encontrado no diretório.")
        elif len(arquivos_csv) > 1:
            raise ValueError("Múltiplos arquivos CSV encontrados no diretório. Não é possível determinar automaticamente o nome do arquivo.")
        file_name = os.path.join(diretorio, arquivos_csv[0])

        # Ler os dados do arquivo CSV
        data = pd.read_csv(file_name)


        print(data.head())
        cols_to_use = pd.read_csv(file_name, nrows=0).columns.tolist()

        return dataset_url.split('/')[-1], cols_to_use, file_name

