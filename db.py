import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import json


class postDatabase:
    params = None
    
    @classmethod
    def initialize(cls):
        if cls.params is None:
            with open('config.json', 'r') as file:
                config = json.load(file)
                
            cls.params = {
                'host': config['host'],
                'user': config['user'],
                'password': config['password']
            }

    @staticmethod
    def createDatabase(dataset_name):
        postDatabase.initialize()
        
        db_name = dataset_name

        conn = psycopg2.connect(**postDatabase.params, dbname='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE "{db_name}"') 
            print(f"Banco de dados {db_name} criado com sucesso.")
        else:
            print(f"O banco de dados {db_name} já existe.")

        cursor.close()
        conn.close()

    @staticmethod
    def insertData(dataset_name, cols_to_use, file_name):
        postDatabase.initialize()

        db_name = dataset_name

        conn = psycopg2.connect(**postDatabase.params, dbname=db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airbnb_data (
                {0}
            )
        """.format(', '.join([f"{col} VARCHAR" for col in cols_to_use])))
        conn.commit()

        file_name = file_name
        data = pd.read_csv(file_name)

        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO airbnb_data ({0})
                VALUES ({1})
            """.format(', '.join(cols_to_use), ', '.join(['%s'] * len(cols_to_use))), tuple(row))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"Dados inseridos no banco {db_name} com sucesso.")
