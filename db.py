import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd

class postDatabase:
    @staticmethod
    def createDatabase(dataset_name, cols_to_use):
        params = {
            'host': 'localhost',
            'user': 'postgres',
            'password': '123'
        }

        db_name = dataset_name # THIS DB NAME É O DO CREATE DATABASE

        conn = psycopg2.connect(**params, dbname='postgres') # THIS ONE É DO NOME DO BANCO QUE TAMO USANDO (POSTGRES)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE {db_name}")
        cursor.close()
        conn.close()

        print(f"Banco de dados {db_name} criado com sucesso.")

    def insertData(dataset_name, cols_to_use):
        params = {
            'host': 'localhost',
            'user': 'postgres',
            'password': '123'
        }

        db_name = dataset_name

        conn = psycopg2.connect(**params, dbname=db_name)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS airbnb_data (
                id SERIAL PRIMARY KEY,
                {0}
            )
        """.format(', '.join([f"{col} VARCHAR" for col in cols_to_use])))
        conn.commit()

        file_name = f'kaggle/{dataset_name}.csv'
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
