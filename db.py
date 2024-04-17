import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import extras
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
        conn = psycopg2.connect(**postDatabase.params, dbname='postgres')
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dataset_name,))
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(f'CREATE DATABASE "{dataset_name}"')
            print(f"Banco de dados {dataset_name} criado com sucesso.")
        else:
            print(f"O banco de dados {dataset_name} já existe.")
        cursor.close()
        conn.close()


    @staticmethod
    def insertData(dataset_name, cols_to_use, file_name):
        postDatabase.initialize()

        conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
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

        print(f"Dados inseridos no banco {dataset_name} com sucesso.")



    @staticmethod
    def createDimensionTables(dataset_name):
        postDatabase.initialize()
        conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
        cursor = conn.cursor()

        # Criação de tabelas de dimensão
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_host (
            host_id INT PRIMARY KEY,
            host_name VARCHAR(255),
            calculated_host_listings_count INT
        );
        CREATE TABLE IF NOT EXISTS dim_location (
            neighbourhood_group VARCHAR(255),
            neighbourhood VARCHAR(255),
            latitude NUMERIC(9,6),
            longitude NUMERIC(9,6),
            PRIMARY KEY (neighbourhood, latitude, longitude)
        );
        CREATE TABLE IF NOT EXISTS dim_property (
            id INT PRIMARY KEY,
            name TEXT,
            room_type VARCHAR(255),
            price INT,
            availability_365 INT
        );
        CREATE TABLE IF NOT EXISTS dim_booking (
            id INT,
            minimum_nights INT,
            number_of_reviews INT,
            last_review DATE,
            reviews_per_month NUMERIC(5,2),
            PRIMARY KEY (id, last_review)
        );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabelas de dimensão criadas com sucesso.")

    @staticmethod
    def insertDataIntoDimensions(dataset_name, file_name):
        postDatabase.initialize()
        conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
        cursor = conn.cursor()

        data = pd.read_csv(file_name)
        
        # Inserção na tabela de Anfitriões
        hosts = data[['host_id', 'host_name', 'calculated_host_listings_count']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_host (host_id, host_name, calculated_host_listings_count) VALUES (%s, %s, %s)
        ON CONFLICT (host_id) DO NOTHING;
        """, hosts.values.tolist())

        # Inserção na tabela de Localização
        locations = data[['neighbourhood_group', 'neighbourhood', 'latitude', 'longitude']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_location (neighbourhood_group, neighbourhood, latitude, longitude) VALUES (%s, %s, %s, %s)
        ON CONFLICT (neighbourhood, latitude, longitude) DO NOTHING;
        """, locations.values.tolist())

        # Inserção na tabela de Propriedades
        properties = data[['id', 'name', 'room_type', 'price', 'availability_365']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_property (id, name, room_type, price, availability_365) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """, properties.values.tolist())

        # Inserção na tabela de Reservas
        bookings = data[['id', 'minimum_nights', 'number_of_reviews', 'last_review', 'reviews_per_month']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_booking (id, minimum_nights, number_of_reviews, last_review, reviews_per_month) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id, last_review) DO NOTHING;
        """, bookings.values.tolist())

        conn.commit()
        cursor.close()
        conn.close()
        print("Dados inseridos nas tabelas de dimensão com sucesso.")