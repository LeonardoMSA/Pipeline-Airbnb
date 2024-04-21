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
            host_id_pk SERIAL PRIMARY KEY,
            host_id INT,
            host_name VARCHAR(255),
            calculated_host_listings_count INT
        );
        CREATE TABLE IF NOT EXISTS dim_location (
            location_id SERIAL PRIMARY KEY,
            neighbourhood_group VARCHAR(255),
            neighbourhood VARCHAR(255),
            latitude NUMERIC(9,6),
            longitude NUMERIC(9,6),
            UNIQUE(latitude, longitude)
        );
        CREATE TABLE IF NOT EXISTS dim_property (
            property_id SERIAL PRIMARY KEY,
            name TEXT,
            room_type VARCHAR(255),
            price INT,
            availability_365 INT
        );
        CREATE TABLE IF NOT EXISTS dim_booking (
            booking_id SERIAL PRIMARY KEY,
            minimum_nights INT,
            number_of_reviews INT,
            last_review DATE,
            reviews_per_month NUMERIC(5,2)
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
        ON CONFLICT (host_id_pk) DO NOTHING;
        """, hosts.values.tolist())

        # Inserção na tabela de Localização
        locations = data[['neighbourhood_group', 'neighbourhood', 'latitude', 'longitude']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_location (neighbourhood_group, neighbourhood, latitude, longitude) VALUES (%s, %s, %s, %s)
        ON CONFLICT (location_id) DO NOTHING;
        """, locations.values.tolist())

        # Inserção na tabela de Propriedades
        properties = data[['name', 'room_type', 'price', 'availability_365']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_property (name, room_type, price, availability_365) VALUES (%s, %s, %s, %s)
        ON CONFLICT (property_id) DO NOTHING;
        """, properties.values.tolist())

        # Inserção na tabela de Reservas
        bookings = data[['minimum_nights', 'number_of_reviews', 'last_review', 'reviews_per_month']].drop_duplicates().dropna()
        psycopg2.extras.execute_batch(cursor, """
        INSERT INTO dim_booking (minimum_nights, number_of_reviews, last_review, reviews_per_month) VALUES (%s, %s, %s, %s)
        ON CONFLICT (booking_id) DO NOTHING;
        """, bookings.values.tolist())

        conn.commit()
        cursor.close()
        conn.close()
        print("Dados inseridos nas tabelas de dimensão com sucesso.")

    @staticmethod
    def createFactTables(dataset_name):
        postDatabase.initialize()
        conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
        cursor = conn.cursor()

        # Criação da tabela de fatos para Performance de Listagem
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_finance_performance (
            fact_listing_id SERIAL PRIMARY KEY,
            property_id INT,
            host_id INT,
            location_id INT,
            booking_id INT,
            average_price_neighboorhood_roomtype FLOAT,
            FOREIGN KEY (property_id) REFERENCES dim_property(property_id),
            FOREIGN KEY (host_id) REFERENCES dim_host(host_id_pk),
            FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
            FOREIGN KEY (booking_id) REFERENCES dim_booking(booking_id)
        );
        """)

        # Criação da tabela de fatos para Satisfação do Cliente
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS fact_host_performance (
            fact_customer_id SERIAL PRIMARY KEY,
            property_id INT,
            host_id INT,
            location_id INT,
            booking_id INT,
            avrg_price_host_neighborhood FLOAT,
            FOREIGN KEY (property_id) REFERENCES dim_property(property_id),
            FOREIGN KEY (host_id) REFERENCES dim_host(host_id_pk),
            FOREIGN KEY (location_id) REFERENCES dim_location(location_id),
            FOREIGN KEY (booking_id) REFERENCES dim_booking(booking_id)
        );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        print("Tabelas de fatos criadas com sucesso.")

    # @staticmethod
    # def insertFinancePerformanceFacts(dataset_name):
    #     postDatabase.initialize()
    #     conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
    #     cursor = conn.cursor()

    #     cursor.execute("""
    #     INSERT INTO fact_finance_performance (property_id, location_id, booking_id, average_price_neighborhood_roomtype)
    #     SELECT p.property_id, l.location_id, b.booking_id, AVG(p.price)
    #     FROM dim_property p
    #     JOIN dim_location l ON p.property_id = l.location_id
    #     JOIN dim_booking b ON p.property_id = b.property_id
    #     GROUP BY p.property_id, l.location_id, b.booking_id;
    #     """)
    #     conn.commit()
    #     cursor.close()
    #     conn.close()
    #     print("Dados de performance financeira inseridos com sucesso.")

    # @staticmethod
    # def insertHostPerformanceFacts(dataset_name):
    #     postDatabase.initialize()
    #     conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
    #     cursor = conn.cursor()

    #     # Calcular a média do preço das listagens de um anfitrião por bairro
    #     cursor.execute("""
    #     INSERT INTO fact_host_performance (property_id, location_id, booking_id, avrg_price_host_neighborhood)
    #     SELECT p.property_id, l.location_id, b.booking_id, AVG(p.price)
    #     FROM dim_property p
    #     JOIN dim_location l ON l.location_id = l.location_id
    #     JOIN dim_booking b ON p.property_id = b.booking_id
    #     GROUP BY p.property_id, l.location_id, b.booking_id;
    #     """)
    #     conn.commit()
    #     cursor.close()
    #     conn.close()
    #     print("Dados de performance do anfitrião inseridos com sucesso.")


    @staticmethod
    def populateFactTables(dataset_name):
        postDatabase.initialize()
        conn = psycopg2.connect(**postDatabase.params, dbname=dataset_name)
        cursor = conn.cursor()

        # Inserir dados na tabela fact_finance_performance
        cursor.execute("""
        INSERT INTO fact_finance_performance (property_id, host_id, location_id, booking_id)
        SELECT p.property_id, h.host_id, l.location_id, b.booking_id
        FROM dim_property p
        CROSS JOIN dim_host h
        CROSS JOIN dim_location l
        CROSS JOIN dim_booking b;
        """)
        
        # Inserir dados na tabela fact_host_performance
        cursor.execute("""
        INSERT INTO fact_host_performance (property_id, host_id, location_id, booking_id)
        SELECT p.property_id, h.host_id, l.location_id, b.booking_id
        FROM dim_property p
        CROSS JOIN dim_host h
        CROSS JOIN dim_location l
        CROSS JOIN dim_booking b;
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Tabelas de fatos populadas com sucesso.")