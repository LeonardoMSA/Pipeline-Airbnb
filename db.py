import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

params = {
    'host': 'localhost',
    'user': 'postgres',
    'password': 'leoazevedo0622'
}

db_name = 'paiva'

conn = psycopg2.connect(**params, dbname='postgres')
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Cria um cursor e o novo banco de dados    
cursor = conn.cursor()
cursor.execute(f"CREATE DATABASE {db_name}")
cursor.close()
conn.close()

print(f"Banco de dados {db_name} criado com sucesso.")