from sqlalchemy import create_engine

username = 'postgres'
password = '123456'
ipaddress = 'localhost'
port = 15432
dbname = 'big-data'

postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'

cnx = create_engine(postgres_str)
