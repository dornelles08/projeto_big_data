from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()


username = os.environ['POSTGRES_USER']
password = os.environ['POSTGRES_PASS']
ipaddress = os.environ['POSTGRES_HOST']
port = os.environ['POSTGRES_PORT']
dbname = 'big-data'

postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'

cnx = create_engine(postgres_str)
