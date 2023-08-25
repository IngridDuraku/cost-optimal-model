from sqlalchemy import create_engine
from dotenv import dotenv_values

config = dotenv_values()
engine = create_engine(f'postgresql+psycopg2://{config["USERNAME"]}:{config["PASSWORD"]}@{config["HOST"]}/{config["DATABASE"]}')
