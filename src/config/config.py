import os

path = os.path.split(os.getcwd())[0]

INPUT_PATH = f"{path}/data/Films_2.xlsx"
DB_PATH = f"sqlite:///{path}/data/database.db"