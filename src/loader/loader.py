from sqlalchemy import create_engine
from abc import ABC, abstractmethod


class DataLoader(ABC):
    @abstractmethod
    def loader(self, *dfs):
        pass

class Loader(DataLoader):
    def __init__(self, db_url):

        self.url = db_url
    
    def loader(self, *dfs):

        engine = create_engine(self.url)

        for df, name in zip(dfs, ("film", "inventory", "rental", "customer", "store")):

            aux = df.toPandas()

            aux.to_sql(name, con=engine, if_exists="replace", index=False)

            print(f"Tabla {name} cargada a la base de datos")

        print("Tablas cargadas exitosamente") 


