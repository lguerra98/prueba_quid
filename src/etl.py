from extractor.excel_extractor import ExcelExtractor
from transformer.transformer import Transformer
from loader.loader import Loader
from config.config import INPUT_PATH, DB_PATH

class ETL:
    def __init__(self, extractor, transformer, loader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
    
    def execute(self):
        dfs = self.extractor.extract()
        dfs_mod = self.transformer.transform(*dfs)
        self.loader.loader(*dfs_mod)

def main():

    extractor = ExcelExtractor(INPUT_PATH)
    transformer = Transformer()
    loader = Loader(DB_PATH)

    etl = ETL(extractor, transformer, loader)

    etl.execute()

if __name__ == "__main__":
    
    main()
        