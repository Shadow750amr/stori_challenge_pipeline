import requests
import logging
import pandas as pd
import json

 
logger = logging.getLogger(__name__) #tracking

URL = 'https://fakestoreapi.com/products'
OUT_JSON_NAME = 'fake_store_products.json'
OUT_CSV_NAME = 'fake_store_products.csv'

class Extraction:
    '''
    This class manages archives downloads from an URL and its storage in a local repo in both csv and json file for audit process.
    Atributes: url (str): resource path
               filename (str): the file's name (also cona be a desination path)
    Methods:   Executes customed download process (streamning) and save it.

    '''
    def __init__(self, url: str, filename: str,output_name:str) -> None:                    # Despite this is declarative I specically used an expected return
        self.url = url
        self.filename = filename
        self.output_name = output_name
        logger.info("Clase Extraction instanciada correctamente.")
        return None

    def connect_and_save(self) ->str:
        try:
            with requests.get(self.url, stream=True) as response:
                response.raise_for_status()
                with open(self.filename, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):    # Notice the chunk size it is intentional so there is no RAM affectation
                        if chunk:
                            file.write(chunk)
            logger.info(f"Archivo {self.filename} generado con éxito.")
            return self.filename
        except Exception as e:
            logger.error(f"Error en la descarga: {e}")
    def flatten_json(self) -> str:
        with open(self.filename,'r') as json_file:
            raw_json = json.load(json_file)
            data = pd.json_normalize(raw_json)
            data.to_csv(self.output_name)

if __name__ == "__main__":
    extractor = Extraction(URL, OUT_JSON_NAME,OUT_CSV_NAME)
    extractor.connect_and_save()
    extractor.flatten_json()

    