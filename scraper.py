from connection import Connection
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import pandas as pd
import time
import os

class Scraper:  
    def __init__(self, driver):
        self.driver = driver
        self.data = []

    # Função para extrair cabeçalhos da tabela
    def extract_table_headers(self):
        header_elements = self.driver.find_elements(By.XPATH, '//table[@class="table table-responsive-sm table-responsive-md"]/thead/tr/th')
        headers = [header.text.replace("Setor","setor").replace("Código","codigo").replace("Ação","acao").replace("Tipo","tipo").replace("Qtde. Teórica","qtd").replace("Part. (%)","participacao").replace("Part. (%)Acum.","participacao_acumulada").strip() for header in header_elements]
        headers.remove("%setor")
        headers.append("data_pregao")
        return headers

    def save_to_parquet(self, headers):
        df = pd.DataFrame(self.data, columns=headers)

        for col in df.columns:
            df[col] = pd.to_numeric(df[col].str.replace('.', '').str.replace(",",".").str.replace('%', ''), errors='ignore')

        filename = f'output_{datetime.now().strftime("%d_%m_%y")}.parquet.gzip'
        df.to_parquet(filename, compression='gzip')
        return filename
    
    def extract_data_from_table(self, execute_get=True):
        if execute_get:
            url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
            self.driver.get(url)
            time.sleep(5)
            
            select_element =  self.driver.find_elements(By.ID, "segment") 
            select = Select(select_element[0])
            select.select_by_value("2")

            time.sleep(5)

        rows = self.driver.find_elements(By.XPATH, '//table[@class="table table-responsive-sm table-responsive-md"]/tbody/tr')

        for row in rows:
            cols = row.find_elements(By.TAG_NAME, 'td')
            row_data = [col.text for col in cols]
            row_data.append(f'{datetime.now().strftime("%Y-%m-%d")}')
            self.data.append(row_data)

        return self.data
