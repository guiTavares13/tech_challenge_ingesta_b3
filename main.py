from connection import Connection
from scraper import Scraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()

driver = webdriver.Chrome( options=chrome_options)

conn = Connection()
scraper = Scraper(driver)  
scraper.extract_data_from_table()
headers = scraper.extract_table_headers()

while True:
    try:
        next_button = driver.find_element(By.XPATH, '//li[@class="pagination-next"]/a') 
        next_button.click()
        
        scraper.extract_data_from_table(execute_get=False)
    except:
        break

driver.quit()

if len(scraper.data) > 0:
    saved_file_name = scraper.save_to_parquet(headers)
    
    if saved_file_name is not None:
        conn.upload_file_to_s3(saved_file_name, "bucketbovespa", f'raw/{saved_file_name}')
