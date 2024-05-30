from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import datetime as dt
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def extract_transform():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.get("https://banggia.vps.com.vn/chung-khoan/derivative-VN30")
    driver.implicitly_wait(1000)

    utc_offset = dt.timedelta(hours=7)
    current_datetime = datetime.now()
    current_datetime= current_datetime + utc_offset
    current_datetime = current_datetime.strftime("%d/%m/%Y %H:%M:%S")
    current_datetime = current_datetime[:-2] + '00'

    price = driver.find_elements(By.XPATH, "/html/body/div/div/div[5]/div[1]/table/tbody/tr[1]/td[11]/span[1]")
    price = [pri.text for pri in price]
    price = ''.join(map(str,price))
    price = price.replace("," , "")
    price = float(float(price))

    volume = driver.find_elements(By.XPATH, "/html/body/div/div/div[5]/div[1]/table/tbody/tr[1]/td[21]/span")
    volume = [vol.text for vol in volume]
    volume = ''.join(map(str,volume))
    volume = volume.replace("," , "")
    volume = int(float(volume))
    
    # Create DataFrame
    data = {
        'DateTime': [current_datetime],
        'Price': [price],
        'Volume': [volume],
    }
    df = pd.DataFrame(data)   
    return df

def load(df):
    # load data into AWS RDS
    engine = create_engine('postgresql://tuannm101:minhtuan2410A@tuannm101.cuknr6tjarlj.ap-southeast-1.rds.amazonaws.com:5432/tuannm101')
    df.to_sql("vn30f1m", engine, if_exists="append", index=False)

def etl():
    load(extract_transform())

    
