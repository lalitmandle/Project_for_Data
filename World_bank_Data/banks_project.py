# import all importent library
import pandas as pd 
import numpy as np 
import requests 
import sqlite3
from bs4 import BeautifulSoup 
from datetime import datetime 

url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
output_csv_path = "./Largest_banks_data.csv"
table_attribs = ["Name", "MC_USD_Billion"]
table_attris_final = ["Name","MC_USD_Billion","MC_GBP_Billion","MC_EUR_Billion","MC_INR_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
conn = sqlite3.connect("Banks.db")

# log_file = code_log.txt

#Extraction of data
def extract(url,table_attris):
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    table =data.find_all('tbody')
    rows = table[1].find_all('tr')
    for row in rows:
        col = row.find_all()
    print(rows)
extract(url,table_attribs)

