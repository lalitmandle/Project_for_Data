#import all required libraries
import pandas as pd 
import numpy as np 
import requests
import sqlite3
from bs4 import BeautifulSoup 
from datetime import datetime

# Defining known values
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country','GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'
conn = sqlite3.connect(db_name)

#Task_1 - Extracting information 
def extract(url, table_attribs):
    #1. Extract the web page as text
    page = requests.get(url).text
    #2. Parse the text into an HTML object
    data = BeautifulSoup(page,'html.parser')
    #3. Create an empty pandas DataFrame with columns as the table_attribs
    df = pd.DataFrame(columns=table_attribs)
    #4. Extract all 'tbody' attributes of the HTML obj and axtract all rows of index 2 table using 'tr' attr
    table =data.find_all('tbody')
    rows = table[2].find_all('tr')
    #5. Check contents of each row, having attr 'td', for following conditions
    for row in rows:
        col = row.find_all('td')
        #a. row should not be empty
        if len(col) != 0:
            #b. first column should contain a hyperlink and third coln should not be '—'
            if col[0].find('a') is not None and '—' not in col[2]:
                #6. Store all entries matching the conditions in step 5 a dictionary with key same as entries
                #of table_attribs and append all these dictionaries one by one to the dataframe
                data_dict = {'Country':col[0].a.contents[0],
                            'GDP_USD_millions':col[2].contents}
                df1 = pd.DataFrame(data_dict,index = [0])
                df = pd.concat([df,df1], ignore_index = True)
    return df

#Task_2 - Transform information 
def transform(df):
    #1. convert contents of the 'GDP_USD_millions' coln of df from currency formate to floating numbers
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    #2. Divide all these values by 1000 and round it to 2 decimal places
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df['GDP_USD_millions'] = GDP_list
    #3. Modify the name of the coln from 'GDP_USD_millions' to 'GDP_USD_billions'
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

#Task_3- Loading information
#1.save the transformed df to csv file, for this pass the df and csv file path to the func load_to_csv()
def load_to_csv(df,csv_path):
    df.to_csv(csv_path)
#2. save the transformed df as table in the database, need to implemented in the fuctions 
#load_to_db(), which accepts the df the connection obj to the sql database and table name
def load_to_db(df,conn,table_name):
    df.to_sql(table_name, conn, if_exists='replace', index=False)

#Task_4 - Querying the database tables 
def run_query(query_statement,conn):
    print(query_statement)
    query_output = pd.read_sql(query_statement,conn)
    print(query_output)
    print("hello lalit ")

#Task_5 - Logging progress 
#need to be done by log_progress() func, this func will be called multiple times while the 
#execution of this code and will be asked to add a log entry in a.txt file 
#etl_project_log.txt the entry should be the following formate 
#'<Time_stamp>:<message_text>' and each entry must be in a separate line.
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now() #get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("./etl_project_log.txt","a") as f:
        f.write(timestamp + ' : '+message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
log_progress('SQL Connection initiated.')
load_to_db(df, conn, table_name)
log_progress('Data loaded to Database as table. Running the query')
query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, conn)
log_progress('Process Complete.')
conn.close()