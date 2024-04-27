# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import pandas as pd 
import numpy as np 
import requests
import sqlite3
from datetime import datetime

#initializing variables
url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

#web extraction
def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = soup.find_all('tbody')
    rows = tables[2].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {'Country': col[0].a.contents[0],
                             'GDP_USD_millions': col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df
            
#define transform function
def transform(df):
    df['GDP_USD_millions'] = df['GDP_USD_millions'].apply(lambda x: float(x.replace(',', '')))
    df['GDP_USD_millions'] =  np.round(df['GDP_USD_millions']/1000, 2)
    df = df.rename(columns={'GDP_USD_millions': 'GDP_USD_billions'})
    return df

#define functions to load to CSV/database
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    

#define query function
def run_query(string):
    conn = sqlite3.connect(db_name)
    result = pd.read_sql(string, conn)
    print(string)
    print(result)
           
            
#define logging function
def log_progress(message):
    now = datetime.now().strftime('%Y %m %d %H:%M:%S')
    with open('etl_project_log.txt', 'a') as appendfile:
        appendfile.write(now + ': ' + message + '\n')
        
#_____________________________________________________________________________________________
#_____________________________________ETL implementation______________________________________

log_progress('Begin web extraction')
df = extract(url, table_attribs)
log_progress('Extract complete')

log_progress('Begin data transformation')
df = transform(df)
log_progress('Transformation complete')

log_progress('Begin load to csv')
load_to_csv(df, csv_path)
log_progress('Load complete')

log_progress('Begin load to database')
load_to_db(df, table_name)
log_progress('Load complete')

run_query(f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100")

log_progress('Process complete')