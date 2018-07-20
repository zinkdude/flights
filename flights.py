"""

python script -
Jeff Zinkerman = 5/21/2018

1) Download Table HTML Data from walmart web site
    https://www.orbitz.com...
2) Put into SQL DB


"""

import urllib3
import shutil
import io
import requests
import pandas as pd
import sqlalchemy
import pyodbc
import pymssql
import pymysql
import numpy as np
import lxml.html as LH
import json
import urlgenflights
from datetime import datetime
from datetime import date
from sqlalchemy.types import String
from sqlalchemy.types import DateTime
from sqlalchemy import text
from bs4 import BeautifulSoup

MY_SQL_FLAG = 1
MYSQL_SCHEMA_NAME = 'bpwebscrape'
SQL_CMD = 'Call spUpdateFlightData'
url_list = []

def create_urls():
    #Read from file and assign to an array
    in_file = urlgenflights.url_file  
    with io.open(in_file) as f:
        for newline in f.readlines():
            url_list.append(newline.rstrip())    
    
def insert_new_db() :
    try :
        db_engine1 = sqlalchemy.create_engine("mysql+mysqldb://xxxxxx/" + MYSQL_SCHEMA_NAME, echo_pool=True)
        res = db_engine1.execute(text(SQL_CMD).execution_options(autocommit=True))
        #print(res.name)
    except Exception as e:
        print('Error updating SQL Table on second time.',str(e),'\n')
    else :
        print('SQL Updated OK into second table.')
 
def write_db(df1, db_action):
    # write the DataFrame to a table in the sql database
    try :
        if (MY_SQL_FLAG ==1) :
            db_engine = sqlalchemy.create_engine("mysql+mysqldb:/xxxxxx/" + MYSQL_SCHEMA_NAME, echo_pool=True)
            # mysql adjustments - explicitly give string length as 50 and make table name lower case
            df1.to_sql("temp_flights_scrape", db_engine, if_exists=db_action, index=True, dtype={'date_added': DateTime, 'flight_date': DateTime, 'class': sqlalchemy.types.Integer, 'stops': sqlalchemy.types.Integer, 'page_title' : String(250), 'plane' : String(50)})
        else :
            db_engine = sqlalchemy.create_engine("mssql+pyodbc://zzz:xxxxxx@dsn_dbQuant")
            df1.to_sql("temp_flights_scrape", db_engine, if_exists=db_action, index=True, dtype={'date_added': DateTime, 'flight_date': DateTime, 'class': sqlalchemy.types.Integer, 'stops': sqlalchemy.types.Integer, 'page_title' : String(250), 'plane' : String(50)})
    except Exception as e:
        print('Error writing to SQL Table',str(e),'\n')
    else :
        print('SQL Updated OK into table.')


def scrape_url(url1) :
    header_no_bot = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    }
    page1 = requests.get(url1, verify=False, headers=header_no_bot)

    if (page1.status_code == 200) :    
        html = page1.content

        #res = requests.get(url1)
        soup = BeautifulSoup(html,'lxml')        
        page_title = soup.title.string
        print(page_title)
        #Need to wait for flights to refresh  - or find the background call, which is:
        #https://www.orbitz.com/Flight-Search-Paging?c=155be1bb-649f-4f86-915a-ab09d0d5d2c6&is=1&sp=asc&cz=200&cn=0&ul=0                                                       
        item_list = soup.find_all("script", attrs={"type": "text/javascript"})[1]
        #print(item_list)
        
        #Now Find Keyword :  logging.guid = "d8549bb1-4a58-4050-8406-cc0cb2180949"
        page_attributes = str(item_list)        
        page_attributes = page_attributes[page_attributes.find('Flight-Search-Paging')+23:page_attributes.find('Flight-Search-Paging')+59]        
        new_url = "https://www.orbitz.com/Flight-Search-Paging?c=" + page_attributes + "&is=1&sp=asc&cz=200&cn=0&ul=0"
        #print(new_url)    
        
        page1 = requests.get(new_url)
        air_data = page1.json()
        flight_data = air_data.get("content").get("legs")
        
        df = pd.pandas.DataFrame(columns=['date_added','flight_date','flight_time','airport_depart','airport_arrive','carrier','price','class','stops','plane','page_title'])
        
        flist = []                
        for key, value in flight_data.items():
            flist.append(value)            
        i = 0
        for (fitem) in flist:
            # print(fitem.get("price").get("formattedPrice"))   
            #print(fitem.get("price").get("totalPriceAsDecimal"))
            #print(fitem.get("carrierSummary").get("airlineName"), fitem.get("price").get("totalPriceAsDecimal"), fitem.get("stops") )        
            carrier = fitem.get("carrierSummary").get("airlineName")
            price = fitem.get("price").get("offerPrice")    #was totalPriceAsDecimal
            stops = fitem.get("stops")
            depart_airport = fitem.get("departureLocation").get("airportCode")
            arrive_airport = fitem.get("arrivalLocation").get("airportCode")
            flight_date = fitem.get("departureTime").get("date")
            if (flight_date != "") :
                flight_date = datetime.strptime(flight_date, "%m/%d/%Y")            #convert to a datetime !!!!!
                flight_date = datetime.strftime(flight_date,'%Y-%m-%d')    #convert to proepr DB format
            flight_time = fitem.get("departureTime").get("time")
            cabin_class = fitem.get("timeline")[0].get("carrier").get("cabinClass")
            plane =  fitem.get("timeline")[0].get("carrier").get("plane")   #i.e. Airbus A319, etc
            
            #>>> fitem.get("basicEconomy") --> {'enabled': False, 'areaOneTrip': False, 'rules': []}
            
            #insert into Dataframe
            # dt.datetime.now()
            df.loc[i] = [date.today(), flight_date, flight_time, depart_airport, arrive_airport, carrier, price, cabin_class, stops, plane, page_title]
            i= i + 1                                            
                                                                        
        print("Items Found: ", str(len(flist)))        
    else:
        print('Yo bro. Webpage no good.')        
    return df

def main() :
    urllib3.disable_warnings()
    urlgenflights.main()
    create_urls()    

    for j in range(len(url_list)) :     
        df1 = scrape_url(url_list[j])
        if not df1.empty :
            if (j==0) :
                write_db(df1,'replace')
            else :
                write_db(df1,'append') 
        else :
            print("scrape failed.")
            exit 
    insert_new_db()    
                               
if __name__ == '__main__': main()
