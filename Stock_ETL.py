import mysql.connector as mc
import pandas as pd
from vnstock3 import Vnstock
from sqlalchemy import create_engine
import time
from datetime import datetime, timedelta
import schedule

class Stock_Importer:
    def __init__(self,DB,pw):
        self.host= 'localhost'
        self.user='root'
        self.password= pw.strip()
        self.database= DB
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}')
        self.stock = Vnstock(show_log=False).stock(source='VCI')
        df_lst = self.stock.listing.symbols_by_exchange()
        self.all_symbols = df_lst[df_lst['exchange'].isin(['HSX','HNX'])]['symbol'].to_list()
        # Replace with your actual credentials
        #self.engine = create_engine("mysql+pymysql://username:password@host:port/database")
    def connect(self):
        """Establishes a connection to the SQL database."""
        self.connection =mc.connect(
            host=self.host,
            user= self.user,
            password = self.password,
            database = self.database
        )
                                                               
    def Create_new_table(self,name,cols):
        #Connect to the database
        self.connect()
        #create table 
        cursor = self.connection.cursor()
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {name} ({cols} int) "
        cursor.execute(create_table_sql)
        self.connection.commit()
        
    def disconnect(self):
        #Closes the database connection
             self.connection.close()
         
    def exchange_dim(self):
        # Get data
        df_market = self.stock.listing.symbols_by_exchange()
        exchange_dim = df_market[['exchange']].drop_duplicates().reset_index(drop=True)
        exchange_dim['id_exchange'] = exchange_dim.index+1
        exchange = exchange_dim[['id_exchange','exchange']]
        # Use the DataFrame's to_sql method to import the data 
        exchange.to_sql('Dim_exchange', con=self.engine, if_exists='replace', index=False) 
    
    def dim_table_code_industry(self):
        #get data
        df = self.stock.listing.industries_icb()
        df_ma_nganh = df[['icb_name','icb_code']]
        # Imprort data to mysql
        df_ma_nganh.to_sql('dim_table_code_industry', con=self.engine, if_exists='replace', index=False)
    
    def list_company(self):
        # Get data from exchange_dim table:
        query = 'select * from Dim_exchange' # Query to fletch data from mysql 
        try:
            exchange_dim = pd.read_sql(query, con=self.engine)
        except Exception as e:
            pass
        finally:
            self.exchange_dim()
            exchange_dim = pd.read_sql(query, con=self.engine)
        # Get data & creat table
        df_market = self.stock.listing.symbols_by_exchange()
        df = self.stock.listing.symbols_by_industries()
        df_list_Company = df[['symbol','organ_name','icb_code3']]
        df_merge = pd.merge(df_market[['exchange','symbol']],df[['symbol','organ_name','icb_code3']],on ='symbol',how='left')
        df_company = pd.merge(df_merge,exchange_dim,on='exchange',how='left')
        df_company= df_company[[col for col in df_company.columns if col != 'exchange']]
        
        #import data to mysql:
        df_company.to_sql('List_company', con=self.engine, if_exists='replace', index=False)
    
    def stock_prices(self,date_start,date_end, method = 'append'):
        data = []
        for symbol in self.all_symbols:      
                try:
                    df = self.stock.quote.history(symbol=symbol, start=date_start, end=date_end) 
                    df['symbol'] =symbol
                    data.append(df[['time','symbol','close','volume']])
                except Exception:
                    pass
                #time.sleep(5)
        df_price = pd.concat(data)  
        df_price.to_sql('stock_prices', con=self.engine, if_exists= method, index=False)
    def finance_ratio(self):
        data = []
        for symbol in self.all_symbols:
            try:
                stock_data = Vnstock(show_log=False).stock(symbol=symbol, source='VCI').finance.ratio(period='quarter', lang='vi')
                data.append(stock_data)
            except Exception:
                pass
        df = pd.concat(data) 
        df.columns = df.columns.droplevel(0)
        df.to_sql('finance_ratio', con=self.engine, if_exists='replace', index=False)
    def company_prrofile(self):
        data =[]
        for symbol in self.all_symbols:
            try:
                company = Vnstock(show_log=False).stock(symbol=symbol, source='TCBS').company.overview()
                company['symbol'] =symbol
                data.append(company)
            except Exception:
                    pass
        df = pd.concat(data)
        df = df[df.columns[3:]]  
        df.to_sql('Company_profile', con=self.engine, if_exists='replace', index=False)
    def balance_sheet(self):
        data =[]
        for symbol in self.all_symbols:
            try:
                df_bs = Vnstock(show_log=False).stock(symbol=symbol, source='TCBS').finance.balance_sheet( period='quarter')
                df_bs['symbol'] =symbol
                data.append(df_bs)
            except Exception:
                    pass
        df = pd.concat(data) 
        df.to_sql('Balance_sheet', con=self.engine, if_exists='replace', index=False)
    
    def PnL(self):
        data =[]
        for symbol in self.all_symbols:
            try:
                df_Pnl = Vnstock(show_log=False).stock(symbol=symbol, source='TCBS').finance.income_statement(period='quarter')
                df_Pnl['symbol'] =symbol
                data.append(df_Pnl)
            except Exception:
                    pass
        df = pd.concat(data) 
        df.to_sql('PnL', con=self.engine, if_exists='replace', index=False)
        
    def update_prices(self):
        try:
            # Establish database connection
            self.connect()
            cursor = self.connection.cursor()

            # Execute SQL query to get the last date.
            query = "SELECT MAX(TIME) AS last_date FROM stock_prices"  
            cursor.execute(query)
            result = cursor.fetchone()

            # Extract the date and convert to pandas Timestamp.
            today = datetime.now().date()
            last_date = result[0]
            #last_date = datetime.strptime(last_date, "%Y-%m-%d")
            if  today > last_date:
                if datetime.now().hour >=15:
                    start_date = last_date + timedelta(days=1)
                    self.stock_prices(start_day.strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d'),'append')
                else:
                    start_day = last_date + timedelta(days=1)    
                    end_date = datetime.now() -timedelta(days=1) 
                    self.stock_prices(start_day.strftime('%Y-%m-%d'),end_date.strftime('%Y-%m-%d'),'append')
                print('finished update..')
            else: 
                pass               
        #except Exception as err:
         #       pass
        finally:
            self.disconnect()

# Schedule the import_data function to run every day at a specific time 
def job_price():
    data = []
    stock = Stock_Importer('STOCK','Eninoskybaby94$')
    date = datetime.date.today().strftime('%Y-%m-%d')
    for symbol in stock.all_symbols:
        try:
            df = stock.stock.quote.history(start= date, end= date, interval='1D')
            data.append(df)
        except Exception as e:
            pass
    df = pd.concat(data)
    df.to_sql('stock_prices', con=stock.engine, if_exists='append', index=False)

stock = stock = Stock_Importer('STOCK','Eninoskybaby94$')
stock.update_prices()

























