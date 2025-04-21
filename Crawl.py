import mysql.connector
import pandas as pd
from vnstock import Vnstock


#Establish a connection to your MySQL database:
def connect_DB():
    mydb = mysql.connector.connect(
    host="your_host",
    user="your_user",
    password="your_password",
    database="your_database"
    )
    mycursor = mydb.cursor()

        # Create a table in your database to store the data:
    mycursor.execute("CREATE TABLE IF NOT EXISTS stock_prices (symbol VARCHAR(255), date DATE, open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume INT)")

    #Iterate through the symbols and insert the data into the table:

    all_symbols = df_list_Company['symbol'].tolist()
    stock = Vnstock()

# Create exchange_dim 
def exchange_dim():
    stock = Vnstock(show_log=False).stock(source='VCI')
    df_market = stock.listing.symbols_by_exchange()
    exchange_dim = df_market[['exchange']].drop_duplicates().reset_index(drop=True)
    exchange_dim['id_exchange'] = exchange_dim.index+1
    exchange_dim[['id_exchange','exchange']]
    # Cursor
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS exchange_dim (
    exchange_id INT PRIMARY KEY,
    exchange VARCHAR(255)
    )
""")
    for index, row in exchange_dim.iterrows():
        cursor.execute("""
    INSERT INTO exchange_dim (exchange_id, exchange)
    VALUES (%s, %s)
    """, (row['exchange_id'], row['exchange']))
    connection.commit()
    connection.commit()

# Create dim_table code_industry
def dim_table_code_industry():
    
    df = stock.listing.industries_icb()
    df_ma_nganh = df[['icb_name','icb_code']]
    df_ma_nganh

    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS df_ma_nganh (
        icb_code VARCHAR(255) PRIMARY KEY, 
        icb_name VARCHAR(255)
    )
    """)
    connection.commit()
    for index, row in df_ma_nganh.iterrows():
        cursor.execute("""
        INSERT INTO df_ma_nganh (icb_code, icb_name)
        VALUES (%s, %s)
        """, (row['icb_code'], row['icb_name']))
    connection.commit()

def company():
    df = stock.listing.symbols_by_industries()
    df_list_Company = df[['symbol','organ_name','icb_code3']]
    df_merge = pd.merge(df_market[['exchange','symbol']],df[['symbol','organ_name','icb_code3']],on ='symbol',how='left')
    df_company = pd.merge(df_merge,exchange_dim,on='exchange',how='left')
    df_company= df_company[[col for col in df_company.columns if col != 'exchange']]
    df_company.head(10)
    cursor = connection.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS df_list_Company (
    symbol VARCHAR(255) PRIMARY KEY, 
    exchange VARCHAR(255),
    organ_name VARCHAR(255),
    icb_code3 VARCHAR(255)
    )
    """)
    
    for index, row in df_list_Company.iterrows():
        cursor.execute("""
        INSERT INTO df_list_Company (symbol, exchange, organ_name, icb_code3)
        VALUES (%s, %s, %s, %s)
        """, (row['symbol'], row['exchange'], row['organ_name'], row['icb_code3']))
        connection.commit()
        connection.commit()

def stock_prices():
    for symbol in all_symbols:
        try:
            df = stock.quote.history(symbol=symbol, start='2006-01-01', end='2024-12-04')
            for index, row in df.iterrows():
                sql = "INSERT INTO stock_prices (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                val = (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'])
                mycursor.execute(sql, val)
                mydb.commit()
        except Exception:
            pass

def table_ratio():
    cursor = mydb.cursor()
    cursor.execute("SELECT DISTINCT symbol FROM your_table")  # Replace 'your_table'
    symbols = [row[0] for row in cursor.fetchall()]

for symbol in symbols:
  try:
    df_ratio = stock.finance.ratio(period='year', lang='en', dropna=True, symbol = symbol)
    for index, row in df_ratio.iterrows():
        sql = "INSERT INTO your_table_name (column1_name, column2_name, ...) VALUES (%s, %s, ...)"
        values = (row['column1_name'], row['column2_name'], ...)  # Adapt to your column names
        cursor.execute(sql, values)
        mydb.commit()
  except Exception:
      pass  



