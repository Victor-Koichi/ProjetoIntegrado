import sqlite3
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from classes import adapt_datetime

with sqlite3.connect('inventory.db') as conn:
    query_moves = "SELECT * FROM Movements"
    all_moves = pd.read_sql_query(query_moves, conn, parse_dates='timestamp')    
    query_products = "SELECT * FROM Products"
    all_products = pd.read_sql_query(query_products, conn)
    two_months_ago = datetime.now() - relativedelta(months=2)
    thirty_days_ago = datetime.now() - relativedelta(days=30)
    fifteen_days_ago = datetime.now() - relativedelta(days=15)
    last_thirty_days_sales = all_moves[(all_moves['movement_category'] == 'SALE') & (all_moves['timestamp'] >= thirty_days_ago)]
    last_two_months_purchases = all_moves[(all_moves['movement_category'] == 'PURCHASE') & (all_moves['timestamp'] >= two_months_ago)]

    not_saled = all_products[~all_products['product_code'].isin(last_thirty_days_sales['product_code'])]
    
    count_df = last_two_months_purchases.groupby('product_code').size().reset_index(name='count')
    recommend_add = {}
    for row in count_df.itertuples():
        if row.count > 3:
            recommend_add[row.product_code] = row.count
            
