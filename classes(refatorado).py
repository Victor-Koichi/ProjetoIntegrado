import sqlite3
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# ☆☆ Datetime adapting
    # Function to adapt datetime to SQLite compatible string format
def adapt_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

    # Function to convert SQLite string to Python datetime
def convert_datetime(s):
    return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

    # Register adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)


class InventoryManager:
    def __init__(self, inventory_db):
        self.inventory_db = inventory_db
        # **The registered product details are stored in a table, and the stock details are stored in another**
        self.create_tables()
        
# ☆☆Create Products and Stock TABLEs☆☆  
    def create_tables(self):
        with sqlite3.connect(self.inventory_db) as conn:
            cursor = conn.cursor()
            # ---------Create Products Table---------
            create_products_table = """
            CREATE TABLE IF NOT EXISTS Products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code VARCHAR(7) UNIQUE,
                name VARCHAR(30) NOT NULL,
                category VARCHAR(20) NOT NULL
            );
            """
            cursor.execute(create_products_table)
            # ---------Create Stock Table---------
            create_stock_table = """
            CREATE TABLE IF NOT EXISTS Stock (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code VARCHAR(7) UNIQUE,
                name VARCHAR(30) NOT NULL,
                real_stock INTEGER NOT NULL DEFAULT 0,
                min_stock INTEGER NOT NULL,
                max_stock INTEGER NOT NULL,
                regular_stock INTEGER NOT NULL,
                location VARCHAR(20) NOT NULL,
                FOREIGN KEY (product_code) REFERENCES Products (product_code) ON DELETE CASCADE
            );
            """
            cursor.execute(create_stock_table)
            # ------------Create Movements Table----------(add partname after)
            create_movements_table = """
            CREATE TABLE IF NOT EXISTS Movements (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code VARCHAR(7),
                name VARCHAR(30) NOT NULL,
                movement_category VARCHAR(20),
                moved_quantity INTEGER,
                before_change INTEGER,
                after_change INTEGER,
                timestamp DATETIME 
            )
            """
            cursor.execute(create_movements_table)
            conn.commit()
            
# ☆☆Add producto to Products and Stock TABLEs☆☆         
    def add_product(self, product_code, name, category, min_stock, regular_stock, max_stock, location):
        if min_stock >= 0 and regular_stock > min_stock and max_stock > regular_stock:
            product_details = (product_code, name, category)
            stock_details = (product_code, name, min_stock, regular_stock, max_stock, location)
            self.add_to_tables(self.inventory_db, product_details, stock_details)
        else:
            print('O estoque mínimo deve ser 0 ou maior, o estoque regular maior que o mínimo e o estoque máximo maior que o regular.')
    
    @staticmethod
    def add_to_tables(inventory_db, product_details, stock_details):
        with sqlite3.connect(inventory_db) as conn:
            cursor = conn.cursor()
            try:
                # ----Add product static details to Products TABLE----   
                insert_product = "INSERT INTO Products (product_code, name, category) VALUES (?, ?, ?)"
                cursor.execute(insert_product, product_details)
                # -------Add product dynamic datas to Stock TABLE------
                insert_to_stock = "INSERT INTO Stock (product_code, name, min_stock, regular_stock, max_stock, location) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_to_stock, stock_details)
                conn.commit()
                print(f"Produto '{product_details[1]}' cadastrado e inserido ao estoque com sucesso.")
            
                # *Catch UNIQUE column violation*
            except sqlite3.IntegrityError:
                print(f"Erro: já existe produto com código {product_details[0]}")
                
# ☆☆☆ Product Sale (Reduce Stock) ☆☆☆       
    def product_sale(self, product_code, sale_qnty):
        if not sale_qnty > 0:
            print('Insira quantidade de venda maior que 0.')
        else:
            with sqlite3.connect(self.inventory_db) as conn:
                cursor = conn.cursor()
                    # Get current stock
                result = self.get_real_stock(cursor, product_code)
                    # Check if there is enough stock to make the sale
                if result:
                    current_stock = result[0]
                    if current_stock >= sale_qnty:
                        new_stock = current_stock - sale_qnty
                        update_data = (new_stock, product_code)
                        log_data = (product_code, result[1], 'SALE', sale_qnty, current_stock, new_stock)
                            # update Stock and create log in Movements
                        self.update_stock(cursor, update_data, log_data)
                        conn.commit()
                        
                        print(f"Foram vendidos {sale_qnty} pcs, do produto {product_code}, estoque atual = {new_stock} ")
                    else:
                        print(f'Estoque do produto {product_code} insuficiente.')
                else:
                    print(f"Produto {product_code} não encontrado no estoque")
                    
# ☆☆☆ Product Purchase (Add Stock)☆☆☆
    def product_purchase(self, product_code, purchase_qnty):
        if not purchase_qnty > 0:
            print('Insira quantidade de compra maior que 0.')
        else:
            with sqlite3.connect(self.inventory_db) as conn:
                cursor = conn.cursor()
                    # Get current stock
                result = self.get_real_stock(cursor, product_code)
                    # If found in stock, increment real_stock
                if result:
                    current_stock = result[0]
                    new_stock = current_stock + purchase_qnty
                    update_data = (new_stock, product_code)
                    log_data = (product_code, result[1], 'PURCHASE', purchase_qnty, current_stock, new_stock)
                        # update Stock and create log in Movements
                    self.update_stock(cursor, update_data, log_data)
                    conn.commit()
                    
                    print(f"Foram adicionados {purchase_qnty} pcs, do produto {product_code} ao estoque, estoque atual = {new_stock} ")
                else:
                    print(f"Produto {product_code} não encontrado no estoque")

    @staticmethod
    def get_real_stock(cursor, product_code):
            cursor.execute("SELECT real_stock, name FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()
            return result

    @staticmethod
    def update_stock(cursor, new_stock, logs):
        sql = "UPDATE Stock SET real_stock = ? WHERE product_code = ?"
        cursor.execute(sql, new_stock)
        
        date = datetime.now()
        logs = logs + (date,)
        sql = "INSERT INTO Movements (product_code, name, movement_category, moved_quantity, before_change, after_change, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)" 
        cursor.execute(sql, logs)
        
# ☆☆☆Search for especific product code (simple report)☆☆☆
    def simple_report(self, product_code):
        with sqlite3.connect(self.inventory_db) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT product_code, name, real_stock, location FROM Stock WHERE product_code = ?', (product_code,))
            stock = cursor.fetchone()
        print(f'O produto "{stock[1]}: {stock[0]}", está na locação {stock[3]}, com {stock[2]} unidades em estoque.')
# ☆☆☆☆Show a overall report☆☆☆☆
    def overall_report(self, moves_qnty=5):
        with sqlite3.connect(self.inventory_db) as conn:
            query = "SELECT * FROM Stock"
            oa_df = pd.read_sql_query(query, conn)
            query = "SELECT * FROM Movements"
            moves_df = pd.read_sql_query(query, conn)
        # Filter products from Stock that are with low/regular/over stock
        self.filter_df(oa_df)
        # Show last 5 movement logs
        self.last_moves(moves_df, moves_qnty)

    @staticmethod
    def filter_df(df):
        filter = {'low_stock': [], 'regular_stock': [], 'over_stock': []}
        for row in df.itertuples():
            if row.real_stock > row.max_stock:
                filter['over_stock'].append(row.product_code)
            elif row.real_stock <= row.min_stock:
                filter['low_stock'].append(row.product_code)
            else:
                filter['regular_stock'].append(row.product_code)

        print('Segue os produtos que estão em baixo estoque:')
        print(df[df['product_code'].isin(filter['low_stock'])])
        print('Segue os produtos que estão com excesso de estoque:')
        print(df[df['product_code'].isin(filter['over_stock'])])
        print('Segue os produtos que estão com estoque regular:')
        print(df[df['product_code'].isin(filter['regular_stock'])])
        
    @staticmethod
    def last_moves(df, moves_qnty):
        print(f'Abaixo estão as últimas {moves_qnty} movimentações:')
        moves = []
        for i in range(moves_qnty):
            # check if df length is valid
            if len(df) - i - 1 >= 0:
                moves.append(df.iloc[len(df) - i - 1])
        new_df = pd.DataFrame(moves)
        for row in new_df.itertuples():
            print(f'Produto: {row.product_code}, Operação: {row.movement_category}, Quantidade: {row.moved_quantity}, Antes: {row.before_change}, Depois: {row.after_change}, Horário: {row.timestamp}')
            
# ☆☆☆☆Analyse stock condition☆☆☆☆
        # Look for items that has not been sold for the specified sales days
        # Look for items that had stock purchase more than the specified times, within specified months
    def analized_report(self, sales_days=30, purchases_months=2, purchase_count=4):
        moves_df, products_df = self.get_movements_and_products_df()
        last_sales, last_purchases = self.filter_last_sales_and_purchases(moves_df, sales_days, purchases_months)
        not_saled = self.not_saled_items(products_df, last_sales)
        purchases_more_than_x = self. filter_by_purchases_count(last_purchases, purchase_count)
        
        print(f'Produtos que não são vendidos há mais de {sales_days} dias.')
        for row in not_saled.itertuples():
            print(f'{row.name} de código {row.product_code}')
        print(f'Nos últimos {purchases_months} meses:')
        for product_code, sales in purchases_more_than_x.items():
            print(f'O produto de código {product_code} teve pedido reposição de estoque {sales} vezes.')
        
        
    
    def get_movements_and_products_df(self):
        with sqlite3.connect(self.inventory_db) as conn:
            query = "SELECT * FROM Movements"
            moves_df = pd.read_sql_query(query, conn, parse_dates='timestamp')
            query = "SELECT * FROM Products"
            products_df = pd.read_sql_query(query, conn)
        return moves_df, products_df
    
    def filter_last_sales_and_purchases(self, moves_df, sales_days, purchases_months):
        x_months_ago = datetime.now() - relativedelta(months=purchases_months)
        x_days_ago = datetime.now() - relativedelta(days=sales_days)
        last_x_days_sales = self.filter_by_time_category(moves_df, 'SALE', x_days_ago)
        last_x_months_purchases = self.filter_by_time_category(moves_df, 'PURCHASE', x_months_ago)
        return last_x_days_sales, last_x_months_purchases
    @staticmethod
    def filter_by_time_category(df, operation, time):
        df_filtered = df[(df['movement_category'] == operation) & (df['timestamp'] >= time)]
        return df_filtered
    @staticmethod
    def not_saled_items(products_df, last_sales):
        return products_df[~products_df['product_code'].isin(last_sales['product_code'])]
    @staticmethod
    def filter_by_purchases_count(last_purchases, count):
        filter_df = last_purchases.groupby('product_code').size().reset_index(name='count')
        more_than_count_purchases = {}
        for row in filter_df.itertuples():
            if row.count >= count:
                more_than_count_purchases[row.product_code] = row.count
        return more_than_count_purchases
    
manage = InventoryManager('inventoring.db')
# manage.add_product('CAM-001', 'Camiseta', 'Vestuário', 10, 30, 50, 'VEST01')
# manage.add_product('CAM-002', 'Camiseta', 'Vestuário', 10, 30, 50, 'VEST01')
# manage.add_product('MAQ-001', 'Lava-louças', 'Eletrodomésticos', 2, 4, 10, 'ELET03')    
# manage.add_product('COM-001', 'Processador', 'Eletrônicos', 5, 10, 20, 'COMP01')
# manage.add_product('PAP-001', 'Caneta', 'Papelaria', 100, 200, 500, 'PAP01')
# manage.add_product('CAS-001', 'Caneca', 'Casa', 0, 10, 20, 'PAP01')
# manage.product_purchase('CAM-001', 30)
# manage.product_purchase('PAP-001', 600)
# manage.product_purchase('COM-001', 4)
# manage.product_purchase('MAQ-001', 1)
# manage.analized_report()
manage.overall_report()

# manage.add_product('CAS-004', 'Caneca', 'Casa', 0, 10, 20, 'PAP01')
