import sqlite3
from datetime import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

# Função para adaptar datetime para string no formato aceito pelo SQLite
def adapt_datetime(dt):
    return dt.strftime('%Y-%m-%d %H:%M:%S')

# Função para converter string do SQLite para datetime do Python
def convert_datetime(s):
    return datetime.strptime(s.decode('utf-8'), '%Y-%m-%d %H:%M:%S')

# Registrar adaptadores e conversores
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("DATETIME", convert_datetime)




# **To register a product you need to give it a name and category, minimum stock, maximum stock, regular stock and location**
class Product:
    def __init__(self, product_code, name, category, min_stock, max_stock, regular_stock, location):
        self.product_code = product_code
        self.name = name
        self.category = category
        self.min_stock = min_stock
        self.max_stock = max_stock
        self.regular_stock = regular_stock
        self.location = location
        # **The registered product details are stored in a table, and the stock details are stored in another**
        self.create_tables()
        self.add_to_product_database()
        self.add_to_stock_database()
        
    def create_tables(self):
        with sqlite3.connect('inventory.db') as conn:
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
                movement_category VARCHAR(20),
                moved_quantity INTEGER,
                before_change VARCHAR(20),
                after_change VARCHAR(20),
                timestamp DATETIME 
            )
            """
            cursor.execute(create_movements_table)

            conn.commit()
            
    # ☆☆Add product static datas to Products TABLE☆☆
    def add_to_product_database(self):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            product_details = (self.product_code, self.name, self.category)
            
            # ---------Add to database---------
            try:               
                insert_product = "INSERT INTO Products (product_code, name, category) VALUES (?, ?, ?)"
                cursor.execute(insert_product, product_details)
                conn.commit()
                print(f"Produto '{self.name}' cadastrado com sucesso.")
            
            # *Catch UNIQUE column violation*
            except sqlite3.IntegrityError:
                print(f"Erro: já existe produto com código {self.product_code}")
                
    # ☆☆Add product dynamic datas to Stock TABLE☆☆
    def add_to_stock_database(self):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            stock_details = (self.product_code, self.name, self.min_stock, self.max_stock, self.regular_stock, self.location)
            
            # ---------Add to database---------
            try:
                insert_to_stock = "INSERT INTO Stock (product_code, name, min_stock, max_stock, regular_stock, location) VALUES (?, ?, ?, ?, ?, ?)"
        
                cursor.execute(insert_to_stock, stock_details)
                conn.commit()
                print(f"Estoque para produto '{self.product_code}' inserido com sucesso.")
                
            # *Catch UNIQUE column violation*
            except sqlite3.IntegrityError:
                print(f"Erro: já existe produto com código {self.product_code}")
        
    def update_data(self):
        pass
    
class Moves:
    def __init__(self):
        # Inserto movement logs into Movements TABLE
        self.log_movements = "INSERT INTO Movements (product_code, movement_category, moved_quantity, before_change, after_change, timestamp) VALUES (?, ?, ?, ?, ?, ?)"   
          
        self.update_stock = "UPDATE Stock SET real_stock = ? WHERE product_code = ?"
        
    # ☆☆☆ Product Sale (Reduce Stock) ☆☆☆
    def product_sale(self, product_code, sale_qnty):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            
            # Get current stock
            cursor.execute("SELECT real_stock FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()

            if result:
                current_stock = result[0]
                # Check if there is enough stock to make the sale
                if current_stock >= sale_qnty:
                    new_stock = current_stock - sale_qnty
                    #update stock table
                    cursor.execute(self.update_stock, (new_stock, product_code))
                    
                    date = datetime.now()
                    # insert into movement logs (refatorate? instance the execute code?)
                    cursor.execute(self.log_movements, (product_code, 'SALE', sale_qnty, current_stock, new_stock, date))
                    conn.commit()    
                    #talvez consultar novamente a table?
                    print(f"Foram vendidos {sale_qnty} pcs, do produto {product_code}, estoque atual = {new_stock} ")
                else:
                    print(f"Estoque do produto {product_code} insuficiente")
            # If not found in stock
            else:
                print(f"Produto {product_code} não encontrado no estoque")
                
                # ☆☆☆ Product Purchase (Add Stock)☆☆☆
    def stock_incrementing(self, product_code, purchase_qnty):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            
            # Get actual stock
            cursor.execute("SELECT real_stock FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()

            # If found in stock, add to real_stock
            if result:
                # (make function to sale and purchase, and just use the function here)
                current_stock = result[0]
                new_stock = current_stock + purchase_qnty
                cursor.execute(self.update_stock, (new_stock, product_code))
                
                date = datetime.now()
                # add to log (refactor?)
                cursor.execute(self.log_movements, (product_code, 'PURCHASE', purchase_qnty, current_stock, new_stock, date))
                conn.commit()    
                print(f"Foram adicionados {purchase_qnty} pcs, do produto {product_code} ao estoque, estoque atual = {new_stock} ")
            else:
                print(f"Produto {product_code} não encontrado no estoque")

                    # ☆☆ Stocking location change ☆☆ 
    def location_movement(self, product_code, new_location):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            
            # Get current location
            cursor.execute("SELECT location FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()

            if result:
                current_location = result[0]
                # Check if current location is not the new location, if not, move
                if current_location != new_location:
                    # move location
                    cursor.execute("UPDATE Stock SET location = ? WHERE product_code = ?", (new_location, product_code))
                    
                    date = datetime.now()
                    # log into movements (fix moved_qnty)
                    cursor.execute(self.log_movements, (product_code, 'RE-LOCATION', 0, current_location, new_location, date))
                    conn.commit()    
                    print(f"Produto {product_code} foi movimentado de {current_location} para {new_location}")
                else:
                    print(f"Produto {product_code} já se encontra na locação {new_location}")
            else:
                print(f"Produto {product_code} não encontrado no estoque")
    
    @staticmethod # Return the df with last 5 moves from movement logs
    def last_moves(moves):
        with sqlite3.connect('inventory.db') as conn:
            query = "SELECT * FROM Movements"
            df = pd.read_sql_query(query, conn)
            moves = []
            for i in range(moves):
                # check if df length is valid
                if len(df) - i - 1 >= 0:
                    moves.append(df.iloc[len(df) - i - 1])
                
            new_df = pd.DataFrame(moves)    
        return new_df
            
    # ☆☆☆☆Show a overall report☆☆☆☆
    def overall_report(self):
        with sqlite3.connect('inventory.db') as conn:
            query = "SELECT * FROM Stock"
            df = pd.read_sql_query(query, conn)
            
            # ------- can make function --------
            # create empty lists and append the product code as it's evaluated
            low_stock = []
            over_stock = []
            regular_stock = []
            for row in df.itertuples():
                if row.real_stock > row.max_stock:
                    over_stock.append(row.product_code)
                elif row.real_stock <= row.min_stock:
                    low_stock.append(row.product_code)
                else:
                    regular_stock.append(row.product_code)
                    
            print('Segue os produtos que estão em baixo estoque:')
            # --another function--
            print(df[df['product_code'].isin(low_stock)])
            print('Segue os produtos que estão com excesso de estoque:')
            print(df[df['product_code'].isin(over_stock)])
            
            # ultimas movimentações (function?)
            last_moves = self.last_moves(5)
            print('As últimas movimentações foram:')
            for row in last_moves.itertuples():
                print(f'Produto: {row.product_code}, Motivo: {row.movement_category}, Quantidade: {row.moved_quantity}, Antes: {row.before_change}, Depois: {row.after_change}, Horário: {row.timestamp}')
            
            # Estoque normal
            print('Segue os produtos que estão com estoque regular:')
            print(df[df['product_code'].isin(regular_stock)])
    
        
    # ☆☆☆Search for especific product code (simple report)☆☆☆
def simple_report(product_code):
    with sqlite3.connect('inventory.db') as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT product_code, name, real_stock, location FROM Stock WHERE product_code = ?', (product_code,))
        stock = cursor.fetchone()
        print(f'O produto {stock[1]}: {stock[0]}, está na locação {stock[3]}, com {stock[2]} unidades em estoque.')
    # ☆☆Show detailed information about stock and tips☆☆
    
def return_df_by_time_and_category(df, category, time):
    new_df = df[(df['movement_category'] == category) & (df['timestamp'] >= time)]
    return new_df
    
def return_last_sales_and_purchases():
    with sqlite3.connect('inventory.db') as conn:
        query_moves = "SELECT * FROM Movements"
        all_moves = pd.read_sql_query(query_moves, conn, parse_dates='timestamp')    
        query_products = "SELECT * FROM Products"
        all_products = pd.read_sql_query(query_products, conn)
        two_months_ago = datetime.now() - relativedelta(months=2)
        thirty_days_ago = datetime.now() - relativedelta(days=30)
        fifteen_days_ago = datetime.now() - relativedelta(days=15)
        last_thirty_days_sales = return_df_by_time_and_category(all_moves, 'SALE', thirty_days_ago)
        last_two_months_purchases = return_df_by_time_and_category(all_moves, 'PURCHASE', two_months_ago)

        not_saled = all_products[~all_products['product_code'].isin(last_thirty_days_sales['product_code'])]
        
        count_df = last_two_months_purchases.groupby('product_code').size().reset_index(name='count')
        recommend_add = {}
        for row in count_df.itertuples():
            if row.count > 3:
                recommend_add[row.product_code] = row.count
    return not_saled, recommend_add
    
            
def analized_report():
    not_saled, increase_stock_recommendation = return_last_sales_and_purchases()
    print('Segue algumas informações que podem ser relevantes:')
    print('Produtos que não são vendidos há mais de 30 dias.')
    for row in not_saled.itertuples():
        print(f'{row.name} de código {row.product_code}')
    print('Nos últimos 2 meses:')
    for product_code, sales in increase_stock_recommendation.items():
        print(f'O produto de código {product_code} teve pedido reposição de estoque {sales} vezes.')
        
    # criar produtos---------
# produto1 = Product('CAM-001', 'Camiseta', 'Vestuário', 10, 50, 30, 'VEST01')    
# produto2 = Product('CAM-002', 'Camiseta', 'Vestuário', 10, 50, 30, 'VEST01')    
# produto3 = Product('MAQ-001', 'Lava-louças', 'Eletrodomésticos', 2, 10, 4, 'ELET03')    
# produto4 = Product('COM-001', 'Processador', 'Eletrônicos', 5, 20, 10, 'COMP01')    
# produto5 = Product('PAP-001', 'Caneta', 'Papelaria', 100, 500, 200, 'PAP01')    
# produto6 = Product('CAS-001', 'Caneca', 'Casa', 0, 20, 10, 'PAP01')    
# produto7 = Product('CAS-002', 'Quadro', 'Casa', 2, 10, 5, 'PAP02')    
# produto8 = Product('COM-002', 'Teclado', 'Eletrônicos', 3, 20, 10, 'COMP02')    
# produto9 = Product('COM-003', 'Impressora', 'Eletrônicos', 2, 8, 5, 'COMP02')    
# produto10 = Product('MAQ-002', 'Fogão', 'Eletrodomésticos', 2, 10, 6, 'ELET02')
# --------------
# teste_report()
# simple_report('CAM-001')
# ----------teste de movimento----------
# movimento = Moves()
# movimento.stock_incrementing('CAM-001', 50)
# movimento.stock_incrementing('PAP-001', 150)
# movimento.product_sale('CAM-001', 10)
# movimento.location_movement('MAQ-001', 'NEW-LOQ')
# movimento.overall_report()
analized_report()
