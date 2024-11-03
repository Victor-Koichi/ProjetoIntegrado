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
        # Select database
        self.inventory_db = inventory_db
        self.create_tables()
        self.user_code = None
        self.privillege = 0
        self.start()
    
    def start(self):
        while True:
            option = int(input("Para fazer login digite 1, para criar novo usuário digite 2"))
            if option == 1:
                user = input("Digite seu id: ")
                self.user_code = user
                self.log_user()
            elif option == 2:
                self.create_user()
            else:
                print("Tente novamente")
                
    def create_user(self):
        while True:
            new_user = input("Deseja criar novo usuário? Y/N:\n")
            if new_user == "Y":
                # Input user name and position
                new_user = input("Digite seu nome:\n")
                privillege = int(input("Digite o número do seu cargo (Estoquista[1], Usuário[2], Gerente de Setor[3]):\n"))
                while True:
                    if privillege != 1 and privillege != 2 and privillege != 3 :
                        print("Cargo inválido, tente novamente")
                    else:
                        with sqlite3.connect(self.inventory_db) as conn:
                            cursor = conn.cursor()
                            insert_user = "INSERT INTO USERS (name, privillege) VALUES (?,?)"
                            cursor.execute(insert_user, (new_user, privillege))
                            conn.commit()
                            break
                print(f"{new_user} Cadastrado.")
                self.privillege = privillege
                break
            elif new_user == "N":
                print('Comece novamente')
                break
            else:
                print('Tente novamente')
    def log_user(self):
        with sqlite3.connect(self.inventory_db) as conn:
            cursor = conn.cursor()
            while True:
                cursor.execute("SELECT id, name, privillege FROM Users WHERE id = ?", (self.user_code,))
                result = cursor.fetchone()
                # If user found, get the privilleges
                if result:
                    self.privillege = result[2]
                    print(f'Bem vindo, {result[1]}.\nO que deseja fazer?')
                    comando = int(input('[0] Buscar o produto pelo código, [1] Relatório de posição semanal, [2] Análise do estado do estoque,\n[3] Solicitar reposição de estoque, [4] Registrar entrada de produto, [5] Venda de produto,\n[6] Adicionar produto novo, [7] Aprovar reposição de estoque, [8] Procurar ordens de reposição em aberto\n: '))
                    if comando == 0:
                        consult = input("Digite o código do produto: ")
                        self.simple_report(consult)
                    elif comando == 1:
                        print("Segue o relatório semanal: ")
                        self.overall_report()
                    elif comando == 2:
                        print("Segue a análise do estado do estoque: ")
                        self.analized_report()
                    elif comando == 3:
                        # adicionar validações
                        p_code = input("Para solicitar reposição de estoque, digite o código do produto: ")
                        name = input("Agora digite o nome do produto: ")
                        quantity = int(input("Digite a quantidade necessária: "))
                        self.purchase_order(p_code, name, quantity)    
                    elif comando == 4:
                        p_code = input("Para registrar entrada, digite o código do produto: ")
                        quantity = int(input("Digite a quantidade: "))
                        nf = input("Digite o número da NF: ")
                        self.product_entry(p_code, quantity, nf)
                    elif comando == 5:
                        p_code = input("Para venda, digite o código do produto: ")
                        quantity= int(input("Digite a quantidade: "))
                        self.product_sale(p_code, quantity)
                    elif comando == 6:
                        p_code = input("Para criar um novo produto, digite o código do produto")
                        name = input("Digite o nome: ")
                        category = input("Digite a categoria do produto: ")
                        min_stock = int(input("Digite a quantidade mínima de estoque: "))
                        regular_stock = int(input("Digite a quantidade normal de estoque: "))
                        max_stock = int(input("Digite a quantidade máxima de estoque: "))
                        location = input("Digite a locação: ")
                        self.add_product(p_code, name, category, min_stock, regular_stock, max_stock, location)
                    elif comando == 7:
                        order_id = input("Para aprovar uma solicitação de reposição, digite o id da solicitação: ")
                        self.approve_order(order_id)
                    elif comando == 8:
                        print("Segue as ordens de reposição em aberto: ")
                        self.get_not_approved()
                    else:
                        print('Digite comando válido.')
                else:
                    print(f"Usuário {self.user_code} não encontrado.")
            
# ☆☆Create Users, Products, Stock, Movements and Purchase TABLEs☆☆  
    # **The registered product details(statics) are stored in a table, and the stock details(dynamics) are stored in another**
    def create_tables(self):
        with sqlite3.connect(self.inventory_db) as conn:
            cursor = conn.cursor()
            # ---------Create Products Table---------
            create_user_table = """
            CREATE TABLE IF NOT EXISTS Users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(30) NOT NULL,
                privillege INTEGER NOT NULL DEFAULT 0
            );
            """
            cursor.execute(create_user_table)
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
            # ------------Create Movements Table----------
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
            # ---------Create Purchase Table---------
            create_purchase_table = """
            CREATE TABLE IF NOT EXISTS Purchase (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_code VARCHAR(7) UNIQUE,
                name VARCHAR(30) NOT NULL,
                purchase_order_quantity INTEGER NOT NULL,
                order_approved BOOLEAN DEFAULT FALSE,
                order_finished BOOLEAN DEFAULT FALSE,
                order_date DATETIME,
                FOREIGN KEY (product_code) REFERENCES Products (product_code) ON DELETE CASCADE
            );
            """
            cursor.execute(create_purchase_table)
            conn.commit()
            
# ☆☆Add product to Products and Stock TABLEs☆☆
    def add_product(self, product_code, name, category, min_stock, regular_stock, max_stock, location):
        # Check if the user is user or manager
        if self.privillege == 2 or self.privillege == 3:
            # Check if min stock is not negative, regular stock is bigger than min, and max is bigger than regular
            if min_stock >= 0 and regular_stock > min_stock and max_stock > regular_stock:
                # Separate product informations into product details(static), and stock details(dynamic)
                product_details = (product_code, name, category)
                stock_details = (product_code, name, min_stock, regular_stock, max_stock, location)
                # register into the tables
                self.add_to_tables(self.inventory_db, product_details, stock_details)
            else:
                print('O estoque mínimo deve ser 0 ou maior, o estoque regular maior que o mínimo e o estoque máximo maior que o regular.')
        else:
            print('Acesso negado.')
    
    @staticmethod
    def add_to_tables(inventory_db, product_details, stock_details):
        with sqlite3.connect(inventory_db) as conn:
            cursor = conn.cursor()
            try:
                # ----Add product static details to Products TABLE----   
                insert_product = "INSERT INTO Products (product_code, name, category) VALUES (?, ?, ?)"
                cursor.execute(insert_product, product_details)
                # -------Add stock dynamic datas to Stock TABLE------
                insert_to_stock = "INSERT INTO Stock (product_code, name, min_stock, regular_stock, max_stock, location) VALUES (?, ?, ?, ?, ?, ?)"
                
                cursor.execute(insert_to_stock, stock_details)
                conn.commit()
                print(f"Produto '{product_details[1]}' cadastrado e inserido ao estoque com sucesso.")
            
                # *Catch UNIQUE column violation*
            except sqlite3.IntegrityError:
                print(f"Erro: já existe produto com código {product_details[0]}")
                
# ☆☆☆ Purchase Order ☆☆☆   
    def purchase_order(self, product_code, name, quantity):
        # Check if the user is user or manager
        if self.privillege == 2 or self.privillege == 3:
            with sqlite3.connect(self.inventory_db) as conn:
                cursor = conn.cursor()
                purchase_details = (product_code, name, quantity, datetime.now())
                # Insert order into purchase table
                insert_order = "INSERT INTO Purchase (product_code, name, purchase_order_quantity, order_date) VALUES (?, ?, ?, ?)"
                cursor.execute(insert_order, purchase_details)
                conn.commit()
        else:
            print('Acesso negado.')                
# ☆☆☆ Approve Order ☆☆☆   
    def approve_order(self, order_id):
        # Check if the user is manager
        if self.privillege == 3:
            with sqlite3.connect(self.inventory_db) as conn:
                cursor = conn.cursor()
                # Update order to approved
                approve_product = ('TRUE', order_id)
                sql = "UPDATE Purchase SET order_approved = ? WHERE id = ?"
                cursor.execute(sql, approve_product)
                conn.commit()
        else:
            print('Acesso negado.')                
# ☆☆☆ Get not approved orders ☆☆☆   
    def get_not_approved(self):
        # Check if the user is user or manager
        if self.privillege == 2 or self.privillege == 3:
            with sqlite3.connect(self.inventory_db) as conn:
                query = "SELECT * FROM Purchase WHERE order_approved = FALSE"
                not_approved_df = pd.read_sql_query(query, conn)
                print('Produtos com ordem de compra não aprovadas')
                for row in not_approved_df.itertuples():
                    print(f'ID da solicitação: {row.id}, Código: {row.product_code}, Nome: {row.name}, Quantidade solicitada: {row.purchase_order_quantity}.')
        else:
            print('Acesso negado.')
# ☆☆☆ Product Sale (Reduce Stock) ☆☆☆       
    def product_sale(self, product_code, sale_qnty):
        # Check if the user is user or manager
        if self.privillege == 2 or self.privillege == 3:
                # Check if sale quantity is not negative or 0
            if not sale_qnty > 0:
                print('Insira quantidade de venda maior que 0.')
            else:
                with sqlite3.connect(self.inventory_db) as conn:
                    cursor = conn.cursor()
                        # Get current stock
                    result = self.get_real_stock(cursor, product_code)
                        # If found in stock
                    if result:
                        current_stock = result[0]
                        # Check if there is enough stock to make the sale
                        if current_stock >= sale_qnty:
                            # Reduce sold quantity from current stock
                            new_stock = current_stock - sale_qnty
                                # Prepare update and log infos
                            update_data = (new_stock, product_code)
                            log_data = (product_code, result[1], 'SALE', sale_qnty, current_stock, new_stock)
                                # Update Stock and create log in Movements
                            self.update_stock(cursor, update_data, log_data)
                            conn.commit()
                            print(f"Foram vendidos {sale_qnty} pcs, do produto {product_code}, estoque atual = {new_stock} ")
                        else:
                            print(f'Estoque do produto {product_code} insuficiente.')
                    else:
                        print(f"Produto {product_code} não encontrado no estoque")
        else:
            print('Acesso negado.')
                    
# ☆☆☆ Product Purchase (Add Stock)☆☆☆
    def product_entry(self, product_code, purchase_qnty, nf):
        # Check if it's stocker or manager
        if self.privillege == 1 or self.privillege == 3:
            # Validate nf
            valid_nf = self.validate_nf(nf)
            if valid_nf:
            # Check if purchase quantity is not negative or 0
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
                                # Add purchase quantity to current stock
                            new_stock = current_stock + purchase_qnty
                                # Prepare update and log infos
                            update_data = (new_stock, product_code)
                            log_data = (product_code, result[1], 'PURCHASE', purchase_qnty, current_stock, new_stock)
                                # Update Stock and create log in Movements
                            self.update_stock(cursor, update_data, log_data)
                            self.register_entry(cursor, product_code)
                            conn.commit()
                            
                            print(f"Foram adicionados {purchase_qnty} pcs, do produto {product_code} ao estoque, estoque atual = {new_stock} ")
                        else:
                            print(f"Produto {product_code} não encontrado no estoque")
            else:
                print('NF inválida')
        else:
            print("Somente estoquista tem a permissão para essa ação")

    @staticmethod
    def validate_nf(nf):
        if nf:
            return True
        else:
            return False
    @staticmethod
    def get_real_stock(cursor, product_code):
            # Get current stock and name, from product code
            cursor.execute("SELECT real_stock, name FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()
            return result

    @staticmethod
    def register_entry(cursor, product_code):
            # Update current purchase order to finished, from product code
        approve = ('TRUE', product_code)
        sql = "UPDATE Purchase SET order_finished = ? WHERE product_code = ?"
        cursor.execute(sql, approve)
    @staticmethod
    def update_stock(cursor, new_stock, logs):
        # Update current stock to new stock, from product code
        sql = "UPDATE Stock SET real_stock = ? WHERE product_code = ?"
        cursor.execute(sql, new_stock)

        # Register movement logs
        date = datetime.now()
        logs = logs + (date,)
        sql = "INSERT INTO Movements (product_code, name, movement_category, moved_quantity, before_change, after_change, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)" 
        cursor.execute(sql, logs)
        
# ☆☆☆Search for especific product code (simple report)☆☆☆
    def simple_report(self, product_code):
        # Check if the user is logged
        if self.privillege == 1 or self.privillege == 2 or self.privillege == 3:
            with sqlite3.connect(self.inventory_db) as conn:
                cursor = conn.cursor()
                # Get product informations from product code
                cursor.execute('SELECT product_code, name, real_stock, location FROM Stock WHERE product_code = ?', (product_code,))
                stock = cursor.fetchone()
            print(f'O produto "{stock[1]}: {stock[0]}", está na locação {stock[3]}, com {stock[2]} unidades em estoque.')
        else:
            print('Acesso negado')
# ☆☆☆☆Show a overall report☆☆☆☆
    def overall_report(self):
        # Check if it's user or manager
        if self.privillege == 2 or self.privillege == 3:
            with sqlite3.connect(self.inventory_db) as conn:
                # Get stock informations
                query = "SELECT * FROM Stock"
                oa_df = pd.read_sql_query(query, conn)
                # Get movement logs
                query = "SELECT * FROM Movements"
                moves_df = pd.read_sql_query(query, conn, parse_dates='timestamp')
            # Filter products from Stock that are with low/regular/over stock
            self.filter_df(oa_df)
            # Show last seven days movement logs
            self.last_moves(moves_df)
        else:
            print('Acesso negado.')

    @staticmethod
    def filter_df(df):
        filter = {'low_stock': [], 'regular_stock': [], 'over_stock': []}
        for row in df.itertuples():
                #  If current stock is higher than max stock, add to over stock list
            if row.real_stock > row.max_stock:
                filter['over_stock'].append(row.product_code)
                # If current stock is lower than min stock, add to low stock list
            elif row.real_stock <= row.min_stock:
                filter['low_stock'].append(row.product_code)
                # If current stock is not higher than max, nor lower than min, add to regular stock list
            else:
                filter['regular_stock'].append(row.product_code)

        print('Segue os produtos que estão em baixo estoque:')
        print(df[df['product_code'].isin(filter['low_stock'])])
        print('Segue os produtos que estão com excesso de estoque:')
        print(df[df['product_code'].isin(filter['over_stock'])])
        print('Segue os produtos que estão com estoque regular:')
        print(df[df['product_code'].isin(filter['regular_stock'])])
        
    @staticmethod
    def last_moves(df):
        print(f'Segue abaixo a movimentação dos últimos sete dias:')
        seven_days_ago = datetime.now() - relativedelta(days=7)
        filtered_df = df[(df['timestamp'] >= seven_days_ago)]
        # Print last moves using the created df
        for row in filtered_df.itertuples():
            print(f'Produto: {row.product_code}, Operação: {row.movement_category}, Quantidade: {row.moved_quantity}, Antes: {row.before_change}, Depois: {row.after_change}, Horário: {row.timestamp}')
            
# ☆☆☆☆Analyse stock condition☆☆☆☆
        # Look for items that has not been sold for the specified sales days
        # Look for items that had stock purchase more than the specified times, within specified months
    def analized_report(self, sales_days=30, purchases_months=2, purchase_count=4):
        # Check if it's manager
        if self.privillege == 3:
            # Get movements and products Tables
            moves_df, products_df = self.get_movements_and_products_df()
            # Get last sales and purchases for the specified periods, from the movements table
            last_sales, last_purchases = self.filter_last_sales_and_purchases(moves_df, sales_days, purchases_months)
            # Filter the products that were not sold in the specified period
            not_saled = self.not_saled_items(products_df, last_sales)
            # Filter the products that had more than n purchases in the specified period
            purchases_more_than_x = self. filter_by_purchases_count(last_purchases, purchase_count)
            
            print(f'Produtos que não são vendidos há mais de {sales_days} dias.')
            for row in not_saled.itertuples():
                print(f'{row.name} de código {row.product_code}')
            print(f'Nos últimos {purchases_months} meses:')
            for product_code, sales in purchases_more_than_x.items():
                print(f'O produto de código {product_code} teve pedido reposição de estoque {sales} vezes.')
        else:
            print('Acesso negado.')
        
        
    def get_movements_and_products_df(self):
        with sqlite3.connect(self.inventory_db) as conn:
            # Get movements table while converting dates into datetime, and turn into df
            query = "SELECT * FROM Movements"
            moves_df = pd.read_sql_query(query, conn, parse_dates='timestamp')
            # Get products table and turn into df
            query = "SELECT * FROM Products"
            products_df = pd.read_sql_query(query, conn)
        return moves_df, products_df
    
    def filter_last_sales_and_purchases(self, moves_df, sales_days, purchases_months):
        # Get the specified past date datetime (n months ago)
        x_months_ago = datetime.now() - relativedelta(months=purchases_months)
        # Get the specified past date datetime (n days ago)
        x_days_ago = datetime.now() - relativedelta(days=sales_days)
        # From the sales in moves df, filter the last n days rows
        last_x_days_sales = self.filter_by_time_category(moves_df, 'SALE', x_days_ago)
        # From the purchases in moves df, filter the last n months rows
        last_x_months_purchases = self.filter_by_time_category(moves_df, 'PURCHASE', x_months_ago)
        return last_x_days_sales, last_x_months_purchases
    @staticmethod
    def filter_by_time_category(df, operation, time):
        # From a df, return a new df that matches the specified movement category and the period is after the specified datetime
        df_filtered = df[(df['movement_category'] == operation) & (df['timestamp'] >= time)]
        return df_filtered
    @staticmethod
    def not_saled_items(products_df, last_sales):
        # From the products table, filter the products that are NOT in the provided last sales list
        return products_df[~products_df['product_code'].isin(last_sales['product_code'])]
    @staticmethod
    def filter_by_purchases_count(last_purchases, count):
        # From the last purchases log, create a new df that lists the count of how many times the same product code appears
        filter_df = last_purchases.groupby('product_code').size().reset_index(name='count')
        # From the df, filter products that had more than n counts, into a list
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
# manage.product_entry('CAM-001', 30)
# manage.product_entry('PAP-001', 600)
# manage.product_entry('COM-001', 4)
# manage.product_entry('MAQ-001', 1)
# manage.analized_report()
# manage.overall_report()

# manage.add_product('CAS-004', 'Caneca', 'Casa', 0, 10, 20, 'PAP01')
