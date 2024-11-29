from datetime import datetime
from dateutil.relativedelta import relativedelta
import sqlite3
import pandas as pd
import logging
from typing import Optional

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

# Configuração de logging
logging.basicConfig(
    filename='inventory_system.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class BaseEntity:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Executa uma consulta no banco de dados com tratamento de erros."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                conn.commit()
                return cursor
        except sqlite3.Error as e:
            logging.error(f"Erro ao executar consulta: {e}")
            raise


class User(BaseEntity):
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS Users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name VARCHAR(30) NOT NULL,
            privilege INTEGER NOT NULL DEFAULT 0
        );
        """
        self.execute_query(query)
        logging.info("Tabela 'Users' criada com sucesso.")

    def add_user(self, name: str, privilege: int):
        query = "INSERT INTO Users (name, privilege) VALUES (?, ?)"
        self.execute_query(query, (name, privilege))
        logging.info(f"Usuário '{name}' adicionado com privilégio {privilege}.")


class Product(BaseEntity):
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS Products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code VARCHAR(7) UNIQUE,
            name VARCHAR(30) NOT NULL,
            category VARCHAR(20) NOT NULL
        );
        """
        self.execute_query(query)
        logging.info("Tabela 'Products' criada com sucesso.")

    def add_product(self, product_code: str, name: str, category: str):
        query = "INSERT INTO Products (product_code, name, category) VALUES (?, ?, ?)"
        self.execute_query(query, (product_code, name, category))
        logging.info(f"Produto '{name}' (código: {product_code}) adicionado.")
        print(f"Produto '{name}' (código: {product_code}) adicionado com sucesso.")

    def get_location(self, product_code: str):
        """Consulta a localização atual do produto a partir do número do produto."""
        query = "SELECT location FROM Stock WHERE product_code = ?"
        cursor = self.execute_query(query, (product_code,))
        result = cursor.fetchone()
        if result:
            return result[0]
        return "Localização não encontrada"


class Stock(BaseEntity):
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS Stock (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code VARCHAR(7) UNIQUE,
            name VARCHAR(30) NOT NULL,
            real_stock INTEGER NOT NULL DEFAULT 0,
            min_stock INTEGER NOT NULL,
            max_stock INTEGER NOT NULL,
            location VARCHAR(20) NOT NULL,
            FOREIGN KEY (product_code) REFERENCES Products (product_code) ON DELETE CASCADE
        );
        """
        self.execute_query(query)
        logging.info("Tabela 'Stock' criada com sucesso.")

    def add_stock(self, product_code: str, name: str, real_stock: int, min_stock: int, max_stock: int, location: str):
        query = """
        INSERT INTO Stock (product_code, name, real_stock, min_stock, max_stock, location)
        VALUES (?, ?, ?, ?, ?, ?)
        """
        self.execute_query(query, (product_code, name, real_stock, min_stock, max_stock, location))
        logging.info(f"Estoque do produto '{name}' (código: {product_code}) registrado.")
        print(f"Estoque do produto '{name}' (código: {product_code}) registrado com sucesso.")

    def update_stock(self, product_code: str, quantity: int):
        query = """
        UPDATE Stock
        SET real_stock = real_stock + ?
        WHERE product_code = ?
        """
        self.execute_query(query, (quantity, product_code))
        logging.info(f"Estoque do produto com código {product_code} atualizado.")


class Movement(BaseEntity):
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS Movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code VARCHAR(7),
            name VARCHAR(30) NOT NULL,
            movement_category VARCHAR(20),
            moved_quantity INTEGER,
            before_stock INTEGER,
            after_stock INTEGER,
            timestamp DATETIME,
            FOREIGN KEY (product_code) REFERENCES Stock (product_code) ON DELETE CASCADE
        );
        """
        self.execute_query(query)
        logging.info("Tabela 'Movements' criada com sucesso.")

    def add_movement(self, product_code: str, name: str, category: str, quantity: int, before_stock: int, after_stock: int):
        query = """
        INSERT INTO Movements (product_code, name, movement_category, moved_quantity, before_stock, after_stock, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        timestamp = datetime.now()
        self.execute_query(query, (product_code, name, category, quantity, before_stock, after_stock, timestamp))
        logging.info(f"Movimentação registrada: {quantity} unidades de '{name}' (código: {product_code}) movidas na categoria '{category}'.")
        print(f"Movimentação registrada: {quantity} unidades de '{name}' (código: {product_code}) movidas na categoria '{category}'.")


class PurchaseOrder(BaseEntity):
    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS PurchaseOrders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code VARCHAR(7),
            name VARCHAR(30) NOT NULL,
            purchase_quantity INTEGER NOT NULL,
            order_approved BOOLEAN DEFAULT FALSE,
            order_finished BOOLEAN DEFAULT FALSE,
            order_date DATETIME,
            FOREIGN KEY (product_code) REFERENCES Stock (product_code) ON DELETE CASCADE
        );
        """
        self.execute_query(query)
        logging.info("Tabela 'PurchaseOrders' criada com sucesso.")

    def create_order(self, product_code: str, name: str, quantity: int):
        query = """
        INSERT INTO PurchaseOrders (product_code, name, purchase_quantity, order_date)
        VALUES (?, ?, ?, ?)
        """
        order_date = datetime.now()
        self.execute_query(query, (product_code, name, quantity, order_date))
        logging.info(f"Ordem de compra criada para '{name}' (código: {product_code}) com quantidade {quantity}.")
        print(f"Ordem de compra criada para '{name}' (código: {product_code}) com quantidade {quantity}.")

    def approve_order(self, user_id, order_id):
        """Aprova uma ordem de compra apenas se o usuário tiver privilégio suficiente (gerente)."""
        if not self.check_privilege(user_id, 2):  # Privilegio 2 para gerente
            print("Erro: Usuário não tem privilégio para aprovar ordens de compra.")
            return

        query = "UPDATE PurchaseOrders SET order_approved = TRUE WHERE id = ?"
        self.execute_query(query, (order_id,))
        logging.info(f"Ordem de compra com ID {order_id} aprovada.")
        print(f"Ordem de compra com ID {order_id} aprovada.")

    def finalize_order(self, user_id, order_id: int):
        """Finaliza a ordem de compra e realiza a entrada de material no estoque após conferir a NF."""
        if not self.check_privilege(user_id, 1):  # Privilegio 1 para estoquista
            print("Erro: Usuário não tem privilégio suficiente para realizar esta ação.")
            return

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = """
            SELECT product_code, name, purchase_quantity, order_approved, order_finished
            FROM PurchaseOrders
            WHERE id = ?
            """
            order = cursor.execute(query, (order_id,)).fetchone()

            if not order:
                print(f"Erro: Ordem de compra com ID {order_id} não encontrada.")
                return

            product_code, name, quantity, approved, finished = order

            if not approved:
                print(f"Erro: A ordem de compra com ID {order_id} ainda não foi aprovada.")
                return

            if finished:
                print(f"Erro: A ordem de compra com ID {order_id} já foi finalizada.")
                return

            nf_code = 'nfcode'
            nf_verified = self.verify_nf(nf_code)
            if not nf_verified:
                print("Erro: Nota Fiscal não conferida corretamente.")
                return

            query_stock = "SELECT real_stock FROM Stock WHERE product_code = ?"
            stock_data = cursor.execute(query_stock, (product_code,)).fetchone()

            if stock_data:
                current_stock = stock_data[0]
                new_stock = current_stock + quantity

                # Atualiza o estoque
                update_stock_query = "UPDATE Stock SET real_stock = ? WHERE product_code = ?"
                cursor.execute(update_stock_query, (new_stock, product_code))

                # Registra a movimentação
                timestamp = datetime.now()
                log_data = (product_code, name, "PURCHASE", quantity, current_stock, new_stock, timestamp)
                insert_movement_query = """
                INSERT INTO Movements (product_code, name, movement_category, moved_quantity, before_stock, after_stock, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_movement_query, log_data)

                # Marca a ordem como finalizada
                update_order_query = "UPDATE PurchaseOrders SET order_finished = TRUE WHERE id = ?"
                cursor.execute(update_order_query, (order_id,))

                conn.commit()
                print(f"Ordem de compra com ID {order_id} finalizada. Estoque atualizado para {new_stock}.")

    def verify_nf(self, nf_code):
        """Simula a verificação da NF."""
        valid_nfs = ['NF123456', 'NF789101']  # NFs válidas
        return nf_code not in valid_nfs
    
    def check_privilege(self, user_id, required_privilege):
        """Verifica se o usuário tem privilégio suficiente para realizar uma ação."""
        with sqlite3.connect(self.db_path) as conn:
            query = "SELECT privilege FROM Users WHERE id = ?"
            cursor = conn.cursor()
            cursor.execute(query, (user_id,))
            user_privilege = cursor.fetchone()

            if user_privilege is None:
                logging.warning(f"Usuário com ID {user_id} não encontrado.")
                return False

            if user_privilege[0] >= required_privilege:
                return True
            else:
                logging.warning(f"Usuário com ID {user_id} não tem privilégio suficiente.")
                return False
# Expansão da classe InventoryManager
class InventoryManagerRefactored:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.user = User(db_path)
        self.product = Product(db_path)
        self.stock = Stock(db_path)
        self.movement = Movement(db_path)
        self.purchase_order = PurchaseOrder(db_path)

    def setup(self):
        """Configura todas as tabelas no banco de dados."""
        self.user.create_table()
        self.product.create_table()
        self.stock.create_table()
        self.movement.create_table()
        self.purchase_order.create_table()

    def register_product_movement(self, product_code: str, quantity: int, category: str):
        """Registra uma movimentação de produto (ex.: venda, entrada)."""
        stock_data = self.stock.execute_query(
            "SELECT real_stock, name FROM Stock WHERE product_code = ?", (product_code,)
        ).fetchone()

        if stock_data:
            current_stock, name = stock_data
            new_stock = current_stock + quantity if category == "ENTRY" else current_stock - quantity

            if new_stock < 0:
                logging.warning(f"Estoque insuficiente para realizar a movimentação do produto {product_code}.")
                print(f"Erro: Estoque insuficiente para o produto '{name}' (código: {product_code}).")
                return

            # Atualiza o estoque
            self.stock.execute_query(
                "UPDATE Stock SET real_stock = ? WHERE product_code = ?", (new_stock, product_code)
            )

            # Registra a movimentação
            self.movement.add_movement(
                product_code, name, category, quantity, current_stock, new_stock
            )
            print(f"Movimentação de {quantity} unidades do produto '{name}' registrada com sucesso. Estoque atual: {new_stock}.")
        else:
            print(f"Erro: Produto com código '{product_code}' não encontrado.")
            
    def generate_weekly_report(self):
        """Gera um relatório semanal mostrando o status crítico do estoque e movimentações recentes.""" 
        print("=== Relatório Semanal ===")
        with sqlite3.connect(self.db_path) as conn:
            # Relatório de estoque crítico
            query_stock = """
            SELECT product_code, name, real_stock, min_stock, max_stock
            FROM Stock
            """
            stock_df = pd.read_sql_query(query_stock, conn)

            low_stock = stock_df[stock_df['real_stock'] <= stock_df['min_stock']]
            over_stock = stock_df[stock_df['real_stock'] > stock_df['max_stock']]

            print("\nProdutos com estoque crítico (abaixo do mínimo):")
            print(low_stock if not low_stock.empty else "Nenhum produto com estoque crítico.")

            print("\nProdutos com excesso de estoque:")
            print(over_stock if not over_stock.empty else "Nenhum produto com excesso de estoque.")

            # Movimentações dos últimos 7 dias
            query_movements = """
            SELECT product_code, name, movement_category, moved_quantity, before_stock, after_stock, timestamp
            FROM Movements
            WHERE timestamp >= ?
            """
            seven_days_ago = datetime.now() - relativedelta(days=7)
            movements_df = pd.read_sql_query(query_movements, conn, params=(seven_days_ago,))

            print("\nMovimentações nos últimos 7 dias:")
            print(movements_df if not movements_df.empty else "Nenhuma movimentação registrada nos últimos 7 dias.")

    def perform_detailed_analysis(self, sales_days=30, purchase_months=2, purchase_count=4):
        """
        Realiza uma análise detalhada do estoque:
        1. Identifica produtos que não tiveram vendas nos últimos 'sales_days'.
        2. Identifica produtos com excesso de reposições nos últimos 'purchase_months'.
        """
        print("=== Análise Detalhada ===")
        with sqlite3.connect(self.db_path) as conn:
            # Movimentações recentes
            query_movements = """
            SELECT product_code, movement_category, timestamp
            FROM Movements
            """
            movements_df = pd.read_sql_query(query_movements, conn, parse_dates=['timestamp'])

            # Converte as datas para o formato datetime, tratando valores inválidos
            movements_df['timestamp'] = pd.to_datetime(movements_df['timestamp'], errors='coerce')

            # Produtos cadastrados
            query_products = "SELECT product_code, name FROM Products"
            products_df = pd.read_sql_query(query_products, conn)

            # Identificar produtos sem vendas nos últimos 'sales_days'
            cutoff_date_sales = datetime.now() - relativedelta(days=sales_days)
            recent_sales = movements_df[
                (movements_df['movement_category'] == "SALE") & (movements_df['timestamp'] >= cutoff_date_sales)
            ]
            unsold_products = products_df[~products_df['product_code'].isin(recent_sales['product_code'])]

            print(f"\nProdutos sem vendas nos últimos {sales_days} dias:")
            print(unsold_products if not unsold_products.empty else "Todos os produtos tiveram vendas recentes.")

            # Identificar produtos com excesso de reposições nos últimos 'purchase_months'
            cutoff_date_purchases = datetime.now() - relativedelta(months=purchase_months)
            recent_purchases = movements_df[
                (movements_df['movement_category'] == "PURCHASE") & (movements_df['timestamp'] >= cutoff_date_purchases)
            ]

            purchase_counts = recent_purchases['product_code'].value_counts()
            frequent_purchases = purchase_counts[purchase_counts >= purchase_count]

            print(f"\nProdutos com mais de {purchase_count} reposições nos últimos {purchase_months} meses:")
            print(frequent_purchases if not frequent_purchases.empty else "Nenhum produto excedeu o limite de reposições.")

# Uso expandido com relatórios e análises
db_path = "intei.db"
manager = InventoryManagerRefactored(db_path)

# Configurar tabelas e gerar relatórios/análises
manager.setup()
manager.generate_weekly_report()
manager.perform_detailed_analysis()

# manager.user.add_user("Admin", 2)

# manager.product.add_product('CAM-001', 'Camiseta', 'Vestuário')
# manager.product.add_product('CAM-002', 'Camiseta', 'Vestuário')
# manager.product.add_product('MAQ-001', 'Lava-louças', 'Eletrodomésticos')    
# manager.product.add_product('COM-001', 'Processador', 'Eletrônicos')
# manager.product.add_product('PAP-001', 'Caneta', 'Papelaria')
# manager.product.add_product('CAS-001', 'Caneca', 'Casa')
# manager.stock.add_stock('CAM-001', 'Camiseta', 30, 10, 50, 'VEST01')
# manager.stock.add_stock('CAM-002', 'Camiseta', 30, 10, 50, 'VEST01')
# manager.stock.add_stock('MAQ-001', 'Lava-louças', 4, 2, 10, 'ELET03')    
# manager.stock.add_stock('COM-001', 'Processador', 10, 5, 20, 'COMP01')
# manager.stock.add_stock('PAP-001', 'Caneta', 200, 100, 500, 'PAP01')
# manager.purchase_order.create_order('CAS-001', 'Caneca', 10) 
# manager.purchase_order.create_order('CAM-002', 'Camiseta', 30)
# manager.purchase_order.create_order('MAQ-001', 'Lava-louças', 3)    
# manager.purchase_order.create_order('COM-001', 'Processador', 10)
# manager.purchase_order.create_order('PAP-001', 'Caneta', 200)
# manager.purchase_order.create_order('CAS-001', 'Caneca', 10) 
# manager.purchase_order.create_order('CAM-001', 'Camiseta', 30)
# manager.purchase_order.create_order('CAM-001', 'Camiseta', 30)
# manager.purchase_order.create_order('CAM-001', 'Camiseta', 30)
# manager.purchase_order.create_order('CAM-001', 'Camiseta', 30)
# manager.purchase_order.create_order('CAM-001', 'Camiseta', 30)

# i = 0
# while i < 50:
#     manager.purchase_order.finalize_order(1, i)
#     i+=1

# manager.register_product_movement("COM-001", 20, "SALE")  # Venda de 5 unidades