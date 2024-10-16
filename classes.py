import sqlite3


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
                category VARCHAR(20) NOT NULL,
                min_stock INTEGER NOT NULL,
                max_stock INTEGER NOT NULL,
                regular_stock INTEGER NOT NULL
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
                after_change VARCHAR(20)
            )
            """
            cursor.execute(create_movements_table)

            conn.commit()
    def add_to_product_database(self):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            product_details = (self.product_code, self.name, self.category, self.min_stock, self.max_stock, self.regular_stock)
            
            # ---------Add to database---------
            try:               
                insert_product = "INSERT INTO Products (product_code, name, category, min_stock, max_stock, regular_stock) VALUES (?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_product, product_details)
                conn.commit()
                print(f"Produto '{self.name}' cadastrado com sucesso.")
            
            # *Catch UNIQUE column violation*
            except sqlite3.IntegrityError:
                print(f"Erro: já existe produto com código {self.product_code}")
                
    def add_to_stock_database(self):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            stock_details = (self.product_code, self.name, self.location)
            
            # ---------Add to database---------
            try:
                insert_to_stock = "INSERT INTO Stock (product_code, name, location) VALUES (?, ?, ?)"
        
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
        self.log_movements = "INSERT INTO Movements (product_code, movement_category, moved_quantity, before_change, after_change) VALUES (?, ?, ?, ?, ?)"        
        self.update_stock = "UPDATE Stock SET real_stock = ? WHERE product_code = ?"
#         Tabela Produto:

# ID do Produto (chave primária)
# Nome
# Categoria
# Localização
# Estoque Mínimo
# Estoque Máximo
# Estoque Normal
# Outros atributos fixos (descrição, fornecedor, etc.)

# Tabela Estoque:
# ID do Produto (chave estrangeira que referencia Produto)
# Estoque Real
# Última atualização
# Local de armazenagem (se for relevante ter mais de um local de estoque)

# Tabela MovimentacaoEstoque:
# ID da Movimentação (chave primária)
# ID do Produto (chave estrangeira que referencia Produto)
# Quantidade Movimentada
# Tipo de Movimentação (entrada ou saída)
# Data da Movimentação
# Motivo (compra, venda, ajuste, etc.)
        pass
    
    def product_sale(self, product_code, sale_qnty):
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            
            # Get actual stock
            cursor.execute("SELECT real_stock FROM Stock WHERE product_code = ?", (product_code,))
            result = cursor.fetchone()

            if result:
                current_stock = result[0]
                # Check if there is enough stock to make the sale
                if current_stock >= sale_qnty:
                    new_stock = current_stock - sale_qnty
                    #update stock table
                    cursor.execute(self.update_stock, (new_stock, product_code))
                    # insert into movement logs (refatorate? instance the execute code?)
                    cursor.execute(self.log_movements, (product_code, 'SALE', sale_qnty, current_stock, new_stock))
                    conn.commit()    
                    #talvez consultar novamente a table?
                    print(f"Foram vendidos {sale_qnty} pcs, do produto {product_code}, estoque atual = {new_stock} ")
                else:
                    print(f"Estoque do produto {product_code} insuficiente")
            # If not found in stock
            else:
                print(f"Produto {product_code} não encontrado no estoque")
                
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
                cursor.execute(self.log_movements, (product_code, 'PURCHASE', purchase_qnty, current_stock, new_stock))
                conn.commit()    
                print(f"Foram adicionados {purchase_qnty} pcs, do produto {product_code} ao estoque, estoque atual = {new_stock} ")
            else:
                print(f"Produto {product_code} não encontrado no estoque")

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
                    # log into movements (fix moved_qnty)
                    cursor.execute(self.log_movements, (product_code, 'RE-LOCATION', 0, current_location, new_location))
                    conn.commit()    
                    print(f"Produto {product_code} foi movimentado de {current_location} para {new_location}")
                else:
                    print(f"Produto {product_code} já se encontra na locação {new_location}")
            else:
                print(f"Produto {product_code} não encontrado no estoque")
    
    @staticmethod
    def report():
        with sqlite3.connect('inventory.db') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM Movements')
            moves = cursor.fetchall()
            for move in moves:
                print(move)
            
    
def teste_report(product_code):
    conn = sqlite3.connect('inventory.db')
    cursor = conn.cursor()
    cursor.execute('SELECT id, product_code, name FROM Products WHERE product_code = ?', (product_code,))
    product = cursor.fetchall()
    print(product)
    
def simple_report(product_code):
    conn = sqlite3.connect('inventory.db')
    cursor = conn.cursor()
    cursor.execute('SELECT id, product_code, name, real_stock, location FROM Stock WHERE product_code = ?', (product_code,))
    stock = cursor.fetchall()
    print(stock)

def detailed_report():
    pass
    
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
# teste_report('CAM-001')
# simple_report('CAM-001')
# ----------teste de movimento----------
movimento = Moves()
movimento.stock_incrementing('CAM-001', 50)
movimento.product_sale('CAM-001', 10)
movimento.location_movement('MAQ-001', 'NEW-LOQ')
movimento.report()
