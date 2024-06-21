#  CRUD with SQLite
import sqlite3

#Generate random strings https://docs.python.org/3/library/uuid.html
import uuid
import pandas as pd

# Creation of a new database with a randomly generated name
# (ie, every time the script is run it should create a new database with a random name)

#Create random data base name using uuid module, limits the name to 10 char.
dbFileName = (str(uuid.uuid4())[:10] +'.db')

#Connect to data base
conn = sqlite3.connect(dbFileName)

#create a Cursor object by calling the cursor() method of the Connection object.
cursor = conn.cursor()

# Creation of at least three new tables. Each table must have at least three columns.
def create_tables():
  sql_statements = [
    """CREATE TABLE IF NOT EXISTS customer_table (
        customer_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        business_name TEXT,
        email TEXT NOT NULL,
        birthday DATE,
        shipping_address TEXT,
        billing_address TEXT,
        register_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS product_table (
        product_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        price REAL NOT NULL,
        qty_available INT
    )""",
     """CREATE TABLE IF NOT EXISTS order_table (
        order_id INTEGER PRIMARY KEY,
        customer_id INT NOT NULL,
        description TEXT,
        price REAL NOT NULL,
        qty_available INT,
        FOREIGN KEY (customer_id) REFERENCES customer_table (customer_id)
    )""",
    """CREATE TABLE IF NOT EXISTS order_detail_table (
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity_ordered INTEGER,
        price_at_order REAL,
        subtotal REAL,
        FOREIGN KEY (order_id) REFERENCES order_table(order_id),
        FOREIGN KEY (product_id) REFERENCES product(product_id)
      )""",
]

  for sql_statement in sql_statements:
        cursor.execute(sql_statement)
        conn.commit()

# Call the create_tables function to create the tables
create_tables()

# Insertion of at least three rows of data for each table.
# Defining functions to add data to each table:

#Function to add new customers to table
def add_customers(conn):
    customer = [
        ('Nicole Goncalves', 'RRC', 'nicole@example.com', '1992-05-15', '123 Elm St', '456 Oak St'),
        ('Renan Cruz', 'Westland', 'renan@example.com', '1991-06-20', '789 Pine St', '101 Maple St'),
        ('Vinicius Leonardo', 'Element', 'vini@example.com', '2001-11-30', '111 Birch St', '222 Cedar St')
    ]
    cursor = conn.cursor()
    cursor.executemany(
        '''
        INSERT INTO customer_table (name, business_name, email, birthday, shipping_address, billing_address)
        VALUES(?, ?, ?, ?, ?, ?)
        ''', customer)
    conn.commit()

#Function to add new products to table
def add_products(conn):
    products = [
        ('Honey 250ml', 'Raw Honey 250ml Squeeze Bottle', 19.99, 100),
        ('Honey 500ml', 'Raw Honey 500ml Squeeze Bottle', 29.99, 50),
        ('Honey 1L', 'Raw Honey 1 liter Squeeze Bottle', 39.99, 25)
    ]
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO product_table (name, description, price, qty_available)
        VALUES (?, ?, ?, ?)
    ''', products)
    conn.commit()

#Function to add new orders to table
def add_orders(conn):
    #Retrieve customer_id's FK
    cursor = conn.cursor()
    cursor.execute('SELECT customer_id FROM customer_table')
    customers_ids = [row[0] for row in cursor.fetchall()]

     # Retrieve the current price of products from the product_table snapshot
    cursor.execute('SELECT product_id, price FROM product_table')
    product_prices = {row[0]: row[1] for row in cursor.fetchall()}

    orders = [
        (customers_ids[0], 'First order', product_prices[1], 3),
        (customers_ids[1], 'First order', product_prices[2], 2),
        (customers_ids[2], 'First order', product_prices[3], 1)
    ]

    cursor.executemany('''
        INSERT INTO order_table (customer_id, description, price, qty_available)
        VALUES (?, ?, ?, ?)
    ''', orders)
    conn.commit()

#Function to add new order details to table
def add_order_details(conn):
    # Retrieve order and product IDs (Foreign Keys)
    cursor = conn.cursor()
    cursor.execute('SELECT order_id FROM order_table')
    order_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute('SELECT product_id FROM product_table')
    product_ids = [row[0] for row in cursor.fetchall()]

    order_details = [
        (order_ids[0], product_ids[0], 2, 19.99, 39.98),
        (order_ids[0], product_ids[1], 1, 29.99, 29.99),
        (order_ids[1], product_ids[2], 1, 19.99, 19.99)
    ]

    cursor.executemany('''
        INSERT INTO order_detail_table (order_id, product_id, quantity_ordered, price_at_order, subtotal)
        VALUES (?, ?, ?, ?, ?)
    ''', order_details)
    conn.commit()

  # Call the functions to add data to the tables
add_customers(conn)
add_products(conn)
add_orders(conn)
add_order_details(conn)

# After the above steps, output the entire contents of the database to the console.
# Print the name of the database file created just for reference
print(f"This data base name is: {dbFileName}")

# Function to print table data using Pandas
def print_table_data(conn, table_name):
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    print(f"\n{table_name} contents:")
    print(df.to_string())  # Use to_string() to print the entire DataFrame

# Print the data in the tables - Loopthough tables and exceute print function
for table in ['customer_table', 'product_table', 'order_table', 'order_detail_table']:
    print_table_data(conn, table)

# Update at least one row of data in the database.
def update_customer(conn, customer_id, name, business_name, email, birthday, shipping_address, billing_address):
    sql = '''UPDATE customer_table
             SET name = ?,
             business_name = ?,
             email = ?,
             birthday = ?,
             shipping_address = ?,
             billing_address = ?
             WHERE customer_id = ?
          '''
    cursor = conn.cursor()
    cursor.execute(sql, (name, business_name, email, birthday, shipping_address, billing_address, customer_id))
    conn.commit()

#Execute function update_customer()
update_customer(conn, 2, 'Ana Nunes', 'HoneyBee', 'ana@honeybee.com', '1989-01-01', '390 Tyson Trail', '395 Tyson Trail')

# Select and output the changed data
def select_updated_customer(conn):
    df = pd.read_sql_query("SELECT * FROM customer_table WHERE customer_id = 2", conn)
    print("\nUpdated row on the customer_table data:")
    print(df.to_string())

select_updated_customer(conn)

#Print table containing updated row.
print("\nCustomer Table with updated row:")
print_table_data(conn, 'customer_table')

# Delete at least one row of data from the database.
def delete_row(conn, table_name, column_name, cell_value):
    cur = conn.cursor()
    delete_query = f'DELETE FROM {table_name} WHERE {column_name} = ?'
    cur.execute(delete_query, (cell_value,))
    conn.commit()

#Execute function to delete row:
delete_row(conn, 'customer_table', 'name', 'Ana Nunes')

# After deleting, output the entire table that the data was deleted from to the console.
print("\nUpdated Table with deleted row:")
print_table_data(conn, 'customer_table', )

# Print the data in the tables - Loopthough tables and exceute print function
# for table in ['customer_table', 'product_table', 'order_table', 'order_detail_table']:
# print_table_data(conn, table)

# Make sure you close the connection to the database when you are finished.
conn.close()