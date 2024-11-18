from flask import Flask, jsonify, request
import sqlite3
import threading
import os
import itertools
from datetime import datetime  
import time
from flask_caching import Cache 

app = Flask(__name__)

app.config['CACHE_TYPE'] = 'simple'
cache = Cache(app)

thread_local = threading.local()

catalog_replica = [
    os.path.join(os.path.dirname(__file__), '../catalog/catalog-1/catalog1.db'),
    os.path.join(os.path.dirname(__file__), '../catalog/catalog-2/catalog2.db')
]
order_replica = [
    os.path.join(os.path.dirname(__file__), '../order/order-1/order1.db'),
    os.path.join(os.path.dirname(__file__), '../order/order-2/order2.db')
]

catalog_itertools = itertools.cycle(catalog_replica)
order_itertools = itertools.cycle(order_replica)

def catalog_db_connection():
    catalog_db_path = next(catalog_itertools)
    print(f"the catalog replica: {catalog_db_path}")
    if not hasattr(thread_local, 'catalog_conn'):
        thread_local.catalog_conn = sqlite3.connect(catalog_db_path)
        thread_local.catalog_conn.row_factory = sqlite3.Row
    return thread_local.catalog_conn

def order_db_connection():
    order_db_path = next(order_itertools)
    print(f"Using order replica: {order_db_path}")
    if not hasattr(thread_local, 'order_conn'):
        thread_local.order_conn = sqlite3.connect(order_db_path)
        thread_local.order_conn.row_factory = sqlite3.Row
    return thread_local.order_conn

@app.teardown_appcontext
def cleanup_databases(error):
    if hasattr(thread_local, 'catalog_conn'):
        thread_local.catalog_conn.close()
    if hasattr(thread_local, 'order_conn'):
        thread_local.order_conn.close()

@app.route('/', methods=['GET'])
def front():  
    return """  
    <html>  
        <head>  
            <title>front page</title>  
        </head>  
        <body style="background-color: black; color: white;">  
            <h1 style="text-align: center; margin: 400px 0px 0px 0px;">Welcome to the front-server page</h1>  
        </body>  
    </html>  
    """

@app.route('/product/<id>', methods=['GET'])
def fetch_product_by_id(id):
    start = time.time()

    if not id.isdigit():
        return jsonify({"message": "Product ID must be numeric"}), 400

    cached_response = cache.get(f'product_{id}')  
    if cached_response:  
        print("Using cache") 
        duringTime = time.time() - start  
        print("The time:", duringTime) 
        return jsonify(cached_response), 200  
    
    print("Using database")
    catalog_conn = catalog_db_connection()
    with catalog_conn:
        cursor = catalog_conn.cursor()
        cursor.execute("SELECT * FROM books WHERE id=?", (int(id),))
        product = cursor.fetchone()

        if product:
            productD =dict(product)
            cache.set(f'product_{id}', {"product": productD}, timeout=60)  
            duringTime = time.time() - start  
            print("The time:", duringTime) 
            return jsonify(productD), 200
        else:
            duringTime = time.time() - start  
            print("The time:", duringTime) 
            return jsonify({"message": "Product not found"}), 404

@app.route('/products/<string:topic>', methods=['GET'])
def fetch_products_by_topic(topic):
    start = time.time()

    products = cache.get(topic)  
    if products is not None:  
        print("using cache")  
        duringTime = time.time() - start  
        print("The time:", duringTime) 
        return jsonify(products), 200  # Return cached data  
        
    else:  
        print("using database")  

    catalog_conn = catalog_db_connection()
    with catalog_conn:
        cursor = catalog_conn.cursor()
        if topic.lower() == 'all':
            cursor.execute("SELECT * FROM books")
        else:
            cursor.execute("SELECT * FROM books WHERE topic LIKE ?", ('%' + topic + '%',))
        products = cursor.fetchall()
        if products:
            products_list = [dict(product) for product in products]    
            cache.set(topic, products_list, timeout=60)
            duringTime = time.time() - start  
            print("The time:", duringTime)
            return jsonify([dict(product) for product in products]), 200
        else:
            duringTime = time.time() - start  
            print("The time:", duringTime)
            return jsonify({"message": "No products found"}), 404

@app.route('/purchase/<int:id>/', methods=['PUT'])
def purchase_product(id):
    start = time.time()
    try:
        id = int(id)
    except ValueError:
        return jsonify({"message": "Book ID must be a numeric value"}), 400

    catalog1_conn = sqlite3.connect(catalog_replica[0])
    catalog1_conn.row_factory = sqlite3.Row

    catalog2_conn = sqlite3.connect(catalog_replica[1])
    catalog2_conn.row_factory = sqlite3.Row

    with catalog1_conn:
        cursor1 = catalog1_conn.cursor()
        cursor1.execute("SELECT * FROM books WHERE id=?", (id,))
        product1 = cursor1.fetchone()
        if product1:
            if product1['quantity'] > 0:
                updated_quantity = product1['quantity'] - 1
                cursor1.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, id))
            else:
                duringTime = time.time() - start  
                print("The time:", duringTime)
                return jsonify({"message": "Product out of stock", "success": False}), 400
        else:
            duringTime = time.time() - start  
            print("The time:", duringTime)
            return jsonify({"message": "Product not found", "success": False}), 404

    with catalog2_conn:
        cursor2 = catalog2_conn.cursor()
        cursor2.execute("SELECT * FROM books WHERE id=?", (id,))
        product2 = cursor2.fetchone()
        if product2:
            if product2['quantity'] > 0:
                updated_quantity = product2['quantity'] - 1
                cursor2.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, id))
                cache.delete(f'products_{id}')
                productD =dict(product2)
                cache.set(f'product_{id}', {"product": productD}, timeout=60) 
            else:
                duringTime = time.time() - start  
                print("The time:", duringTime)
                return jsonify({"message": "Product out of stock", "success": False}), 400
        else:
            duringTime = time.time() - start  
            print("The time:", duringTime)
            return jsonify({"message": "Product not found", "success": False}), 404

    order1_conn = sqlite3.connect(order_replica[0])
    order1_conn.row_factory = sqlite3.Row

    order2_conn = sqlite3.connect(order_replica[1])
    order2_conn.row_factory = sqlite3.Row

    
    with order1_conn:
        cur1 = order1_conn.cursor()
        cur1.execute("INSERT INTO orders (book_id, order_date, quantity) VALUES (?, ?, ?)", (id, datetime.now()  , 1))
        order1_conn.commit()

    with order2_conn:
        cur2 = order2_conn.cursor()
        cur2.execute("INSERT INTO orders (book_id, order_date, quantity) VALUES (?, ?, ?)", (id, datetime.now()  , 1))
        order2_conn.commit()  

    
    duringTime = time.time() - start  
    print("The time:", duringTime)
    return jsonify({"message": "Product purchased successfully", "success": True}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5000)
