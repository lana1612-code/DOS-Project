from flask import Flask, jsonify, request
import sqlite3
import threading
import os
from datetime import datetime  

app = Flask(__name__)


thread_data = threading.local()

pathOrder2DB = os.path.join(os.path.dirname(__file__), 'order2.db')
pathOrder1DB = os.path.join(os.path.dirname(__file__), '../order-1/order1.db')

pathCatalog1DB = os.path.join(os.path.dirname(__file__), '../../catalog/catalog-1/catalog1.db')
pathCatalog2DB = os.path.join(os.path.dirname(__file__), '../../catalog/catalog-2/catalog2.db')

def openOrder1DB():
    if not hasattr(thread_data, 'order1_db_connection'):
        thread_data.order1_db_connection = sqlite3.connect(pathOrder1DB)
        thread_data.order1_db_connection.row_factory = sqlite3.Row
    return thread_data.order1_db_connection

def openOrder2DB():
    if not hasattr(thread_data, 'order2_db_connection'):
        thread_data.order2_db_connection = sqlite3.connect(pathOrder2DB)
        thread_data.order2_db_connection.row_factory = sqlite3.Row
    return thread_data.order2_db_connection

def catalog1_db_connection():
    if not hasattr(thread_data, 'catalog1_connection'):
        thread_data.catalog1_connection = sqlite3.connect(pathCatalog1DB)
        thread_data.catalog1_connection.row_factory = sqlite3.Row  
    return thread_data.catalog1_connection

def catalog2_db_connection():
    if not hasattr(thread_data, 'catalog2_connection'):
        thread_data.catalog2_connection = sqlite3.connect(pathCatalog2DB)
        thread_data.catalog2_connection.row_factory = sqlite3.Row  
    return thread_data.catalog2_connection

@app.teardown_appcontext
def close_connections(error):
    if hasattr(thread_data, 'order1_db_connection'):
        thread_data.order1_db_connection.close()
    if hasattr(thread_data, 'order2_db_connection'):
        thread_data.order2_db_connection.close()
    if hasattr(thread_data, 'catalog1_connection'):
        thread_data.catalog1_connection.close()
    if hasattr(thread_data, 'catalog2_connection'):
        thread_data.catalog2_connection.close()

@app.route('/', methods=['GET'])
def catalog2():  
    return """  
    <html>  
        <head>  
            <title>order page</title>  
        </head>  
        <body style="background-color: black; color: white;">  
            <h1 style="text-align: center; margin: 400px 0px 0px 0px;" >Welcome to Order Page</h1>  
        </body>  
    </html>  
    """

#! work: done
@app.route('/purchase/<book_id>/', methods=['PUT'])
def process_purchase(book_id):
    
    try:
        book_id = int(book_id)
    except ValueError:
        return jsonify({"message": "Book ID must be a numeric value"}), 400

    order1_connection = openOrder1DB()
    order2_connection = openOrder2DB()
    catalog1_connection = catalog1_db_connection()
    catalog2_connection = catalog2_db_connection()

    with catalog1_connection:
        cursor1 = catalog1_connection.cursor()
        cursor1.execute("SELECT * FROM books WHERE id=?", (book_id,))
        book_info1 = cursor1.fetchone()
        
        if book_info1:
            if book_info1['quantity'] > 0:
                updated_quantity = book_info1['quantity'] - 1
                cursor1.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, book_id))
            else:
                return jsonify({"message": "Book out of stock", "status": False}), 400
        else:
            return jsonify({"message": "Book not found", "status": False}), 404
    
    with catalog2_connection:
        cursor2 = catalog2_connection.cursor()
        cursor2.execute("SELECT * FROM books WHERE id=?", (book_id,))
        book_info2 = cursor2.fetchone()
        
        if book_info2:
            if book_info2['quantity'] > 0:
                updated_quantity = book_info2['quantity'] - 1
                cursor2.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, book_id))
            else:
                return jsonify({"message": "Book out of stock", "status": False}), 400
        else:
            return jsonify({"message": "Book not found", "status": False}), 404
    
    
    with order1_connection:
        order1_cursor = order1_connection.cursor()
        order1_cursor.execute("INSERT INTO orders (book_id, order_date, quantity) VALUES (?, ?, ?)", (book_id, datetime.now()  , 1))
    
    with order2_connection:
        order2_cursor = order2_connection.cursor()
        order2_cursor.execute("INSERT INTO orders (book_id, order_date, quantity) VALUES (?, ?, ?)", (book_id, datetime.now()  , 1))
    
    return jsonify({"message": "Book successfully purchased", "status": True}), 200

if __name__ == '__main__':
    app.run(debug=True, port=7001)