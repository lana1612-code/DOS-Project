from flask import Flask, jsonify, request
import sqlite3
import threading
import os

app = Flask(__name__)

thread_local = threading.local()

pathOrderDB = os.path.join(os.path.dirname(__file__), 'order.db')
pathCatalogDB = os.path.join(os.path.dirname(__file__), 'catalog.db')

def initCatalogDb():
    if not hasattr(thread_local, 'catalog_conn'):
        thread_local.catalog_conn = sqlite3.connect(pathCatalogDB)
        thread_local.catalog_conn.row_factory = sqlite3.Row
    return thread_local.catalog_conn

def initOrderDb():
    if not hasattr(thread_local, 'order_conn'):
        thread_local.order_conn = sqlite3.connect(pathOrderDB)
        thread_local.order_conn.row_factory = sqlite3.Row
    return thread_local.order_conn


@app.teardown_appcontext
def cleanup_databases(error):
    if hasattr(thread_local, 'catalog_conn'):
        thread_local.catalog_conn.close()
    if hasattr(thread_local, 'order_conn'):
        thread_local.order_conn.close()

@app.route('/product/<id>', methods=['GET'])
def fetch_product_by_id(id):
    if not id.isdigit():
        return jsonify({"message": "Product ID must be numeric"}), 400

    catalog_conn = initCatalogDb()
    cursor = catalog_conn.cursor()
    cursor.execute("SELECT * FROM books WHERE id=?", (id),)
    product = cursor.fetchone()
    cursor.close()
    if product:
        return jsonify(dict(product)), 200
    else:
        return jsonify({"message": "Product not found"}), 404


@app.route('/products/<string:topic>', methods=['GET'])
def fetch_products_by_topic(topic):
    catalog_conn = initCatalogDb()
    with catalog_conn:
        cursor = catalog_conn.cursor()
        if topic.lower() == 'all':
            cursor.execute("SELECT * FROM books")
        else:
            cursor.execute("SELECT * FROM books WHERE topic LIKE ?", ('%' + topic + '%',))
        products = cursor.fetchall()
        if products:
            return jsonify([dict(product) for product in products]), 200
        else:
            return jsonify({"message": "No products found"}), 404

@app.route('/purchase/<int:id>/', methods=['PUT'])
def purchase_product(id):
    catalog_conn = initCatalogDb()
    order_conn = initOrderDb()

    with catalog_conn:
        cursor = catalog_conn.cursor()
        cursor.execute("SELECT * FROM books WHERE id=?", (id,))
        product = cursor.fetchone()
        if product:
            if product['quantity'] > 0:
                updated_quantity = product['quantity'] - 1
                cursor.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, id))
            else:
                return jsonify({"message": "Product out of stock", "success": False}), 400
        else:
            return jsonify({"message": "Product not found", "success": False}), 404

    with order_conn:
        cursor = order_conn.cursor()
        cursor.execute("INSERT INTO orders (book_id) VALUES (?)", (id,))

    return jsonify({"message": "Product purchased successfully", "success": True}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5000)