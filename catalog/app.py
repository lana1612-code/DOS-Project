from flask import Flask, jsonify, request
import sqlite3
import threading

app = Flask(__name__)


thread_data = threading.local()


def acquire_db_connection():
    
    if not hasattr(thread_data, 'database_connection'):
        thread_data.database_connection = sqlite3.connect('catalog.db')
        thread_data.database_connection.row_factory = sqlite3.Row  
    return thread_data.database_connection


@app.teardown_appcontext
def release_db_connection(exception):
    if hasattr(thread_data, 'database_connection'):
        thread_data.database_connection.close()


@app.route('/retrieve/item/<id>', methods=['GET'])
def get_book_by_id(id):
   
    if not id.isdigit():
        return jsonify({"error": "Book ID must be numeric"}), 400

    database = acquire_db_connection()
    cursor = database.cursor()
    cursor.execute("SELECT * FROM books WHERE id=?", (id,))
    book = cursor.fetchone()
    cursor.close()
    if book:
        return jsonify(dict(book)), 200
    else:
        return jsonify({"error": "Book not found"}), 404

@app.route('/retrieve/topic/<topic>', methods=['GET'])
def get_books_by_topic(topic):
    database = acquire_db_connection()
    cursor = database.cursor()
    cursor.execute("SELECT * FROM books WHERE topic=?", (topic,))
    books = cursor.fetchall()
    cursor.close()
    return jsonify([dict(book) for book in books])


@app.route('/modify/<int:id>', methods=['PUT'])
def modify_book(id):
   
    updated_price = request.json.get('price')
    updated_quantity = request.json.get('quantity')

    
    if updated_price is None and updated_quantity is None:
        return jsonify({"error": "No update data provided"}), 400

    database = acquire_db_connection()
    cursor = database.cursor()

    
    if updated_price is not None:
        cursor.execute("UPDATE books SET price=? WHERE id=?", (updated_price, id))
    if updated_quantity is not None:
        cursor.execute("UPDATE books SET quantity=? WHERE id=?", (updated_quantity, id))

    database.commit()
    cursor.close()

   
    cursor = database.cursor()
    cursor.execute("SELECT * FROM books WHERE id=?", (id,))
    book = cursor.fetchone()
    cursor.close()

    if book:
        return jsonify(dict(book)), 200
    else:
        return jsonify({"error": "Book not found"}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5001)