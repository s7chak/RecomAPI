import time

from flask import Flask, request, jsonify
import ops
from datetime import datetime
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)
CORS(app)

# cred = credentials.ApplicationDefault()
# firebase_admin.initialize_app(cred)
# db = firestore.client()
@app.route('/checkapp', methods=['GET'])
def check_app():
    return jsonify('Up')
@app.route('/checkdb', methods=['GET'])
def check_db():
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    return jsonify('DB')

@app.route('/check', methods=['GET'])
def check_books():
    try:
        books_ref = db.collection('books')
        docs = books_ref.limit(1).stream()
        first_book = next(docs, None)
        if first_book:
            book_data = first_book.to_dict()
            return jsonify(book_data), 200
        else:
            return jsonify({"message": "No books found"}), 404
    except Exception as e:
        return jsonify({"message": str(e)}), 500


@app.route('/collect', methods=['GET'])
def collect_books():
    if request.args.get('type') == 'books':
        # Simulating book data collection (replace this with actual API data fetch)
        new_books = [
            {'Title': 'Book 1', 'Summary': 'A dark comedy book.', 'Date': '2023-01-01'},
            {'Title': 'Book 2', 'Summary': 'A thrilling mystery book.', 'Date': '2023-02-01'}
        ]
        for book in new_books:
            ops.add_book(book['Title'], book['Summary'], book['Date'])
        return jsonify({"message": "Books added successfully"}), 201
    return jsonify({"message": "Invalid type"}), 400


@app.route('/taste', methods=['POST'])
def taste_list():
    if request.args.get('type') == 'books':
        book_titles = request.json.get('list', [])
        ops.add_to_taste_list(book_titles)
        return jsonify({"message": "Taste list updated successfully"}), 201
    return jsonify({"message": "Invalid type"}), 400


@app.route('/search', methods=['GET'])
def recommend_books():
    if request.args.get('type') == 'books':
        keywords = request.args.get('keywords', '')
        people = request.args.get('people', '')
        if keywords=='' and people=='':
            return jsonify({'message': "Enter parameters"})
        results = ops.search_books(keywords, people)
        return jsonify(results), 200
    return jsonify({"message": "Invalid type"}), 400


def local_tester():
    res = ops.search_books(people="Yuval Noah Harari")
    # res = ops.search_books(keywords="machine learning and introductory level data science")
    res = ops.search_books(keywords="future of humanity robotics technology")



if __name__ == '__main__':
    # ops.load_books_into_dataframe()
    app.run(host="0.0.0.0")
    # local_tester()