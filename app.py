import time

from flask import Flask, request, jsonify
import ops
from datetime import datetime
from flask_cors import CORS
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)
CORS(app)

@app.route('/', methods=['GET'])
def check_app():
    return 'RecomAPI Up: 1.2'

@app.route('/totalbooks', methods=['GET'])
def check_total_books():
    total =  ops.count_total_books()
    return str(total)

@app.route('/check', methods=['GET'])
def check_books():
    try:
        f = ops.check_db()
        if f:
            book_data = f.to_dict()
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
    keywords = request.args.get('keywords', '')
    people = request.args.get('people', '')
    if keywords == '' and people == '':
        return jsonify({'message': "Enter parameters"})
    #Books
    if request.args.get('type') == 'books':
        results = ops.search_books(keywords, people)
        return jsonify(results), 200
    #Ppaers
    if request.args.get('type') == 'papers':
        results = ops.search_papers(keywords, 20)
        return jsonify(results), 200

    return jsonify({"message": "Invalid type"}), 400




@app.route('/savebooks', methods=['POST'])
def save_books():
    try:
        books_data = request.json.get('books', [])
        if not books_data:
            return jsonify({'message': "No books data provided"}), 400
        ops.save_books_list(books_data)
        return jsonify({"message": f"Books saved successfully: {len(books_data)}"}), 201
    except Exception as e:
        return jsonify({"message": f"Error saving books: {str(e)}"}), 500

@app.route('/reload', methods=['GET'])
def reload_books():
    retnum = ops.reload_books()
    return jsonify({'message': f'Loaded Latest Books: {str(retnum)}', 'count': retnum})

@app.route('/delete', methods=['POST'])
def delete_book():
    book = request.json.get('book')
    if not book:
        return jsonify({"message": "Invalid data"}), 400
    try:
        ops.delete_book(book)

        return jsonify({"message": f"Book deleted successfully: {book.get('Title')}"}), 200
    except Exception as e:
        return jsonify({"message": f"An error occurred: {e}"}), 500

def local_tester():
    res = ops.search_books(people="Yuval Noah Harari")
    # res = ops.search_books(keywords="machine learning and introductory level data science")
    res = ops.search_books(keywords="future of humanity robotics technology")



if __name__ == '__main__':
    ops.load_books_into_dataframe()
    app.run(host="0.0.0.0", port="8091")
    # local_tester()