import time
from concurrent.futures import ThreadPoolExecutor

import firebase_admin
import polars as pl
from firebase_admin import credentials, firestore
from fuzzywuzzy import fuzz
# from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from tqdm import tqdm

cred = credentials.ApplicationDefault()
firebase_admin.initialize_app(cred)
db = firestore.client()

# book_df = pl.read_csv('book_eng.csv')
book_df = None
current_stack = None

def check_db():
    books_ref = db.collection('books')
    docs = books_ref.limit(1).stream()
    first_book = next(docs, None)
    return first_book

def load_books_into_dataframe(batch_size=10000):
    global book_df
    if book_df is not None:
        return
    books_ref = db.collection('books')
    query = books_ref.limit(batch_size)

    docs = query.stream()
    all_books_data = []
    seen_books = set()

    while True:
        batch_data = []
        last_doc = None
        with tqdm(total=batch_size, desc="Loading batch", unit="books") as pbar:
            for doc in docs:
                book_data = doc.to_dict()
                book_id = book_data.get('Title')  # Assuming 'ID' is the unique identifier
                if book_id not in seen_books:
                    batch_data.append(book_data)
                    seen_books.add(book_id)
                last_doc = doc
                pbar.update(1)

        if not batch_data:
            break

        all_books_data.extend(batch_data)
        # print(f"So far: {pl.DataFrame(all_books_data).unique().shape[0]}")

        if len(batch_data) < round(batch_size / 2):
            break

        docs = books_ref.start_after(last_doc).limit(batch_size).stream()

    book_df = pl.DataFrame(all_books_data)
    # book_df.write_csv('book_eng.csv')
    print(f'Total Unique Books: {str(book_df.shape[0])}')
    print(f"Fields: {str(book_df.columns)}")

def search_booklist_batch(batch_df, keywords=None, people=None):
    df_filtered = batch_df

    if people:
        df_filtered = df_filtered.filter(pl.col("People").str.contains(people, literal=True))

    if keywords:
        df_filtered = df_filtered.with_columns((pl.col("Title") + " " + pl.col("Summary")).alias("Combined"))
        matched_indices = df_filtered.select("Combined").to_series().apply(
            lambda x: fuzz.token_set_ratio(keywords, x) > 70
        ).to_numpy()
        try:
            df_filtered = df_filtered.filter(pl.Series(matched_indices).to_list())
        except:
            return {}

    return df_filtered.to_dicts()


def search_books(keywords=None, people=None):
    global book_df, current_stack
    start = time.time()
    if book_df is None:
        load_books_into_dataframe()

    batch_size = 1000
    num_batches = len(book_df) // batch_size + 1

    batches = [book_df[i*batch_size:(i+1)*batch_size] for i in range(num_batches)]

    results = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(search_booklist_batch, batch, keywords, people)
            for batch in batches
        ]
        for future in futures:
            results.extend(future.result())
    current_stack = pl.DataFrame(results)
    end = time.time()
    totaltime = end - start
    print(f"Total time for book search: {totaltime} seconds")
    print(f'Books found: {len(results)}')
    for b in results:
        print(f'{b["Title"]} - {b["People"]}')
    return results

def search_books_query(keywords=None, people=None):
    books_ref = db.collection('books')
    query = books_ref

    if people and not keywords:
        query = query.where(filter=FieldFilter('People', '==', people))
    elif not people and keywords:
        query = query.where(filter=FieldFilter('Title', '>=', keywords)).where(
            filter=FieldFilter('Title', '<=', keywords + '\uf8ff')
        ).where(filter=FieldFilter('Summary', '>=', keywords)).where(
            filter=FieldFilter('Summary', '<=', keywords + '\uf8ff')
        )
    elif people and keywords:
        query = query.where(filter=FieldFilter('People', '==', people)).where(
            filter=FieldFilter('Title', '>=', keywords)
        ).where(filter=FieldFilter('Title', '<=', keywords + '\uf8ff')).where(
            filter=FieldFilter('Summary', '>=', keywords)
        ).where(filter=FieldFilter('Summary', '<=', keywords + '\uf8ff'))

    results = query.stream()

    books = []
    try:
        for doc in results:
            book = doc.to_dict()
            books.append(book)
    except Exception as e:
        print(e)
        return []
    return books

def insert_books(books):
    for book in books:
        book_dict = book.to_dict()
        db.collection('books').add(book_dict)
    print(f'Books uploaded to books db: {len(books)}')


def add_to_done_list(books):
    for book in books:
        book_dict = book.to_dict()
        db.collection('dones').add(book_dict)
    print(f'Books uploaded to firebase : {len(books)}')


def get_recommendations(keywords):
    pass
    # with sqlite3.connect(DATABASE) as conn:
    #     cursor = conn.cursor()
    #     cursor.execute('SELECT * FROM books WHERE Summary LIKE ? OR Title LIKE ?',
    #                    ('%' + keywords + '%', '%' + keywords + '%'))
    #     recommendations = cursor.fetchall()
    #     return recommendations
