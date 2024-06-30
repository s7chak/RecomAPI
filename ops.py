import firebase_admin
import polars as pl
import requests
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from firebase_admin import credentials, firestore
from fuzzywuzzy import fuzz
# from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from tqdm import tqdm
from objects.Object import Paper
#prod
cred = credentials.ApplicationDefault()
book_df = None

#local
# cred = credentials.Certificate('/Users/subhayuchakravarty/.ssh/keys/recoms-firebase-adminsdk-uy0fv-11c9f64cce.json')
# book_df = pl.read_csv('book_eng.csv')
paper_list = None

firebase_admin.initialize_app(cred)
db = firestore.client()
book_table = 'bookshelf'
def check_db():
    books_ref = db.collection(book_table)
    docs = books_ref.limit(1).stream()
    first_book = next(docs, None)
    return first_book

def count_total_books():
    global book_df, current_stack
    if book_df is None:
        load_books_into_dataframe()
    total = book_df.shape[0]
    return total

def load_books_into_dataframe(batch_size=10000):
    global book_df
    if book_df is not None:
        return
    books_ref = db.collection(book_table)
    query = books_ref.limit(batch_size)

    docs = query.stream()
    all_books_data = []
    seen_books = set()

    while True:
        batch_data = []
        last_doc = None
        # with tqdm(total=batch_size, desc="Loading batch", unit="books") as pbar:
        for doc in docs:
            book_data = doc.to_dict()
            book_id = book_data.get('Title')  # Assuming 'ID' is the unique identifier
            if book_id not in seen_books:
                batch_data.append(book_data)
                seen_books.add(book_id)
            last_doc = doc
                # pbar.update(1)

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


def get_paper_abstract(paper):
    try:
        response = requests.get(paper.link)
        response.raise_for_status()  # Check if the request was successful
    except requests.RequestException as e:
        print(f"Error fetching the paper page: {e}")
        return ""

    soup = BeautifulSoup(response.text, 'html.parser')
    abstract_div = soup.find('div', text=lambda t: t and t.strip().startswith('Abstract'))

    if abstract_div:
        abstract_text = abstract_div.get_text(separator=' ', strip=True)
        return abstract_text
    abstract_div = soup.find('div', class_='abstract')  # Example: Find by class
    if abstract_div:
        abstract_text = abstract_div.get_text(separator=' ', strip=True)
        return abstract_text

    return ""


def search_papers(keywords, num_results=20):
    base_url = "https://scholar.google.com/scholar"
    results_per_page = 10
    global paper_list
    paper_list = []
    all_results = []
    count_results=None
    for start in range(0, num_results, results_per_page):
        search_url = f"{base_url}?q={keywords}&start={start}"
        response = requests.get(search_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        if 'Our systems have detected' in str(soup):
            continue
        if not count_results:
            count_div = soup.select_one('#gs_ab_md > div')
            count_results = count_div.text if count_div else "No summary available"
            count_results = count_results.split('about')[1] if 'about' in count_results else count_results

        for item in soup.select('[data-lid]'):
            title = item.select_one('.gs_rt').text
            link = item.select_one('.gs_rt a')['href']
            result = {'Title': title, 'Link': link}
            all_results.append(result)
            p = Paper(title, link=link)
            paper_list.append(p)

        if len(all_results) >= num_results:
            break

    all_results = all_results[:num_results]

    resultdict = {'papers': all_results, 'count': count_results}
    return resultdict

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
    if len(results):
        current_stack = current_stack.sort('Rating', descending=True)

    result = current_stack.to_dicts()
    end = time.time()
    totaltime = end - start
    print(f"Total time for book search: {totaltime} seconds")
    print(f'Books found: {len(results)}')
    # for row in current_stack.iter_rows(named=True):
    #     print(f'{row["Title"]} - {row["Rating"]}')
    return result

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

def save_books_list(books):
    try:
        for book in books:
            book_ref = db.collection(book_table).document()
            book_ref.set(book)
    except Exception as e:
        print(f"Error saving books: {str(e)}")

def reload_books():
    global book_df
    book_df = None
    load_books_into_dataframe()
    num = book_df.shape[0]
    return num

def delete_book(book):
    book_title = book.get('Title')
    global book_table
    if not book_title:
        return jsonify({"message": "Title is required"}), 400
    books_ref = db.collection(book_table)
    query = books_ref.where('Title', '==', book_title).stream()

    for doc in query:
        doc.reference.delete()
def add_to_done_list(books):
    for book in books:
        book_dict = book.to_dict()
        db.collection('dones').add(book_dict)
    print(f'Books uploaded to firebase : {len(books)}')
