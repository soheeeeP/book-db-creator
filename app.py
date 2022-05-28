import json
import re
import os
from flask import Flask, make_response
import pandas as pd
from sqlalchemy import create_engine

from config import api_request_url, API_CLIENT_ID, API_CLIENT_SECRET, MY_SQL_DATABASE_URI, CSV_PATH, RUN_MODE
from book_data_crawler import NaverSearch

app = Flask(__name__)


df_column = [
    "title",
    "author",
    "publisher",
    "isbn",
    "description",
    "image",
]


prod_pubs = [
    "위즈덤하우스",
    "미디어그룹",
    "시공사",
    "문학동네",
    "북이십일",
    "김영사",
    "창비",
    "웅진싱크빅",
    "도서출판길벗",
    "민음사",
    "알에이치코리아",
    "다산북스",
    "학지사",
    "마더텅",
    "아가페출판사",
    "비룡소",
    "한빛미디어",
    "넥서스",
    "박영사",
    "쌤앤파커스",
    "영진닷컴",
    "가나문화콘텐츠",
    "계림북스",
    "을유문화사",
    "자음과모음",
    "개암나무"
]

test_pubs = prod_pubs[:3]


def preprocessor(df):
    reg_ex = '<.+?>'

    df.loc[:, 'title'] = df['title'].map(lambda x: re.sub(reg_ex, '', x))
    df.loc[:, 'author'] = df['author'].map(lambda x: re.sub(reg_ex, '', x))
    df.loc[:, 'publisher'] = df['publisher'].map(lambda x: re.sub(reg_ex, '', x))
    df.loc[:, 'description'] = df['description'].map(lambda x: re.sub(reg_ex, '', x))

    df.loc[:, 'image'] = df['image'].map(lambda x: re.sub(reg_ex, '', x).rsplit('?', 1)[0])
    df.loc[:, 'isbn'] = df['isbn'].map(lambda x: re.sub(reg_ex, '', x).rsplit(' ', 1)[1])

    return df


def dict_to_dataframe(items):
    books = []
    for item in items:
        if item['isbn'] == '':
            continue
        books.append([item[col] for col in df_column])

    df = pd.DataFrame(books, columns=df_column)
    df.dropna(axis=0, how='any', inplace=True)

    return preprocessor(df)


def save_to_db(df):
    engine = create_engine(MY_SQL_DATABASE_URI, encoding='utf-8')
    conn = engine.connect()
    df.to_sql('book_t', conn)
    _df = pd.read_sql('SELECT * FROM book_t', conn)
    _df.to_sql(name='book', if_exists='replace', con=engine, index=True, index_label='isbn')
    conn.close()

    return _df


@app.route('/', methods=['GET'])
def scribble_book_saver():
    pubs = prod_pubs if RUN_MODE == 'prod' else test_pubs

    df_list = []
    response = {pub: 0 for pub in pubs}
    for pub in pubs:
        print('Search books using publisher "{}" as a query parameter ... '.format(pub))
        offset, total = 1, 1
        items = []

        api = NaverSearch(
            client_id=API_CLIENT_ID,
            client_secret=API_CLIENT_SECRET,
            url=api_request_url,
            key="d_publ"
        )

        while offset <= total:
            result = api(pub)
            print(' * crawling [ {} / {} ] ... '.format(offset, total))
            offset, total = result['offset'], result['data']['total']
            items += result['data']['items']

        print('Convert dict to dataframe ...')
        df = dict_to_dataframe(items)
        df_list.append(df)
        response[pub] = df.shape[0]
        print('Converting Finished.')

        # print('Save dataframe in MYSQL database ...')
        # save_to_db(df)
    print('Searching Finished.')

    print('Save dataframe in csv ...')
    total_df = pd.concat(df_list)
    csv_path = os.path.join(CSV_PATH, 'book_db.csv')
    if os.path.exists(csv_path) is False:
        total_df.to_csv(csv_path, encoding='utf-8')

    print('book_db.csv file successfully created at {}'.format(csv_path))
    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    app.run()
