import re
import json
import pandas as pd
from urllib import request, parse
from urllib.error import HTTPError

from sqlalchemy import create_engine

from config import MY_SQL_DATABASE_URI, test_pub_size


class NaverSearch:
    offset = 1
    default_display = 100

    def __init__(self, display=None, client_id=None, client_secret=None, url=None, key=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = url + key + "="

        if display is None:
            self.display = self.default_display

    def __call__(self, param, offset=None):
        data = self.get_query_response(param)
        if offset is None:
            self.offset += self.display

        return {
            "offset": self.offset,
            "data": data
        }

    def get_query_response(self, param):
        param = parse.quote(param)
        url = self.url + param + "&display=" + str(self.display)
        req = request.Request(url=url)
        req.add_header("X-Naver-Client-Id", self.client_id)
        req.add_header("X-Naver-Client-Secret", self.client_secret)

        try:
            response = request.urlopen(req)
        except HTTPError as e:
            raise Exception('ERROR {}: {}'.format(e.code, e.reason))
        else:
            res_code = response.getcode()

        res_body = response.read().decode('utf-8')
        data = json.loads(res_body)

        return data


df_column = [
    "title",
    "author",
    "publisher",
    "isbn",
    "description",
    "image",
    "link"
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

test_pubs = prod_pubs[:test_pub_size]


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
