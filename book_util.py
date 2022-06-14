import csv
import re
import json
from concurrent.futures.thread import ThreadPoolExecutor

import pandas as pd
from urllib import request, parse
from urllib.error import HTTPError

from sqlalchemy import create_engine

import requests
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from webdriver_manager.chrome import ChromeDriverManager

from config import *
from processor import dict_to_dataframe


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

crawl_df_column = df_column + ['pub_review', 'detail', 'category_d1', 'category_d2', 'category_d3']

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


def save_to_db(df):
    engine = create_engine(MY_SQL_DATABASE_URI, encoding='utf-8')
    conn = engine.connect()
    df.to_sql('book_temp', conn)
    _df = pd.read_sql('SELECT * FROM book_temp', conn)
    _df.to_sql(name='book', if_exists='append', con=engine, index=False)
    conn.close()

    return _df


def get_csv_file(pub):
    file_name = pub + CSV_FILE_NAME
    file_path = os.path.join(CSV_PATH, file_name)
    return True if os.path.isfile(file_path) else False


def search_book_by_publisher(pub):
    print('Search books using publisher "{}" as a query parameter ...'.format(pub))
    api = NaverSearch(
        client_id=API_CLIENT_ID,
        client_secret=API_CLIENT_SECRET,
        url=api_request_url,
        key="d_publ"
    )
    offset, total, items = 1, 1, []
    while offset <= total:
        result = api(pub)
        print(' * crawling [ {} / {} ] ... '.format(offset, total))
        offset, total = result['offset'], result['data']['total']
        items += result['data']['items']

    print('Convert dict to dataframe ...')
    df = dict_to_dataframe(items)
    pub_dict = {pub: df.shape[0]}
    print('Converting Finished.')

    print('Save dataframe to csv file ...')
    file_path = os.path.join(CSV_PATH, pub + CSV_FILE_NAME)
    df.to_csv(file_path, encoding='utf-8', index=False)
    print('{} Created.'.format(pub + CSV_FILE_NAME))

    return pub_dict


def crawl_book_detail_info(file_path, temp_path):
    df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
    df['pub_review'], df['detail'], df['category_d1'], df['category_d2'], df['category_d3'] = '', '', '', '', ''

    temp_df = pd.DataFrame(columns=crawl_df_column)
    temp_df.drop(df.filter(regex='Unnamed'), axis=1, inplace=True)
    temp_df.to_csv(temp_path, encoding='utf-8', index=False, columns=crawl_df_column)

    print('Start crawling detail information ... ')
    start, stop, step = df.index.start, df.index.stop, df.index.step
    total_df = []
    for i in range(start, stop, 100):
        links = [(i, stop, df.loc[i]) for i in range(i, i + 100, step)]
        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(get_book_info_using_request, links)

        df_list = [i.values.tolist() for i in results]
        total_df.extend(df_list)
        with open(temp_path, 'a') as f_obj:
            writer = csv.writer(f_obj)
            writer.writerows(df_list)
            f_obj.close()
        print('Crawling [ {} ~ {} ] Finished.'.format(i, i + 100))

    return total_df


def get_book_info_using_selenium(df_loc):
    i, end, row = df_loc
    if 'link' not in row:
        return

    global driver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    try:
        target_url = row['link']
        driver.get(target_url)
    except WebDriverException:
        print(' - exception [ {} / {} ] : {}'.format(i, end, row['title']))
        return

    try:
        full_description = driver.find_element(by=By.ID, value="bookIntroContent")
        row['description'] = full_description.text
    except NoSuchElementException:
        pass

    try:
        pub_review = driver.find_element(by=By.ID, value="pubReviewContent")
        row['pub_review'] = pub_review.text
    except NoSuchElementException:
        pass

    try:
        detail = driver.find_element(by=By.XPATH, value="//*[@id='content']/div[6]/p[1]")
        row['detail'] = detail.text
    except NoSuchElementException:
        pass

    try:
        row['category_d1'] = driver.find_element(by=By.XPATH, value="//*[@id='category_location1_depth']").text
    except NoSuchElementException:
        row['category_d1'] = ''

    try:
        row['category_d2'] = driver.find_element(by=By.XPATH, value="//*[@id='category_location2_depth']").text
    except NoSuchElementException:
        row['category_d2'] = ''

    try:
        row['category_d3'] = driver.find_element(by=By.XPATH, value="//*[@id='category_location3_depth']").text
    except NoSuchElementException:
        row['category_d3'] = ''

    print(' * crawling [ {} / {} ] : {}'.format(i, end, row['title']))
    return row


def get_book_info_using_request(df_loc):
    i, end, row = df_loc
    if 'link' not in row:
        return

    response = requests.get(row['link'])
    soup = BeautifulSoup(response.content, 'html.parser')

    full_description = soup.find(attrs={"id": "bookIntroContent"})
    if full_description:
        row['description'] = full_description.p.text

    pub_review = soup.find(attrs={"id": "pubReviewContent"})
    if pub_review:
        row['pub_review'] = pub_review.p.text

    detail = soup.select_one("#content > div:nth-child(7) > p:nth-child(2)")
    if detail:
        row['detail'] = detail.text

    category_d1 = soup.find(attrs={"id": "category_location1_depth"})
    if category_d1:
        row['category_d1'] = category_d1.text
    category_d2 = soup.find(attrs={"id": "category_location2_depth"})
    if category_d2:
        row['category_d2'] = category_d2.text
    category_d3 = soup.find(attrs={"id": "category_location3_depth"})
    if category_d3:
        row['category_d3'] = category_d3.text

    print(' * crawling [ {} / {} ] : {}'.format(i, end, row['title']))
    return row
