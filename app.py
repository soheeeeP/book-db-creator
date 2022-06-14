import json
import os

import csv
import time

from concurrent.futures import ThreadPoolExecutor

from flask import Flask, make_response, request as flask_request
import pandas as pd

from config import api_request_url, API_CLIENT_ID, API_CLIENT_SECRET, CSV_PATH, CSV_FILE_NAME, RUN_MODE, options
from book_data_saver import *


app = Flask(__name__)


@app.route('/', methods=['GET'])
def main():
    # TODO: tutorial readme
    return make_response()


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


@app.route('/search', methods=['GET'])
def search_book():
    param = flask_request.args.to_dict()
    pub = param.get('publisher', '')
    if pub == '':
        pubs = prod_pubs if RUN_MODE == 'prod' else test_pubs
        response = {
            'warning': 'Pass publisher string as a query parameter.',
            'publisher_list': pubs
        }
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    if get_csv_file(pub):
        response = {'warning': '{} already exists.'.format(pub + CSV_FILE_NAME)}
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    response = search_book_by_publisher(pub)

    readme_path = os.path.join(CSV_PATH, "download.txt")
    with open(readme_path, 'a') as f_obj:
        writer = csv.writer(f_obj)
        writer.writerow(['{}:{}'.format(pub, response[pub])])
        f_obj.close()

    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


@app.route('/save', methods=['GET'])
def save_book():
    param = flask_request.args.to_dict()

    pub = param.get('publisher', '')
    if pub == '':
        pubs = prod_pubs if RUN_MODE == 'prod' else test_pubs
        response = {
            'warning': 'Pass publisher string as a query parameter.',
            'publisher_list': pubs
        }
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    pub_path = os.path.join(CSV_PATH, pub + CSV_FILE_NAME)
    if os.path.isfile(pub_path) is False:
        response = {'warning': '{}.csv does not exist.'.format(pub)}
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    temp_path = os.path.join(CSV_PATH, "temp_" + pub + CSV_FILE_NAME)
    df = crawl_book_detail_info(pub_path, temp_path)
    if df:
        print('Save dataframe in MYSQL database ...')
        save_to_db(df)
        print('Save to DB.')

    return make_response(json.dumps({pub: len(df) if df else 0}, ensure_ascii=False).encode('utf-8'))


@app.route('/save/all', methods=['GET'])
def save_all_book():
    readme_path = os.path.join(CSV_PATH, "download.txt")
    with open(readme_path, "r") as f_obj:
        lines = f_obj.read().splitlines()

    pubs = [l.rsplit(':', 1)[0] for l in lines]
    response = {pub: 0 for pub in pubs}
    for pub in pubs:
        pub_path = os.path.join(CSV_PATH, pub + CSV_FILE_NAME)
        temp_path = os.path.join(CSV_PATH, "temp_" + pub + CSV_FILE_NAME)
        df_list = crawl_book_detail_info(pub_path, temp_path)
        if df_list:
            print('Save dataframe in MYSQL database ...')
            df = pd.DataFrame(df_list, columns=crawl_df_column)
            save_to_db(df)
            print('Save to DB.')
            response[pub] = len(df)

    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    app.run()
