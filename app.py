import json
import os

import csv
import time

from flask import Flask, make_response, request as flask_request
import pandas as pd

from book_util import *


app = Flask(__name__)


@app.route('/', methods=['GET'])
def main():
    # TODO: tutorial readme
    return make_response()


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
