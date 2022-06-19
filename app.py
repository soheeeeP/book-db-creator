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

    if get_csv_file('search', pub):
        response = {'warning': '{} already exists.'.format(pub + CSV_FILE_EXT)}
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    response = search_book_by_publisher(pub)

    readme_path = os.path.join(CSV_PATH, "result.txt")
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

    pub_path = os.path.join(CSV_PATH, "search", pub + CSV_FILE_EXT)
    if os.path.isfile(pub_path) is False:
        response = {'warning': '{}.csv does not exist.'.format(pub)}
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    temp_path = os.path.join(CSV_PATH, "save", pub + CSV_FILE_EXT)
    df = crawl_book_detail_info(pub_path, temp_path)
    if df:
        print('Save dataframe in MYSQL database ...')
        save_to_db(df)
        print('Save to DB.')

    return make_response(json.dumps({pub: len(df) if df else 0}, ensure_ascii=False).encode('utf-8'))


@app.route('/save/all', methods=['GET'])
def save_all_book():
    readme_path = os.path.join(CSV_PATH, "result.txt")
    with open(readme_path, "r") as f_obj:
        lines = f_obj.read().splitlines()

    pubs = {l.rsplit(':', 1)[0]: l.rsplit(':', 1)[1] for l in lines}
    response = {}
    # save_pubs = []
    for pub in pubs.copy().keys():
        pub_path = os.path.join(CSV_PATH, "search", pub + CSV_FILE_EXT)
        temp_path = os.path.join(CSV_PATH, "save", pub + CSV_FILE_EXT)
        df_list = crawl_book_detail_info(pub_path, temp_path)
        if df_list:
            print('Save dataframe in MYSQL database ...')
            df = pd.DataFrame(df_list, columns=crawl_df_column)
            save_to_db(df)
            print('Save to DB.')
            response[pub] = len(df)
            del pubs[pub]

    with open(readme_path, "w") as f_obj:
        for k, v in pubs.items():
            f_obj.write('{}:{}\n'.format(k, v))
        f_obj.close()

    save_path = os.path.join(CSV_PATH, "save.txt")
    with open(save_path, "w") as f_obj:
        for k, v in response.items():
            f_obj.write('{}:{}\n'.format(k, v))
        f_obj.close()

    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    app.run()
