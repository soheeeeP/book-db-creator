import json
import os
from flask import Flask, make_response
import pandas as pd

from config import api_request_url, API_CLIENT_ID, API_CLIENT_SECRET, CSV_PATH, CSV_FILE_NAME, RUN_MODE, options
from book_data_saver import NaverSearch, prod_pubs, test_pubs, dict_to_dataframe

app = Flask(__name__)


@app.route('/save', methods=['GET'])
def scribble_book_saver():
    pubs = prod_pubs if RUN_MODE == 'prod' else test_pubs

    created_csv_name = CSV_FILE_NAME
    csv_path = os.path.join(CSV_PATH, created_csv_name)
    try:
        pd.read_csv(csv_path, encoding='utf-8')
        response = {
            'warning': '{} already exists.'.format(created_csv_name),
            'message': 'If you want to refresh your .csv file, send the request back to the server.'.format(
                created_csv_name)
        }
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))
    except FileNotFoundError as e:
        pass

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
    if os.path.exists(csv_path):
        csv_path = os.path.join(CSV_PATH, 'book_db_temp.csv')
    total_df.to_csv(csv_path, encoding='utf-8')

    print('book_db.csv file successfully created at {}'.format(csv_path))
    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    app.run()
