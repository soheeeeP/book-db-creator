import json
import os
import csv

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from webdriver_manager.chrome import ChromeDriverManager

from flask import Flask, make_response
import pandas as pd

from config import api_request_url, API_CLIENT_ID, API_CLIENT_SECRET, CSV_PATH, CSV_FILE_NAME, RUN_MODE, options
from book_data_saver import NaverSearch, prod_pubs, test_pubs, dict_to_dataframe, df_column

app = Flask(__name__)


@app.route('/', methods=['GET'])
def main():
    # TODO: tutorial readme
    return make_response()


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

    if os.path.exists(csv_path):
        created_csv_name = "temp_" + created_csv_name
        csv_path = os.path.join(CSV_PATH, "temp_" + created_csv_name)
    total_df.to_csv(csv_path, encoding='utf-8', index=False)

    print('{} file successfully created at {}'.format(created_csv_name, CSV_PATH))
    return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))


# TODO: exception을 catch하여 일단 저장 / Thread 사용
@app.route('/crawl', methods=['GET'])
def scribble_book_crawler():
    try:
        csv_path = os.path.join(CSV_PATH, CSV_FILE_NAME)
        df = pd.read_csv(csv_path, encoding='utf-8')

        # 임시 csv 파일 생성
        new_csv_name = "new_" + CSV_FILE_NAME
        new_csv_path = os.path.join(CSV_PATH, new_csv_name)

        _df_column = df_column + ['pub_review', 'detail']
        new_df = pd.DataFrame(columns=_df_column)
        new_df.drop(df.filter(regex="Unnamed"), axis=1, inplace=True)
        new_df.to_csv(new_csv_path, encoding='utf-8', index=False, columns=_df_column)
    except FileNotFoundError as e:
        response = {'error': '{} does not exist'.format(e.filename)}
        return make_response(json.dumps(response, ensure_ascii=False).encode('utf-8'))

    size = df.shape[0]
    start, stop, step = df.index.start, df.index.stop, df.index.step
    temp_idx = start

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    print('Start crawling detail information ... ')
    for i in range(start, stop, step):
        if i > 0 and i % 100 == 0:
            rows = df.loc[temp_idx:i].values.tolist()
            with open(new_csv_path, 'a') as f_obj:
                writer_obj = csv.writer(f_obj)
                writer_obj.writerows(rows)
                f_obj.close()
            temp_idx = i

        try:
            target_url = df.loc[i, 'link']
            driver.get(target_url)
        except WebDriverException:
            print(' - exception [ {} / {} ] : {}'.format(i, stop, df.loc[i, 'title']))
            continue

        try:
            full_description = driver.find_element(by=By.ID, value="bookIntroContent")
            df.loc[i, 'description'] = full_description.text
        except NoSuchElementException:
            pass

        try:
            pub_review = driver.find_element(by=By.ID, value="pubReviewContent")
            df.loc[i, 'pub_review'] = pub_review.text
        except NoSuchElementException:
            pass

        try:
            detail = driver.find_element(by=By.CSS_SELECTOR, value="#content > div:nth-child(7) > p:nth-child(2)")
            df.loc[i, 'detail'] = detail.text
        except NoSuchElementException:
            pass

        print(' * crawling [ {} / {} ] : {}'.format(i, stop, df.loc[i, 'title']))
    print('Crawling Finished.')

    temp_idx = stop % 100
    rows = df.loc[temp_idx:stop].values.tolist()
    with open(new_csv_path, 'a') as f_obj:
        writer_obj = csv.writer(f_obj)
        writer_obj.writerows(rows)
        f_obj.close()

    print('{} file successfully created at {}'.format(new_csv_name, CSV_PATH))
    return make_response(json.dumps({'success': size}, ensure_ascii=False).encode('utf-8'))


if __name__ == '__main__':
    app.run()
