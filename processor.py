import re
import os

import numpy as np
import pandas as pd
from functools import lru_cache

from scipy import sparse
from sklearn.model_selection import train_test_split

from config import CSV_PATH
from nltk.corpus import stopwords
from konlpy.tag import Okt
from sklearn.feature_extraction.text import TfidfVectorizer

df_column = [
    "title",
    "author",
    "publisher",
    "isbn",
    "description",
    "image",
    "link"
]


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


class BookPredictionProcessor:
    okt = Okt()

    def __call__(self, df: pd.DataFrame, label1: list, label2: list):
        # todo: Pickle로 vector, labels 저장해두기
        """
        :param df: 도서 데이터 dataframe
        :param label1: category label list
        :param label2: content label list
        :return:
        """

        category_data = df[label1].to_dict('index')
        cat_vec, cat_mat, cat_arr, cat_dataset = self.contents_vectorizer(data=category_data, columns=label1)

        content_data = df[label2].to_dict('index')
        con_vec, con_mat, con_arr, con_dataset = self.contents_vectorizer(data=content_data, columns=label2)

        df_cat = pd.DataFrame(cat_arr, columns=list(cat_vec.get_feature_names_out()))
        df_con = pd.DataFrame(con_arr, columns=list(con_vec.get_feature_names_out()))

        train_set, test_set = self.split_data(df, list(zip(cat_dataset.values(), con_dataset.values())))
        sparse_mat_train_cat, sparse_mat_test_cat, sparse_mat_train_con, sparse_mat_test_con \
            = self.split_matrix(df_cat, df_con, train_set, test_set)

    def _tfidvectorizer(self, max_dfv, max_featuresv, min_dfv):
        eng_stopwords = list(set(stopwords.words('english')))
        book_stopwords = ['정가', '단가', '저자', '출판사', '소설가', '베스트셀러']
        kor_stopwords_lines = open(os.path.join(CSV_PATH, 'stop_words.txt'), encoding='utf-8').readlines()
        kor_stopwords = [line.rstrip() for line in kor_stopwords_lines]

        stopwords_set = kor_stopwords + eng_stopwords + book_stopwords

        tfidf_vectorizer = TfidfVectorizer(
            max_df=max_dfv,
            max_features=max_featuresv,
            min_df=min_dfv,
            stop_words=stopwords_set,
            use_idf=True,
            ngram_range=(1, 1),
            dtype=np.int64
        )
        return tfidf_vectorizer

    @lru_cache(maxsize=2)
    def tfidvectorizer(self, max_dfv, max_featuresv, min_dfv):
        return self._tfidvectorizer(max_dfv, max_featuresv, min_dfv)

    def contents_vectorizer(self, data, columns, max_dfv=0.8, max_featuresv=1000, min_dfv=0.02):
        vector = self.tfidvectorizer(max_dfv=max_dfv, max_featuresv=max_featuresv, min_dfv=min_dfv)
        corpus = []
        for key, record in data.items():
            docs = ''
            for c in columns:
                if c != 'title' and c in record and record[c] and record[c] != '':
                    docs += " " + str(record[c])
            corpus.append(' '.join(self.okt.nouns(docs)))

        matrix = vector.fit_transform(corpus)
        arr = matrix.toarray()
        feature_name = np.array(vector.get_feature_names_out())

        dataset = {}
        for i in range(arr.shape[0]):
            idx_order = np.argsort(-arr[i])
            features = []
            for idx in idx_order:
                features.append((feature_name[idx], arr[i][idx]))
            dataset[i] = features

        return vector, matrix, arr, dataset

    def split_data(self, raw_data, labels, test_size=0.25):
        train_set, test_set = train_test_split(raw_data, test_size=test_size, random_state=93)

        s_label_1, label_1 = list(zip(raw_data.index, labels[0])), {'train': [], 'test': []} # Label Binarization
        s_label_2, label_2 = list(zip(raw_data.index, labels[1])), {'train': [], 'test': []}

        for ti in np.argwhere(train_set):
            for idx, enc in s_label_1:
                if ti == idx:  # array(shape_0_idx, shape_1_idx)
                    label_1['train'].append(enc)
                    break
            for idx, enc in s_label_2:
                if ti == idx:
                    label_2['train'].append(enc)
                    break

        for ti in np.argwhere(test_set):
            for idx, enc in s_label_1:
                if ti == idx:  # array(shape_0_idx, shape_1_idx)
                    label_1['test'].append(enc)
                    break
            for idx, enc in s_label_2:
                if ti == idx:
                    label_2['test'].append(enc)
                    break

        return train_set, test_set

    def split_matrix(self, cat, con, train_set, test_set):
        train_cat, test_cat = cat.loc[train_set.index], cat.loc[test_set.index]
        mat_train_cat = train_cat.values
        mat_test_cat = test_cat.values
        sparse_mat_train_cat = sparse.csr_matrix(mat_train_cat)
        sparse_mat_test_cat = sparse.csr_matrix(mat_test_cat)

        train_con, test_con = con.loc[train_set.index], con.loc[test_set.index]
        mat_train_con = train_con.values
        mat_test_con = test_con.values
        sparse_mat_train_con = sparse.csr_matrix(mat_train_con)
        sparse_mat_test_con = sparse.csr_matrix(mat_test_con)

        return sparse_mat_train_cat, sparse_mat_test_cat, sparse_mat_train_con, sparse_mat_test_con
