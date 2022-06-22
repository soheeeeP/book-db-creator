import re
import os

import numpy as np
import pandas as pd
from functools import lru_cache

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
        """
        도서 데이터에 대한 벡터화를 수행하는 경우, columns에 ['description', 'pub_review', 'detail']를 전달한다.
        도서 카테고리에 대한 벡터화를 수행하는 경우, columns에 ['category_d1', 'category_d2', 'category_d3']를 전달한다.
        """
        tfidf_vectorizer = self.tfidvectorizer(max_dfv=max_dfv, max_featuresv=max_featuresv, min_dfv=min_dfv)
        corpus = []
        for record in data:
            docs = ''
            for c in columns:
                if c in record and record[c] and record[c] != '':
                    docs += " " + str(record[c])
                corpus.append(' '.join(self.okt.nouns(docs)))

        tfidf_matrix = tfidf_vectorizer.fit_transform(corpus)
        tfidf_vocs = tfidf_vectorizer.vocabulary_
        tfidf_arr = tfidf_matrix.toarray()

        count = tfidf_arr.sum(axis=0)
        idx = np.argsort(-count)        # 내림차순 정렬 index
        feature_name = np.array(tfidf_vectorizer.get_feature_names_out())
        words = list(zip(feature_name[idx], count[idx]))

        return tfidf_vocs, tfidf_arr, tfidf_matrix, words
