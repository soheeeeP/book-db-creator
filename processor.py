import re
import os

import numpy as np
import pandas as pd
from functools import lru_cache

from scipy import sparse
from sklearn import tree
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import train_test_split, GridSearchCV

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

    def __init__(self):
        f_dir = os.path.join(CSV_PATH, 'data', 'save')
        f_list = [os.path.join(f_dir, f_name) for f_name in os.listdir(f_dir)]

        df_list = []
        for f in f_list:
            df = pd.read_csv(f, encoding='utf-8', on_bad_lines='skip')
            df_list.append(df)

        self.raw_df = df_list
        self.contents = ['title', 'description', 'pub_review', 'detail']
        self.labels = ['title', 'category_d1', 'category_d2', 'category_d3']

    def __call__(self, df: pd.DataFrame, con_cols: list, label_cols: list):
        content_data = df[con_cols].to_dict('index')
        con_vec, con_mat, con_arr, con_dataset = self.contents_vectorizer(
            data=content_data,
            columns=con_cols,
            max_featuresv=1000
        )
        df_data_con = pd.DataFrame(con_arr, columns=list(con_vec.get_feature_names_out()))

        category_data = df[label_cols].to_dict('index')
        cat_vec, cat_mat, cat_arr, cat_dataset = self.label_vectorizer(
            data=category_data,
            columns=label_cols,
            max_featuresv=50
        )
        df_label_cat = pd.DataFrame(cat_arr, columns=list(cat_vec.get_feature_names_out()))

        train_set, test_set = train_test_split(df, test_size=0.25, random_state=93, shuffle=True)
        data, target, test_data, test_target, label = self.split_data(
            raw_data=df,
            labels=list(cat_dataset.values()),
            cat=df_label_cat,
            con=df_data_con,
            train_set=train_set,
            test_set=test_set
        )
        # todo: pickle로 vector, labels 저장
        self.create_tclf(data=data, target=target, test_data=test_data, test_target=test_target)

    @property
    def raw_df(self):
        return self._raw_df

    @raw_df.setter
    def raw_df(self, df_list):
        self._raw_df = pd.concat(df_list).dropna(axis=0)

    @raw_df.deleter
    def raw_df(self):
        del self._raw_df

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

    def extract_corpus(self, data, columns):
        corpus = []
        for key, record in data.items():
            docs = ''
            for c in columns:
                if c != 'title' and c in record and record[c] and record[c] != '':
                    docs += " " + str(record[c])
            corpus.append(' '.join(self.okt.nouns(docs)))
        return corpus

    def contents_vectorizer(self, data, columns, max_featuresv, max_dfv=0.8, min_dfv=0.02):
        vector = self.tfidvectorizer(max_dfv=max_dfv, max_featuresv=max_featuresv, min_dfv=min_dfv)
        matrix = vector.fit_transform(self.extract_corpus(data, columns))
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

    def label_vectorizer(self, data, columns, max_featuresv, max_dfv=0.8, min_dfv=0.2):
        vector = self.tfidvectorizer(max_dfv=max_dfv, max_featuresv=max_featuresv, min_dfv=min_dfv)
        matrix = vector.fit_transform(self.extract_corpus(data, columns))
        arr = matrix.toarray()
        feature_name = np.array(vector.get_feature_names_out())

        dataset = {}
        for i in range(arr.shape[0]):
            label_idx = np.argsort(-arr[i])[0]
            dataset[i] = (feature_name[label_idx], arr[i][label_idx])

        return vector, matrix, arr, dataset

    def split_data(self, raw_data, labels, cat, con, train_set, test_set):
        label_set = list(zip(raw_data.index, labels))  # LabelEncoder, LabelBinarizer
        label = {'train': [], 'test': []}

        train_idx = list(train_set.index)
        test_idx = list(test_set.index)

        for idx, enc in label_set:
            if idx in train_idx:
                label['train'].append(enc)
            elif idx in test_idx:
                label['test'].append(enc)
            else:
                continue

        data, target = con.loc[train_set.index], cat.loc[train_set.index]
        test_data, test_target = con.loc[test_set.index], cat.loc[test_set.index]

        spmat_data = sparse.csr_matrix(data.values)
        spmat_target = sparse.csr_matrix(target.values)
        spmat_test_data = sparse.csr_matrix(test_data.values)
        spmat_test_target = sparse.csr_matrix(test_target.values)

        return spmat_data, spmat_target, spmat_test_data, spmat_test_target, label

    def grid_search(self, classifier, params, data, target, test_data, test_target, name=None, n_jobs=1):
        cv = GridSearchCV(classifier, params, cv=5, n_jobs=n_jobs)
        # problem: dataframe? csr_matrix?
        cv.fit(data, target)

        print("Classifier: ", classifier)
        print("Hyper Parameters ", params)

        print(" >> 1. Best Params ")
        print(cv.best_params_)
        print("\t- Model Best Score : ", cv.best_score_)

        print(" >> 2. Classification Report ")
        pred_test_target = cv.predict(test_data)
        print(classification_report(test_target, pred_test_target))

        print(" >> 3. Accuracy Score ")
        accuracy = list(
            zip(cv.cv_results_['params'], cv.cv_results_['mean_train_score'], cv.cv_results_['mean_test_score']))
        for params, train_score, test_score in accuracy:
            print("\t - params :", params)
            print("\t - train mean score : ", train_score)
            print("\t - test mean score : ", test_score)
        print("\t- Model Best Score : ", cv.best_score_)

        print(" >> 4. Confusion Matrix ")
        print(confusion_matrix(test_target, pred_test_target))

        return cv.cv_results_, cv.best_estimator_

    def create_tclf(self, data, target, test_data, test_target):
        tclf = tree.DecisionTreeRegressor()
        params = {'max_depth': range(60, 85), 'min_samples_split': range(15, 30), 'max_features': ['auto']}
        tclf_results, tclf_best_est = self.grid_search(
            classifier=tclf,
            params=params,
            data=data,
            target=target,
            test_data=test_data,
            test_target=test_target,
            name='DecisionTreeRegressor',
            n_jobs=4
        )
        # self.save_model(best_est=tclf_best_est)

    def save_model(self, best_est, name, size, vocab, label_classes, blabel_classes):
        pass

