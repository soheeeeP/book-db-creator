import re
import pandas as pd
from book_util import df_column


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
