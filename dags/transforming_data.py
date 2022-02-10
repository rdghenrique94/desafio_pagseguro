from datetime import date
import pandas as pd
import numpy as np
from conn_sql import open_connection


# utilizar dataframe para limpeza
def pd_csv_read(directory):
    return pd.read_csv(directory, header=0, quotechar="'")


# grupo de funções
def group_def(df):
    cleaning_data(df)
    creating_year_col(df)


# alterando estrutura da coluna age, gender, amount
def cleaning_data(df):
    # alterando coluna age de string para int
    df["age"].mask(df["age"] == 'U', 0, inplace=True)
    df["age"] = df["age"].astype('int')
    # alterando coluna gender para M e F
    df["gender"].where((df["gender"] == "M") | (df["gender"] == "F"), None, inplace=True)
    # alterando linhas de valor 0 para null
    df["amount"].mask(df["amount"] == 0, None, inplace=True)


# adicionando a tabela a coluna day, month, yead, compl_date
def creating_year_col(df):
    # coluna year
    year = 2022
    df["year"] = year

    # coluna month com separados por valores na coluna step
    df["month"] = 0
    df["month"] = np.where((df["step"] >= 0) & (df["step"] <= 30), 1, df["month"])
    df["month"] = np.where((df["step"] >= 31) & (df["step"] <= 60), 2, df["month"])
    df["month"] = np.where((df["step"] >= 61) & (df["step"] <= 90), 3, df["month"])
    df["month"] = np.where((df["step"] >= 91) & (df["step"] <= 120), 4, df["month"])
    df["month"] = np.where((df["step"] >= 121) & (df["step"] <= 150), 5, df["month"])
    df["month"] = np.where((df["step"] >= 151) & (df["step"] <= 180), 6, df["month"])

    # coluna day
    day = 1
    df["day"] = day

    # coluna data completa
    df["compl_date"] = None
    df["compl_date"] = np.where((df["month"] == 1), date(year, 1, day), df["compl_date"])
    df["compl_date"] = np.where((df["month"] == 2), date(year, 2, day), df["compl_date"])
    df["compl_date"] = np.where((df["month"] == 3), date(year, 3, day), df["compl_date"])
    df["compl_date"] = np.where((df["month"] == 4), date(year, 4, day), df["compl_date"])
    df["compl_date"] = np.where((df["month"] == 5), date(year, 5, day), df["compl_date"])
    df["compl_date"] = np.where((df["month"] == 6), date(year, 6, day), df["compl_date"])


# função de execução de limpeza dos dados
def executor():
    csv_read_file = pd_csv_read('bs140513_032310.csv.zip')
    group_def(csv_read_file)
    csv_read_file.to_sql(con=open_connection(), schema="db", name="transactions", if_exists="replace", index=True, index_label="id")
