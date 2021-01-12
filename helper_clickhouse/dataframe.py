import csv
import sys
import numpy as np
import pandas as pd
from clickhouse_driver import Client
from collections import OrderedDict
from toolz import itemmap, keymap, valmap
from .table import create_table
from .string import snack_case_list

def clickhouse_dtypes(DataFrame, index=False):

    MAPPING = {'object': 'String',
            'uint64': 'UInt64',
            'uint32': 'UInt32',
            'uint16': 'UInt16',
            'uint8': 'UInt8',
            'float64': 'Float64',
            'float32': 'Float32',
            'int64': 'Int64',
            'int32': 'Int32',
            'int16': 'Int16',
            'int8': 'Int8',
            'datetime64[D]': 'Date',
            'datetime64[ns]': 'DateTime'}

    PD2CH = keymap(np.dtype, MAPPING)
    CH2PD = itemmap(reversed, MAPPING)
    CH2PD['Null'] = 'object'
    CH2PD['Nothing'] = 'object'

    NULLABLE_COLS = ['UInt64', 'UInt32', 'UInt16', 'UInt8', 'Float64', 'Float32',
                    'Int64', 'Int32', 'Int16', 'Int8', 'String']

    for col in NULLABLE_COLS:
        CH2PD['Nullable({})'.format(col)] = CH2PD[col]

    if index:
        DataFrame = DataFrame.reset_index()

    for col in DataFrame.select_dtypes([bool]):
        DataFrame[col] = DataFrame[col].astype('uint8')

    dtypes = valmap(PD2CH.get, OrderedDict(DataFrame.dtypes))
    if None in dtypes.values():
        raise ValueError('Unknown type mapping in dtypes: {}'.format(dtypes))

    return dtypes


def clickhouse_create_table(table_name, DataFrame: pd.DataFrame, client: Client):
    create_table(table_name, clickhouse_dtypes(DataFrame=DataFrame), client)


def snack_case_cols(DataFrame: pd.DataFrame):
    DataFrame.columns = snack_case_list(list_col=list(DataFrame.columns))


def clickhouse_ingest(table_name, DataFrame: pd.DataFrame, client: Client, autocreate_table=True, snack_case=True):
    if snack_case: snack_case_cols(DataFrame=DataFrame)

    if autocreate_table:
        print("Auto create table")
        clickhouse_create_table(table_name=table_name, DataFrame=DataFrame, client=client)

    for col,dt in DataFrame.dtypes.items():
        if dt == 'object':
            DataFrame[col] = DataFrame[col].astype(str)

    print("Generate data to cols")
    cols = list(DataFrame.columns)
    rows = DataFrame.to_dict('records')

    print("Start Inserting data ")
    sql = 'INSERT INTO {table_name} ({x}) VALUES'.format(x=','.join(cols), table_name=table_name)
    client.execute(sql, rows,types_check=True)

    print("Inserting success")