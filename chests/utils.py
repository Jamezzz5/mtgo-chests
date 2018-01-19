import sys
import os
import logging
import pandas as pd


def dir_check(directory):
    if not os.path.isdir(directory):
        os.makedirs(directory)


def first_last_adj(df, first_row, last_row):
    logging.debug('Removing First & Last Rows')
    first_row = int(first_row)
    last_row = int(last_row)
    if first_row > 0:
        df.columns = df.loc[first_row - 1]
        df = df.iloc[first_row:]
    if last_row > 0:
        df = df[:-last_row]
    if pd.isnull(df.columns.values).any():
        logging.warning('At least one column name is undefined.  Your first'
                        'row is likely incorrect. For reference the first few'
                        'rows are:\n' + str(df.head()))
        sys.exit(0)
    return df


def import_read_csv(path, filename):
    raw_file = path + filename
    try:
        df = pd.read_csv(raw_file, parse_dates=True)
    except pd.io.common.CParserError:
        df = pd.read_csv(raw_file, parse_dates=True, sep=None, engine='python')
    except UnicodeDecodeError:
        df = pd.read_csv(raw_file, parse_dates=True, encoding='iso-8859-1')
    return df


def df_to_csv(df, file_name, file_path=None):
    if file_path:
        dir_check(file_path)
        full_file_path = file_path + file_name
    else:
        full_file_path = file_name
    if full_file_path[-4:] != '.csv':
        full_file_path += '.csv'
    logging.info('Writing df to ' + full_file_path)
    try:
        df.to_csv(full_file_path, index=False, encoding='utf-8')
    except IOError:
        logging.warning(full_file_path + ' could not be saved.')


def df_col_to_float(df, float_col=None):
    if float_col is None:
        float_col = []
    for col in float_col:
        if col not in df:
            continue
        df[col] = df[col].astype('U')
        df[col] = df[col].apply(lambda x: x.replace(',', ''))
        df[col] = df[col].apply(lambda x: x.replace('$', ''))
        df[col] = df[col].apply(lambda x: x.replace('%', ''))
        df[col] = df[col].replace('nan', 0)
        df[col] = df[col].replace('NA', 0)
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df[col] = df[col].astype(float)
    return df
