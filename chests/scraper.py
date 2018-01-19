import requests
import logging
import pandas as pd
from bs4 import BeautifulSoup
import chests.utils as utl


class WebTable(object):
    def __init__(self, url=None):
        self.url = url
        self.df = pd.DataFrame()
        self.soup = None
        self.column_names = []
        self.table_body = None
        self.table_head = None
        self.rows = None
        self.table = None
        self.tables = None
        if self.url:
            self.load_page_find_tables()

    def load_page_find_tables(self):
        self.initialize_page(self.url)
        self.find_all_tables()

    def initialize_page(self, url=None):
        r = requests.get(url)
        self.soup = BeautifulSoup(r.text, 'lxml')

    def find_all_tables(self):
        logging.info('Finding tables at ' + str(self.url))
        if not self.soup:
            self.initialize_page(self.url)
        self.tables = self.soup.findAll('table')

    def get_table_body(self):
        self.table_body = self.table.find('tbody')

    def get_table_head(self):
        self.table_head = self.table.find('thead')

    def get_table_rows(self, table_part):
        self.rows = table_part.find_all('tr')

    def headers_to_df(self):
        logging.debug('Reading table headers to df.')
        self.column_names = []
        self.get_table_head()
        self.get_table_rows(self.table_head)
        cols = self.rows[0].find_all('th')
        for col in cols:
            col = col.text.strip()
            self.column_names.append(col)
        self.df.columns = self.column_names

    def body_to_df(self, col_loc=None):
        logging.debug('Reading table body to df.')
        self.df = pd.DataFrame()
        self.get_table_body()
        if not self.table_body:
            logging.warning('Could not find table body.  ' +
                            'Could not write to df.')
            return None
        self.get_table_rows(self.table_body)
        for row in self.rows:
            cols = row.find_all('td')
            cols = [x.text.strip() for x in cols]
            if cols:
                cols = pd.Series(cols)
                self.df = self.df.append(cols, ignore_index=True)
        if self.df.empty:
            return None
        if isinstance(col_loc, int) or isinstance(col_loc, float):
            self.df = utl.first_last_adj(self.df, col_loc, 0)
        elif col_loc == 'head':
            self.headers_to_df()

    def body_to_df_and_write(self, col_loc, file_path, file_name):
        self.body_to_df(col_loc)
        if not self.df.empty:
            if not file_name:
                file_name = self.file_name_from_columns()
            utl.df_to_csv(self.df, file_name, file_path)

    def all_tables_to_df(self, url=None, col_loc=None, file_path=None,
                         file_name=None, table_idx=None):
        if url:
            self.url = url
            self.load_page_find_tables()
        if table_idx:
            if (table_idx + 1) > len(self.tables):
                logging.warning(str(len(self.tables)) + ' tables found.  '
                                'Could not get table #' + str(table_idx + 1))
                return None
            self.table = self.tables[table_idx]
            self.body_to_df_and_write(col_loc, file_path, file_name)
        else:
            for table in self.tables:
                self.table = table
                self.body_to_df_and_write(col_loc, file_path, file_name)

    def file_name_from_columns(self):
        file_name = '_'.join(list(self.df.columns.values.astype(str)))
        file_name = file_name.replace('/', '-')
        file_name += '.csv'
        return file_name
