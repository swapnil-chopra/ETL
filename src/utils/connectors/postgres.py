import psycopg2
import json
from psycopg2.extras import RealDictCursor
import pandas as pd


class PostgreConnector():
    def __init__(self) -> None:
        self.connection = None
        self.connect()
        pass

    def connect(self):
        self.connection = psycopg2.connect(
            database='etl', user='postgres', password='admin', host='127.0.0.1', port=5432
        )

    def get_data_from_query(self, query: str):

        if not self.connection:
            self.connect()
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)
        data = cursor.fetchall()
        return data

    def get_data_from_query_as_df(self, query):
        df = pd.read_sql_query(query, self.connection)
        return df

    def get_data_from_component(self, component, columns: list = []):
        query = """"""
        if columns:
            columns_str = ','.join(columns)
        else:
            columns_str = "*"
        query = f'select {columns_str} from {component}'
        data = self.get_data_from_query_as_df(query)
        return data

    def execute_manipulation_query(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()

    def create_insert_query(self, data_dict: dict, table):
        query = ''
        cols = ''
        vals = ''
        for data in data_dict.keys():
            cols += f",{data}"
            vals += f",'{data_dict[data]}'"
        cols = cols.strip(',')
        vals = vals.strip(',')
        query = f'Insert into {table} ({cols}) values ({vals})'
        return query

    def insert_data_to_db(self, data_dict, table):
        """"""
        query = self.create_insert_query(data_dict, table)
        self.execute_manipulation_query(query)
        return True

    def create_update_query(self, data_dict, table, primary_col):
        query = ''
        pairs = ''
        for data in data_dict.keys():
            if data == primary_col:
                continue
            pairs += f",{data}='{data_dict[data]}'"
        pairs = pairs.strip(',')
        query = f"Update {table} set {pairs} where {primary_col}='{data_dict[primary_col]}'"
        return query

    def update_data_in_db(self, data_dict, table, primary_col='id'):
        query = self.create_update_query(data_dict, table, primary_col)
        self.execute_manipulation_query(query)
        print(query)


d = {
    "id": '1',
    "process_step": 'EXTRACT',
    "process_status": 'PROCESSING',
    "final_status": 'In Progress',

}
# PostgreConnector().get_data_from_component(
#     'comp_source_to_target_field_mapping')
# PostgreConnector().update_data_in_db(d, 'request_tracker')
