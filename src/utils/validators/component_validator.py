import os
import pandas as pd


def sanatize_data(data: str):
    return data.strip().lower()


def validate_system_configuration(input_data: pd.DataFrame, system_type: str) -> dict:
    system_types = ['target', 'source']
    if system_type not in system_types:
        return {'message': 'Invalid system type provided'}
    enabled_data = input_data[input_data['is_active'] == True]

    # if len(enabled_data) == 1:
    for index, data in enabled_data.iterrows():
        if sanatize_data(data[f'{system_type}_type']) == 'excel' or (sanatize_data(data[f'{system_type}_type']) == 'db' and sanatize_data(data[f'{system_type}']) == 'sqlite'):
            if os.path.exists(data[f'{system_type}_path']):
                return {'message': 'Success'}
        elif sanatize_data(data[f'{system_type}_type']) == 'db':
            if data[f'{system_type}_host'] != '' or data[f'{system_type}_port'] != '' or data[f'{system_type}_db_name'] != '' \
                    or data[f'{system_type}_db_username'] != '' or data[f'{system_type}_db_password'] != '':
                return {'message': 'Success'}
    return {'message': 'Incomplete Data Provided'}
