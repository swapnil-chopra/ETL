import pandas as pd
from src.utils.application.settings import SOURCE_SYSTEM_CONFIG
from src.utils.generics.signals import on_task_completed, save_df_to_parquet


def get_data_from_excel_as_df(path):
    print(path)
    data = pd.read_csv(path)
    data = data.astype('str')
    return data


def extract():
    for index, config in SOURCE_SYSTEM_CONFIG.iterrows():
        if config['source_type'] == 'excel':
            data = get_data_from_excel_as_df(config['source_path'])
        temp_file_location = r'P:\Projects\ETL Projects\ETL\src\parquet_files\data.parquet'
        save_df_to_parquet(data, temp_file_location)
        signal_dict = {"id": '3', "temp_data_path": temp_file_location,
                       "process_status": 'Extraction Completed'}
        on_task_completed(signal_dict, 'request_tracker',
                          {'ip': '192.168.56.101:9092', "topic": 'transform'})
