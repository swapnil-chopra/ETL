import pandas as pd
from src.utils.connectors.postgres import PostgreConnector
from src.utils.brokers.producer import Producer
import json
from src.utils.generics.signals import on_task_completed, save_df_to_parquet

connector = PostgreConnector()
k = connector.get_data_from_component('comp_source_to_target_field_mapping')


def transform_df(input_df: pd.DataFrame, mapping_df: pd.DataFrame):
    """
    this function transforms source data to the specified data mapping in db and returns the final df

    """
    output_df = pd.DataFrame()
    for index, value in mapping_df.iterrows():
        source_col = value['source column']
        target_col = value['target column']
        if mapping_df['transformation type'].lower() == 'direct':
            output_df[target_col] = input_df[source_col]
        elif mapping_df['transformation type'].lower() == 'transform':
            # transform data
            pass
        elif mapping_df['transformation type'].lower() == 'static':
            output_df[target_col] = value['transformation config'].get(
                'static_value', '')
    return output_df


input_df = pd.read_parquet('./src/parquet_files/data.parquet')
transform_df(input_df, k)
