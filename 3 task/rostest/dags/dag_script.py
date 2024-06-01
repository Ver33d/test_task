from clickhouse_driver import Client
import pandas as pd
import json

client = Client(host='localhost', port=9000, user='default', password='pivanet')

query_2 = 'SELECT parameters,timestamp, user, communication_number, communication_id, script_id, script_name, mrf, client_mrf, script_owner, current_script_owner, script_responsible, current_script_responsible, crm_departament from db_test.logg'

result_2 = client.execute(query_2)

df_all = pd.DataFrame(result_2, columns=['parameters','timestamp', 'user', 'communication_number', 'communication_id', 'script_id', 'script_name', 'mrf', 'client_mrf', 'script_owner', 'current_script_owner', 'script_responsible', 'current_script_responsible', 'crm_departament'])

import pandas as pd
import json
import numpy as np
import time


# Функция для преобразования JSON строки в словарь и извлечения нужных полей
def extract_fields(json_str):
    try:
        data_dict = json.loads(json_str)
    except json.JSONDecodeError:
        return {
            'ACCOUNT_NUMBER': '',
            'CALLER_ID': '',
            'COMMUNICATION_THEME': '',
            'COMMUNICATION_DETAIL': '',
            'COMMUNICATION_RESULT': ''
        }
    
    # Извлечение нужных полей
    return {
        'ACCOUNT_NUMBER': data_dict.get('ACCOUNT_NUMBER', ''),
        'CALLER_ID': data_dict.get('CALLER_ID', ''),
        'COMMUNICATION_THEME': data_dict.get('COMMUNICATION_THEME', ''),
        'COMMUNICATION_DETAIL': data_dict.get('COMMUNICATION_DETAIL', ''),
        'COMMUNICATION_RESULT': data_dict.get('COMMUNICATION_RESULT', '')
    }

# Векторизация функции extract_fields
vectorized_extract_fields = np.vectorize(extract_fields)
start_time = time.time()

# Применение векторизованной функции к массиву, полученному из столбца 'parameters'
extracted_data = vectorized_extract_fields(df_all['parameters'].values)

# Преобразование списка словарей в DataFrame
extracted_df = pd.DataFrame(list(extracted_data))
print("Vectorize time:", time.time() - start_time)

df_all = df_all.drop(columns=['parameters'])
df_all = pd.concat([df_all, extracted_df], axis=1)


print(df_all.head())


from clickhouse_driver import Client
import pandas as pd

def determine_clickhouse_type(series):
    if pd.api.types.is_integer_dtype(series):
        return 'Int64'
    elif pd.api.types.is_float_dtype(series):
        return 'Float64'
    elif pd.api.types.is_datetime64_dtype(series):
        return 'Date'
    elif pd.api.types.is_bool_dtype(series):
        return 'UInt8'
    elif pd.api.types.is_object_dtype(series):
        if series.apply(lambda x: isinstance(x, str)).all():
            return 'String'
        else:
            return 'String'
    else:
        return 'String'

def create_table_query(table_name, columns_types, primary_key):
    columns_definition = ', '.join([f'{col} {typ}' for col, typ in columns_types.items()])
    query = f'CREATE TABLE IF NOT EXISTS db_test.{table_name} ({columns_definition}) ENGINE = MergeTree() ORDER BY ({primary_key});'
    return query

clickhouse_types = {col: determine_clickhouse_type(df_all[col]) for col in df_all.columns}

table_name = 'dataset_newww'
primary_key = df_all.columns[0]  
create_query = create_table_query(table_name, clickhouse_types, primary_key)

client.execute(create_query)
print(f"Table {table_name} created successfully")

insert_query = f'INSERT INTO db_test.{table_name} VALUES'
client.execute(insert_query, df_all.to_dict('records'))
print(f"Data uploaded successfully to {table_name}")