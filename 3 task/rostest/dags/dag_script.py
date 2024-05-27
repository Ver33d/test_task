from clickhouse_driver import Client
import pandas as pd
import json

client = Client(host='clickhouse-server_2', port=9000, user='default', password='pivanet')

query = 'SELECT parameters, script_id from db_test.logss'
result = client.execute(query)

df = pd.DataFrame(result, columns=['parameters', 'script_id'])

def expand_interaction_topics(df, column_name='parameters', id_column='script_id'):
    # Функция для преобразования JSON строки в словарь
    def parse_json(x):
        try:
            return json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return x  # Возвращаем исходное значение, если это уже словарь или если ошибка
    
    # Применяем функцию парсинга
    df[column_name] = df[column_name].apply(parse_json)
    
    # Список для хранения DataFrame для каждой строки
    df_list = []
    
    for index, row in df.iterrows():
        # Нормализуем вложенные данные из 'INTERACTION_TOPICS'
        if 'INTERACTION_TOPICS' in row[column_name]:
            topics_df = pd.json_normalize(row[column_name], record_path='INTERACTION_TOPICS',
                                          meta=[k for k in row[column_name].keys() if k != 'INTERACTION_TOPICS' and k != id_column] + [id_column],
                                          errors='ignore')
            # Добавляем обработанные данные в список
            df_list.append(topics_df)
        else:
            # Если 'INTERACTION_TOPICS' отсутствует, добавляем строку без изменений
            other_data_df = pd.DataFrame([row[column_name]], index=[index])
            other_data_df[id_column] = row[id_column]  # Добавляем script_id в DataFrame
            df_list.append(other_data_df)
    
    # Конкатенация всех DataFrame в один
    final_df = pd.concat(df_list, ignore_index=True)
    
    return final_df

expanded_df = expand_interaction_topics(df)

def merge_dicts(dicts):
    result_dict = {}
    for dictionary in dicts:
        if isinstance(dictionary, dict):  # Убедимся, что элемент является словарем
            for key, value in dictionary.items():
                result_dict[key] = value  # Обновление значения для ключа
    return result_dict


# Группировка по script_id и агрегация словарей в parameters
aggregated_df = df.groupby('script_id')['parameters'].agg(merge_dicts).reset_index()

import pandas as pd
from pandas import json_normalize

def recursive_normalize(data, prefix=''):
    # Проверяем, является ли входящий элемент словарем
    if isinstance(data, dict):
        # Нормализация словаря на текущем уровне
        df = json_normalize(data)
    elif isinstance(data, list) and all(isinstance(x, dict) for x in data):
        # Если элемент - список словарей, нормализуем каждый словарь отдельно
        df = pd.concat([recursive_normalize(item, prefix) for item in data], ignore_index=True)
    else:
        # Если это не словарь и не список словарей, возвращаем пустой DataFrame
        return pd.DataFrame()

    # Обновляем имена колонок, добавляя префикс
    df.columns = [f'{prefix}{col}' if prefix else col for col in df.columns]
    return df

def expand_nested_columns(df, column):
    # Применяем рекурсивную функцию к каждой строке данных в заданной колонке
    expanded_frames = [recursive_normalize(row) for row in df[column]]
    expanded_df = pd.concat(expanded_frames, ignore_index=True)
    
    # Объединяем с исходным DataFrame по индексу
    final_df = df.drop(columns=[column]).join(expanded_df)
    return final_df

result_dff = expand_nested_columns(aggregated_df, 'parameters')

def find_dict_columns(df):
    dict_columns = []
    for col in df.columns:
        # Проверяем, является ли хотя бы один элемент в колонке словарем
        if any(isinstance(cell, dict) for cell in df[col]):
            dict_columns.append(col)
    return dict_columns
def find_list_columns(df):
    list_columns = []
    for col in df.columns:
        # Проверяем, является ли хотя бы один элемент в колонке списком
        if any(isinstance(cell, list) for cell in df[col]):
            list_columns.append(col)
    return list_columns

def expand_dict_columns(df, dict_columns):
    for col in dict_columns:
        # Расширяем каждую колонку, содержащую словари
        expanded_data = df[col].apply(pd.Series)
        # Создаем новые имена колонок, добавляя префикс
        expanded_data.columns = [f'{col}.{subcol}' for subcol in expanded_data.columns]
        # Удаляем исходную колонку из DataFrame
        df = df.drop(col, axis=1)
        # Добавляем новые колонки в DataFrame
        df = pd.concat([df, expanded_data], axis=1)
    return df

dict_columns = find_list_columns(result_dff)
def extract_first_list_element(df, list_columns):
    # Применяем изменения только к колонкам в list_columns
    for col in list_columns:
        df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) and x else None)
    return df
# Найдем колонки, содержащие списки
list_columns = find_list_columns(result_dff)

# Применяем функцию для извлечения первого элемента из списков в этих колонках
df = extract_first_list_element(result_dff, list_columns)

# Рекурсивно проходимся по данным: убираем словари и списки
import pandas as pd

def find_dict_columns(df):
    dict_columns = []
    for col in df.columns:
        if any(isinstance(cell, dict) for cell in df[col]):
            dict_columns.append(col)
    return dict_columns

def find_list_columns(df):
    list_columns = []
    for col in df.columns:
        if any(isinstance(cell, list) for cell in df[col]):
            list_columns.append(col)
    return list_columns

def expand_dict_columns(df, dict_columns):
    for col in dict_columns:
        expanded_data = df[col].apply(pd.Series)
        expanded_data.columns = [f'{col}.{subcol}' for subcol in expanded_data.columns]
        df = df.drop(col, axis=1)
        df = pd.concat([df, expanded_data], axis=1)
    return df

def extract_first_list_item(df, list_columns):
    updated_list_columns = []
    original_data = df[list_columns].copy()  # Сохраняем оригинальные данные для сравнения

    for col in list_columns:
        df[col] = df[col].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 and not isinstance(x[0], list) and (not isinstance(x[0], dict) or (isinstance(x[0], dict) and x[0])) else x
        )
        if not df[col].equals(original_data[col]) and df[col].notna().any():
            updated_list_columns.append(col)
        print(f"Column {col} after processing lists, skipping empty dicts:\n{df[col].head()}")

    return df, updated_list_columns, original_data

def process_dataframe(df, depth=0, max_depth=None):
    indent = "  " * depth
    print(f"{indent}Call to process_dataframe at depth {depth}")

    if max_depth is not None and depth >= max_depth:
        print(f"{indent}Reached max recursion depth of {max_depth}. Exiting recursion.")
        return df

    list_columns = find_list_columns(df)
    if list_columns:
        print(f"{indent}Found lists in columns: {list_columns}")
        df, updated_list_columns, original_data = extract_first_list_item(df, list_columns)
        if updated_list_columns:
            print(f"{indent}Processed lists, recursing...")
            df = process_dataframe(df, depth + 1, max_depth)
        # Восстанавливаем колонки с неизмененными данными
        for col in list_columns:
            if col not in updated_list_columns:
                df[col] = original_data[col]
        return df

    dict_columns = find_dict_columns(df)
    if dict_columns:
        print(f"{indent}Found dictionaries in columns: {dict_columns}")
        df = expand_dict_columns(df, dict_columns)
        print(f"{indent}Expanded dictionaries, recursing...")
        df = process_dataframe(df, depth + 1, max_depth)
        return df

    print(f"{indent}No more lists or dictionaries to process. Exiting recursion.")
    return df

df = result_dff

result_ddf = process_dataframe(df)

# Функция для очистки имен столбцов
import re
result_data = result_ddf
def clean_column_names(columns):
    cleaned_columns = {}
    for col in columns:
        cleaned_col = re.sub(r'[^a-zA-Z0-9_]', '', col)
        cleaned_columns[col] = cleaned_col
    return cleaned_columns

# Применение функции для очистки имен столбцов в DataFrame
cleaned_column_names = clean_column_names(result_data.columns)
result_data.rename(columns=cleaned_column_names, inplace=True)

final_dataset = result_data.copy() # делаю копию датафрейма

object_columns = final_dataset.select_dtypes(include=['object']).columns.tolist()

# Преобразование колонок типа object в строки
for col in object_columns:
    final_dataset[col] = final_dataset[col].astype(str)


from clickhouse_driver import Client

client = Client(host='clickhouse-server_2', port=9000, user='default', password='pivanet')

# Подготовка данных для вставки
data_tuples = [tuple(x) for x in final_dataset.to_numpy()]
columns = ', '.join(final_dataset.columns)

# Запрос для удаления существующих данных в таблице
delete_query = 'TRUNCATE TABLE db_test.dataset_new'

# Запрос для вставки данных
insert_query = f'INSERT INTO db_test.dataset_new ({columns}) VALUES'

try:
    # Удаляем существующие данные
    client.execute(delete_query)
    print("Existing data deleted successfully.")
    
    # Вставляем новые данные
    client.execute(insert_query, data_tuples)
    print("Data inserted into db_test.dataset_new successfully.")
except Exception as e:
    print("Ошибка при обработке данных:", e)