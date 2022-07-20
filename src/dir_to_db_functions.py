import os
import re
import shutil
from contextlib import closing

import numpy as np
import pandas as pd
import psycopg2 as ps

from src.list_of_restricted_words import list_of_restricted_words

def get_all_csv_from_dir(path: str) -> list:
    """Return list of all .csv files in specified directory"""
    os.chdir(path)
    only_csv_files = [f for f in os.listdir() if
                      os.path.isfile(os.path.join(os.getcwd(), f)) and f.endswith('csv')]
    return only_csv_files


def create_technical_dir_for_csv(csv_files: list[str], dataset_dir: str) -> str:
    """Create directory where temporary files are placed in

    Args:
        csv_files (list[str]): List of .csv files to be updated to database
        dataset_dir (str): Directory in which temporary folder and files will be created

    Returns:
        str: Absolute path to temporary 'files_to_process' directory
    """
    path = os.path.join(dataset_dir, 'files_to_process')
    # make folder 'files_to_process' in dataset_dir directory
    try:
        os.mkdir(path)
    except Exception as e:
        print(
            f'Making new directory failed due to {e}. Existing directory will be cleared')
    finally:
        # remove all files already existing in directory and copy csv files
        for f in os.listdir(path):
            os.remove(os.path.join(path, f))
        [shutil.copy2(f, path) for f in csv_files]
    return path


def clean_table_names(csv_files: list[str]) -> list[str]:
    """remove restricted special characters from csv_files list"""
    clean_table_names = [re.sub('\W+', '',
                                re.sub(' ', '_', f.replace('.csv', '').lower())) for f in csv_files]
    return clean_table_names


def create_and_clear_df_dict(csv_files: list[str], dataset_dir: str) -> dict:
    """_summary_

    Args:
        csv_files (list[str]): List of .csv files to be updated to database
        dataset_dir (str): Directory in which temporary folder and files will be created

    Returns:
        dict: Dictionary with key allowed to be a table names and pd.DataFrame objects as values 
    """
    df_dict = {}
    file_table_names = list(zip(csv_files, clean_table_names(csv_files)))
    # iterate over list of table names and csv file names and create dictionary
    for i, j in file_table_names:
        try:
            df_dict[j] = pd.read_csv(
                f'{dataset_dir}/files_to_process/'+i, delimiter=',')
        except UnicodeDecodeError:
            df_dict[j] = pd.read_csv(
                f'{dataset_dir}/files_to_process'+i, delimiter=',', encoding='ISO-8859-1')
        # clean column names
        df_dict[j].columns = [
            re.sub('\W+', '', re.sub(' ', '_', f.lower())) for f in df_dict[j].columns]
        df_dict[j].columns = [f if f.upper() not in list_of_restricted_words else f + '_1'
                              for f in df_dict[j].columns]
    return df_dict


def prepare_sql_table_schema(df: pd.DataFrame) -> str:
    """Prepare sql table schema based on pd.DataFrame dtypes"""
    dtypes_mapping = {
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp',
        'timedelta64[ns]': 'varchar'
    }
    col_str = ', '.join(f'{col_name} {col_dtype}' for (
        col_name, col_dtype) in zip(df.columns, df.dtypes.replace(dtypes_mapping)))
    return col_str


def upload_to_db(host: str, port: int, dbname: str, user: str, password: str, tbl_name: str,
                 col_str: str, file_path: str, dataframe: pd.DataFrame) -> None:
    """_summary_

    Args:
        host (str): Hostname
        port (int): Port number
        dbname (str): Database name
        user (str): Username
        password (str): Password
        tbl_name (str): Clean table name (without special characters and not in restricted words list)
        col_str (str): Generated from prepare_sql_table_schema function or user defined table schema
        file_path (str): Path to the temporary .csv file
        dataframe (pd.DataFrame): DataFrame object to upload
    """

    # copy sql statement
    copy_sql = f'''
        COPY {tbl_name} FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS',';
        '''
    # connection to db
    with closing(ps.connect(host=host, port=port,
                            database=dbname, user=user,
                            password=password)) as conn:
        with conn.cursor() as cursor:
            try:
                # remove table if already exist
                cursor.execute(f"DROP TABLE IF EXISTS {tbl_name};")
                # create new table
                cursor.execute(f"CREATE TABLE {tbl_name} ({col_str});")
                # save dataframe as .csv file in termpoarary directory
                dataframe.to_csv(file_path, index=False, header=False)
                # open file and copy data from file to db table
                f = open(file_path, 'r')
                cursor.copy_expert(copy_sql, file=f)
                # grant access to perform select on table
                cursor.execute(f'grant select on table {tbl_name} to public')
                print(f'Table {tbl_name} import to db completed successfullys')
                f.close()
                conn.commit()
            except Exception as e:
                print("Database connection failed due to {}".format(e))


def remove_technical_folder(path: str) -> None:
    """remove temporary directory and child directories recursive"""
    shutil.rmtree(path)
