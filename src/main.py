import os
import re
import shutil

import numpy as np
import pandas as pd
import psycopg2 as ps
from gooey import Gooey, GooeyParser

from dir_to_db_functions import *


@Gooey(program_name="Import csv's to database", tabbed_groups=True, advanced=True)
def parse_args():
    parser = GooeyParser(description='Load file from csv to db')

    dirs = parser.add_argument_group(
        title='Directories', description='Choose source directory and temporary folder directory')
    dirs.add_argument('source_dir', widget='DirChooser',
                      help='Choose source files directory')
    dirs.add_argument('temp_dir', widget='DirChooser',
                      help='Choose temporary directory')

    db_conn = parser.add_argument_group(
        title='DB_Connection', description='Specify database connection parameters')
    db_conn.add_argument('Host', help='Specify database host')
    db_conn.add_argument('Port')
    db_conn.add_argument('Database', help='Enter database name')
    db_conn.add_argument('Username')
    db_conn.add_argument('Password', widget='PasswordField')

    return parser.parse_args()


args = parse_args()


def main():
    try:
        only_csv_files = get_all_csv_from_dir(args.source_dir)
        print('Files to upload: '+', '.join(only_csv_files))
        path_to_remove_after_upload = create_technical_dir_for_csv(
            csv_files=only_csv_files, dataset_dir=args.temp_dir)
        print(f'Temporary directory: {path_to_remove_after_upload} has been created')
        table_names = clean_table_names(only_csv_files)
        df_dict = create_and_clear_df_dict(
            csv_files=only_csv_files, dataset_dir=args.temp_dir)
        for k in table_names:
            dataframe = df_dict[k]
            col_str = prepare_sql_table_schema(dataframe)
            upload_to_db(host=args.Host,
                         port=args.Port,
                         dbname=args.Database,
                         user=args.Username,
                         password=args.Password,
                         tbl_name=k,
                         col_str=col_str,
                         file_path=f'{args.temp_dir}/files_to_process/upload_data_from_copy.csv',
                         dataframe=dataframe)
    except Exception as e:
        print(e)

    finally:
        remove_technical_folder(path_to_remove_after_upload)
        print('Temporary directory removed. Process end.')


if __name__ == '__main__':
    main()
