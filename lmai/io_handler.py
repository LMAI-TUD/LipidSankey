import platform
import os
import time

import pandas as pd


def get_file_list(path):
    return os.listdir(path)


def extend_relative_path(file_path: str):
    rel_path_lst = []
    # Determine the OS
    os_name = platform.system()
    rel_path_check_lst = [f'../', f'../../', f'../../../', f'../../../../']

    if os.path.isabs(file_path):
        rel_path_lst = [file_path]
        print(f'received abs file path {file_path}')
    else:
        if os_name == "Windows":
            user_folder = os.path.expanduser("~\\Documents")
        else:
            user_folder = os.path.expanduser("~/")

        rel_path_lst = [os.path.join(parent_folder, file_path) for parent_folder in rel_path_check_lst]
        if file_path.startswith('../'):
            # remove all ../
            no_parent_path = file_path[3:]
            rel_path_lst = [no_parent_path, file_path] + rel_path_lst
        else:
            rel_path_lst = [file_path] + rel_path_lst
        rel_path_lst.append(os.path.join(user_folder, file_path))
    return rel_path_lst


def check_abs_file_path(file_path: str):
    rel_path_lst = extend_relative_path(file_path)
    abs_file_path = None
    for rel_path in rel_path_lst:
        if os.path.isfile(rel_path):
            abs_file_path = os.path.abspath(rel_path)
            print(f'File found: {abs_file_path}')
            break
    return abs_file_path


def get_input_file(file_path: str):
    # init variables

    abs_file_path = check_abs_file_path(file_path)
    file_name_str = os.path.basename(abs_file_path)
    file_type_str = file_name_str.split('.')[-1]

    return abs_file_path, file_name_str, file_type_str


def get_df_from_file(file_path: str, index_col: int = None, header: int = 0, sep: str = ','):
    abs_file_path, file_name_str, file_type_str = get_input_file(file_path)
    df = None
    if file_type_str == 'csv':
        df = pd.read_csv(abs_file_path, index_col=index_col, header=header, sep=sep)
    elif file_type_str == 'xlsx':
        df = pd.read_excel(abs_file_path, index_col=index_col, header=header)
    else:
        raise ValueError(f'File type {file_type_str} not supported')
    return df


def save_output_file(dataframe: pd.DataFrame, file_path: str, keep_index: bool = False, overwrite: bool = False):
    # check if file_path exists a file
    save_abs_path = None
    save_type_str = None
    save_abs_folder = None
    if os.path.isfile(file_path):
        abs_file_path = os.path.abspath(file_path)
        save_abs_folder = os.path.dirname(abs_file_path)
        file_name_str = os.path.basename(abs_file_path)
        save_type_str = file_name_str.split('.')[-1]
        # generate time tag for file name
        if overwrite:
            pass
        else:
            time_tag = time.strftime('%Y%m%d%H%M%S')
            file_name_str = f'{file_name_str.split(".")[0]}_{time_tag}.{save_type_str}'
        save_abs_path = os.path.join(os.path.dirname(abs_file_path), file_name_str)
        print(f'File {abs_file_path} exists, overwrite set as {overwrite},save file as {save_abs_path}')
    else:
        save_abs_path = os.path.abspath(file_path)
        save_abs_folder = os.path.dirname(save_abs_path)
        save_type_str = file_path.split('.')[-1]

    # check if save_abs_folder exists
    if save_abs_folder and os.path.isdir(save_abs_folder):
        print(f'! Folder exists: {save_abs_folder}')
    else:
        os.makedirs(save_abs_folder)
        print(f'! Folder created: {save_abs_folder}')

    # save file by file types
    if save_abs_path and save_type_str and save_abs_folder:
        if save_type_str == 'csv':
            dataframe.to_csv(save_abs_path, index=keep_index)
        elif save_type_str == 'xlsx':
            dataframe.to_excel(save_abs_path, index=keep_index)
        else:
            print(f'! File type {save_type_str} not supported, save as csv')
            save_abs_path = f'{save_abs_path}.csv'
            dataframe.to_csv(save_abs_path, index=keep_index)
            print(f'! File saved as {save_abs_path}')
    else:
        print(f'! File path not found: {file_path}')

    return save_abs_path
