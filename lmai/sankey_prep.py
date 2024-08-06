# -*- coding: utf-8 -*-
#
# Copyright (C) 2021-2024  LMAI_team @ TU Dresden:
#     LMAI_team: Zhixu Ni, Maria Fedorova
#
# Licensing:
# This code is licensed under AGPL-3.0 license (Affero General Public License v3.0).
# For more information, please read:
#     AGPL-3.0 License: https://www.gnu.org/licenses/agpl-3.0.en.html
#
# Citation:
# Please cite our publication in an appropriate form.
#
# For more information, please contact:
#     Fedorova Lab (#LMAI_team): https://fedorovalab.net/
#     LMAI on Github: https://github.com/LMAI-TUD
#
import pandas as pd

from lmai.io_handler import get_df_from_file


def check_col_names(df, col_names: dict):
    is_col_name_checked = False
    # check if the columns are in the dataframe
    err_lst = []
    for col_typ, col_name in col_names.items():
        if col_name not in df.columns:
            err_lst.append(f"{col_typ}: Column name {col_name} not found in the input file")

    if len(err_lst) > 0:
        print(f"{'\n'.join(err_lst)}")
        raise ValueError(f"{'\n'.join(err_lst)}")
    else:
        is_col_name_checked = True
    return is_col_name_checked


def get_column_group_sum(df, group_by_col_name, group_val_col_name, other_class_threshold=2.0):
    # group by start column
    tmp_df = df.copy()
    tmp_df = tmp_df[[group_by_col_name, group_val_col_name, 'ratio']]
    # group by start column and sum the values
    sum_group_df = tmp_df.groupby(group_by_col_name).sum()
    # filter the group by the threshold
    main_df = sum_group_df[sum_group_df["ratio"] >= other_class_threshold]
    other_df = sum_group_df[sum_group_df["ratio"] < other_class_threshold]
    # sum the other group
    other_val_sum = other_df[group_val_col_name].sum()
    other_ratio_sum = other_df["ratio"].sum()
    # Append the other group to the group_df
    other_df = pd.DataFrame.from_dict(data={"Others": {group_val_col_name: other_val_sum, "ratio": other_ratio_sum}},
                                      orient='index')
    result_df = pd.concat([main_df, other_df])

    # add a new column for the label in format f"{group_by_col_name} ({ratio}%)"
    result_df["label"] = result_df.apply(lambda x: f"{x.name} ({x['ratio']:.1f}%)", axis=1)

    return result_df


def get_flow_values(df, source_col_name, target_col_name, val_col_name):
    # get the sum of each group
    # sum_start_df = get_column_group_sum(df, start_col_name, val_col_name)
    # sum_mid_df = get_column_group_sum(df, mid_col_name, val_col_name)
    # sum_end_df = get_column_group_sum(df, end_col_name, val_col_name)

    # return sum_start_df, sum_mid_df, sum_end_df
    pass


def map_group_to_label(df, source_col_name, target_col_name, val_col_name, source_df, target_df):
    # map the group to the label
    source_node_info = source_df[["label", 'ratio']].to_dict(orient='index')
    target_node_info = target_df[["label", 'ratio']].to_dict(orient='index')
    # construct a new dict for dataframe
    mapped_dct = {}
    idx_counter = 1
    for idx, row in df.iterrows():
        source_node_name = row[source_col_name]
        target_node_name = row[target_col_name]
        if source_node_name in source_node_info:
            source_node_label = source_node_info[source_node_name]["label"]
        else:
            source_node_label = source_node_info['Others']["label"]
        if target_node_name in target_node_info:
            target_node_label = target_node_info[target_node_name]["label"]
        else:
            target_node_label = target_node_info['Others']["label"]
        mapped_dct[f'{source_node_label}_{target_node_label}_{idx_counter}'] = {
            "lipid": idx,
            "source_label": source_node_label,
            "target_label": target_node_label,
            "source_node": source_node_name,
            "target_node": target_node_name,
            "value": row[val_col_name]
        }
        idx_counter += 1
    mapped_df = pd.DataFrame.from_dict(mapped_dct, orient='index')
    # calculate the ratio of the value
    sum_val_num = mapped_df["value"].sum()
    mapped_df["ratio"] = 100 * mapped_df["value"] / sum_val_num
    # group by the source_label and target_label
    tmp_df = mapped_df.copy()
    tmp_df = tmp_df[["source_label", "target_label", "value", "ratio"]]
    grouped_df = tmp_df.groupby(["source_label", "target_label"]).sum()

    # generate a dict for the grouped dataframe
    grouped_dct = grouped_df.to_dict(orient='index')
    grouped_df.reset_index(inplace=True)
    grouped_df.rename(columns={'level_0': 'source_label', 'level_1': 'target_label'}, inplace=True)
    grouped_df.sort_values(by=["source_label", "target_label"], inplace=True, ascending=[True, True])

    return mapped_df, grouped_df, grouped_dct


def get_sankey_data(input_file, start_col_name: str, mid_col_name: str, end_col_name: str, val_col_name: str, lipid_index_col_name: str,
                    other_class_threshold_info: dict = None):
    raw_data_df = get_df_from_file(input_file)
    if lipid_index_col_name in raw_data_df.columns:
        raw_data_df.set_index(lipid_index_col_name, inplace=True, drop=True)

    col_names_info = {
        "start_column": start_col_name,
        "mid_column": mid_col_name,
        "end_column": end_col_name,
        "value_column": val_col_name
    }
    is_col_name_checked = check_col_names(raw_data_df, col_names_info)
    if is_col_name_checked:
        sum_val_num = raw_data_df[val_col_name].sum()
        # calc the percent of each value as percentage
        raw_data_df["ratio"] = 100 * raw_data_df[val_col_name] / sum_val_num

        # check if the sum of ratio is 100
        sum_ratio = raw_data_df["ratio"].sum()
        if sum_ratio > 100.5 or sum_ratio < 99.5:
            raise ValueError(f"Sum of ratio is not 100, but {sum_ratio}")

        # set the threshold for the other class
        if isinstance(other_class_threshold_info, dict) and other_class_threshold_info:
            if start_col_name in other_class_threshold_info:
                start_other_class_threshold = other_class_threshold_info[start_col_name]
            else:
                start_other_class_threshold = 1.0
            if mid_col_name in other_class_threshold_info:
                mid_other_class_threshold = other_class_threshold_info[mid_col_name]
            else:
                mid_other_class_threshold = 1.0
            if end_col_name in other_class_threshold_info:
                end_other_class_threshold = other_class_threshold_info[end_col_name]
            else:
                end_other_class_threshold = 1.0
        else:
            start_other_class_threshold = 1.0
            mid_other_class_threshold = 1.0
            end_other_class_threshold = 1.0

        sum_start_df = get_column_group_sum(raw_data_df, start_col_name, val_col_name, start_other_class_threshold)
        sum_mid_df = get_column_group_sum(raw_data_df, mid_col_name, val_col_name, mid_other_class_threshold)
        sum_end_df = get_column_group_sum(raw_data_df, end_col_name, val_col_name, end_other_class_threshold)

        # print(f"Start column: {start_col_name}")
        # print(sum_start_df)
        # print(f"Mid column: {mid_col_name}")
        # print(sum_mid_df)
        # print(f"End column: {end_col_name}")
        # print(sum_end_df)

        # map the group to the label
        start_mid_df, start_mid_grouped_df, start_mid_grouped_dct = map_group_to_label(raw_data_df, start_col_name,
                                                                                       mid_col_name, val_col_name,
                                                                                       sum_start_df, sum_mid_df)
        mid_end_df, mid_end_grouped_df, mid_end_grouped_dct = map_group_to_label(raw_data_df, mid_col_name,
                                                                                 end_col_name, val_col_name,
                                                                                 sum_mid_df, sum_end_df)

        # concat the mapped dataframes
        mapped_df = pd.concat([start_mid_df, mid_end_df], ignore_index=True)
        mapped_df.reset_index(drop=True, inplace=True)

        # concat the dataframes for the sankey plot ignore and reset the index
        sankey_df = pd.concat([start_mid_grouped_df, mid_end_grouped_df], ignore_index=True)
        sankey_df.reset_index(drop=True, inplace=True)
        # print(sankey_df.head())
        # print(mapped_df.head())
        # # print mapped_df first row as series
        # print(mapped_df.iloc[0])

        return sankey_df, mapped_df
