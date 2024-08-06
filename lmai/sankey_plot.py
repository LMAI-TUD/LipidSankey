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
import json

import pandas as pd
import plotly.graph_objects as go
from natsort import natsorted
from plotly import express as px


def assign_plotly_colors(unique_node_lst):
    if len(unique_node_lst) >= 100:
        print("Can not assign color to more than 100 different nodes automatically, please assign colors manually.")
        # assign blue color to all nodes
        color_lst = ["grey"] * len(unique_node_lst)
    else:
        # Get the Plotly color palette
        color_palette = px.colors.qualitative.Plotly

        # Assign colors to the unique nodes
        color_lst = [color_palette[i % len(color_palette)] for i in range(len(unique_node_lst))]

    return color_lst


def format_sankey_data(df, link_json_path: str, color_map_path: str,
                       source_col: str = 'source_label',
                       target_col: str = 'target_label',
                       value_col: str = 'value'):
    source_lst = df[source_col].unique().tolist()
    target_lst = df[target_col].unique().tolist()
    unique_node_lst = natsorted(list(set(source_lst + target_lst)))
    source_idx_dct = {}
    for s_node in source_lst:
        source_idx_dct[s_node] = unique_node_lst.index(s_node)
    target_idx_dct = {}
    for t_node in target_lst:
        target_idx_dct[t_node] = unique_node_lst.index(t_node)

    # add the source and target index to the dataframe
    df["source_idx"] = df[source_col].map(source_idx_dct)
    df["target_idx"] = df[target_col].map(target_idx_dct)

    source_idx_lst = df["source_idx"].tolist()
    target_idx_lst = df["target_idx"].tolist()
    link_val_lst = df[value_col].tolist()

    # generate unique color for unique nodes from the color palette
    color_lst = assign_plotly_colors(unique_node_lst)
    generated_color_df = pd.DataFrame(data={"label": unique_node_lst, "color": color_lst})
    generated_color_df.to_csv(color_map_path, index=False)

    # construct the link json
    link_json = {"node": unique_node_lst, "source": source_idx_lst, "target": target_idx_lst, "value": link_val_lst}
    with open(link_json_path, "w") as json_obj:
        json.dump(link_json, json_obj)

    return link_json, generated_color_df


def plot_sankey(link_json, color_cfg_path, output_path: str, width: int = 1600, height: int = 1000,
                thickness: int = 100):
    with open(link_json, "r") as json_obj:
        link_json = json.load(json_obj)
    if link_json:
        unique_node_lst = link_json["node"]
        source_idx_lst = link_json["source"]
        target_idx_lst = link_json["target"]
        link_val_lst = link_json["value"]

        color_cfg = pd.read_csv(color_cfg_path)
        if not color_cfg.empty:
            if "label" not in color_cfg.columns or "color" not in color_cfg.columns:
                raise ValueError("Color configuration file should have columns 'label' and 'color'")
            else:
                color_dct = dict(zip(color_cfg["label"], color_cfg["color"]))

                color_lst = [color_dct.get(node, "grey") for node in unique_node_lst]
        else:
            color_lst = assign_plotly_colors(unique_node_lst)

        fig = go.Figure(
            data=[
                go.Sankey(
                    arrangement="snap",
                    node={
                        "label": unique_node_lst,
                        "color": color_lst,
                        "pad": 15,
                        "thickness": thickness,
                    },
                    link=dict(
                        source=source_idx_lst,
                        target=target_idx_lst,
                        value=link_val_lst,
                    ),
                )
            ]
        )

        fig.update_layout(title_text="Plot", font_size=10)

        js = fig.to_json()
        f_name = "Sankey_Cer_colors"
        with open(f"{output_path}.json", "w") as jsf:
            jsf.write(js)
        fig.write_image(f"{output_path}.png", width=width, height=height)
        fig.write_image(f"{output_path}.svg", width=width, height=height)
        print(f"Sankey plot saved to {output_path}.png and {output_path}.svg")
        fig.show()
        # save plotly as html
        fig.write_html(f"{output_path}.html")


def reload_sankey_json(json_path: str, output_path: str, width: int = 1600, height: int = 1000):
    with open(json_path, "r") as json_obj:
        fig = go.Figure(data=json.load(json_obj))
        fig.show()

    if output_path:
        fig.write_image(f"{output_path}.png", width=width, height=height)
        fig.write_image(f"{output_path}.svg", width=width, height=height)
        fig.write_html(f"{output_path}.html")
        print(f"Sankey plot saved to {output_path}.png and {output_path}.svg")
    else:
        pass
