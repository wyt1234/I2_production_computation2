import datetime

import pandas as pd
from pathlib import Path
import yaml
from loguru import logger
from tqdm import tqdm
import warnings
import os
from utils.info_util import info

'''按照项目与月切分'''


def read_config():
    with open('config.yaml', encoding='utf8') as f:
        config = yaml.full_load(f)
    return config


def total_by_df(df):
    df.loc['Total'] = df.sum(axis=0, numeric_only=True)
    return df


# 简化日期为xxxx-xx
def shorter_datetime64(df):
    df['日期'] = df['日期'].astype('str')
    df['日期'] = df['日期'].apply(lambda x: str(x)[:10])
    return df


def run(dirs=None):
    if not dirs:
        config = read_config()
        dirs = config['save']
    combine_file_path = Path(dirs) / datetime.datetime.now().strftime('%Y年%m月%d日') / '合并' / '基础产能表合并.xlsx'
    info('正在加载合并文件...')
    df = pd.read_excel(combine_file_path)
    info('合并文件加载完成')
    pj_info = df['项目'].unique()
    split_dir_path = Path(dirs) / datetime.datetime.now().strftime('%Y年%m月%d日') / '按项目与月拆分'
    if not split_dir_path.exists():
        split_dir_path.mkdir()
    for pj in tqdm(pj_info, '按照项目与月切分：'):
        temp_df = df[df['项目'].isin([pj])]
        temp_df['日期'] = temp_df['日期'].astype('datetime64')
        monthly_unique = temp_df['日期'].dt.month.unique()
        for month in monthly_unique:
            t_df = temp_df[temp_df['日期'].dt.month == month]
            t_df = shorter_datetime64(t_df)
            t_df = total_by_df(t_df)
            temp_file_name = split_dir_path / (str(pj) + '_' + str(month) + '月' + '.xlsx')
            t_df.to_excel(temp_file_name, index=False)
    info('按照项目与月切分完成')
    return


if __name__ == '__main__':
    run()
