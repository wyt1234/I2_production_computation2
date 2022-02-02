import datetime

import pandas as pd
from pathlib import Path
import yaml
from loguru import logger
from tqdm import tqdm
import warnings
import os
from utils.info_util import info

'''按照项目切分'''


def read_config():
    with open('config.yaml', encoding='utf8') as f:
        config = yaml.full_load(f)
    return config


def total_by_df(df):
    df.loc['Total'] = df.sum(axis=0, numeric_only=True)
    return df


def run(dirs=None):
    if not dirs:
        config = read_config()
        dirs = config['save']
    combine_file_path = Path(dirs) / datetime.datetime.now().strftime('%Y年%m月%d日') / '合并' / '基础产能表合并.xlsx'
    info('正在加载合并文件')
    df = pd.read_excel(combine_file_path)
    info('合并文件加载完成')
    pj_info = df['项目'].unique()
    split_dir_path = Path(dirs) / datetime.datetime.now().strftime('%Y年%m月%d日') / '按项目拆分'
    if not split_dir_path.exists():
        info('正在创建目录：', split_dir_path)
        split_dir_path.mkdir()
    for pj in tqdm(pj_info, '按照项目切分：'):
        temp_df = df[df['项目'].isin([pj])]
        temp_df = total_by_df(temp_df)
        temp_file_name = split_dir_path / (str(pj) + '.xlsx')
        temp_df.to_excel(temp_file_name, index=False)
    info('按照项目切分完成')
    return


if __name__ == '__main__':
    run()
