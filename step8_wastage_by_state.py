import datetime
from typing import List

import pandas
import pandas as pd
from pathlib import Path
import yaml
from loguru import logger
from tqdm import tqdm
import warnings
import os
import threading
from utils.info_util import info

# 忽略警告
warnings.filterwarnings('ignore')


def read_config():
    with open('config.yaml', encoding='utf8') as f:
        config = yaml.full_load(f)
    return config


# 计算单表损耗
def append_loss(df2: pandas.DataFrame) -> pandas.DataFrame:
    df2['损耗产能(单量)'] = df2['当日应计产能'] - df2['当日产能']
    df2['损耗产能(总量)'] = (df2['当日应计产能'] - df2['当日产能']) * df2['数量']
    df2['损耗金额(总量)'] = df2['损耗产能(单量)'] / 264 * 300000 * df2['数量']
    return df2


# 计算单表Total
def total_by_df(df):
    df.loc['Total'] = df.sum(axis=0, numeric_only=True)
    return df


# 统一美化格式 输出两位小数
def good_format(df2: pandas.DataFrame, cols: List = None):
    if not cols:
        cols = ['损耗产能(单量)', '损耗产能(总量)', '损耗金额(总量)', '人天', '人月']
    for col in cols:
        if col in df2.columns.tolist():
            df2[col] = df2[col].astype('float64').round(2).apply(lambda x: '{:.2f}'.format(x))
    return df2


# 总表
def summary(df_list: list, state_list: list):
    temp_df_list = []
    for i, df in enumerate(df_list):
        temp_df = df.loc['Total'][['损耗产能(总量)', '数量', '损耗金额(总量)']].to_frame().T
        temp_df = temp_df.reset_index(drop=True)
        temp_df['人天'] = temp_df['损耗产能(总量)'] / temp_df['数量'] * (300000 / 264)
        temp_df['人月'] = temp_df['人天'] / 22
        temp_df['状态'] = state_list[i]
        temp_df_list.append(temp_df)
    df_summary = pd.concat(temp_df_list)
    return df_summary


# 各个分运营中心横向统计
def transverse(df_list: List[pandas.DataFrame], center_name_list: List, state_list: List):
    dic_list = []
    for i, center in enumerate(center_name_list):
        d = {}
        d['服务提供者部门'] = center
        for j, state in enumerate(state_list):
            df2 = df_list[j]
            df3_amt_sum = df2[df2['分运营中心'] == center]['损耗产能(总量)'].sum()
            d[state] = df3_amt_sum
        dic_list.append(d)
    df_transverse = pd.DataFrame(dic_list)
    df_transverse = good_format(df_transverse, state_list)
    return df_transverse


def run(wastage_from=None, save_path=None):
    if not wastage_from:
        config = read_config()
        wastage_from = config['wastage_from']
    if not save_path:
        config = read_config()
        save_path = config['wastage_to']
    info('正在加载文件...')
    df = pd.read_excel(wastage_from)
    df['日期'] = df['日期'].astype('datetime64')
    df['状态'] = df['状态'].astype('str')
    df['分运营中心'] = df['分运营中心'].astype('str')
    df['司龄'] = df['司龄'].astype('str')
    df['病假'] = df['病假'].astype('float32')
    df['当日产能'] = df['当日产能'].astype('float32')
    df['当日应计产能'] = df['当日应计产能'].astype('float32')
    info('文件加载完成')
    #
    year_month_key = lambda x: f"{x.year}-{x.month}"
    year_month_list = df['日期'].apply(year_month_key).unique().tolist()
    for y_m in year_month_list:
        df_list = []
        df2 = df[df['日期'].apply(lambda x: f"{x.year}-{x.month}" == y_m)]
        state_list = df2['状态'].unique().tolist()
        state_list.sort()
        for state in state_list:
            df_temp = df2[df2['状态'] == state]
            df_temp = df_temp.groupby(['分运营中心',
                                       '项目（通用/非通用）',
                                       '行方级别',
                                       '司龄',
                                       '状态',
                                       '当日产能',
                                       '当日应计产能']).size().reset_index().rename(columns={0: '数量'})
            df_temp = append_loss(df_temp)
            df_temp = total_by_df(df_temp)
            df_list.append(df_temp)
        # 总表
        df_summary = summary(df_list, state_list)
        # 各运营中心横向统计
        center_name_list = df['分运营中心'].unique().tolist()
        center_name_list.sort()
        df_transverse = transverse(df_list, center_name_list, state_list)
        # 导出到一张excel
        xlsx_path = Path(os.path.join(save_path, f'产能损耗统计表{y_m}.xlsx'))
        with pd.ExcelWriter(xlsx_path, engine='xlsxwriter') as writer:
            good_format(df_summary).to_excel(writer, '总表', index=False)
            good_format(df_transverse).to_excel(writer, '分运营中心', index=False)
            for df2 in df_list:
                good_format(df2).to_excel(writer, df2['状态'][0], index=False)
            writer.save()

    info('产能损失导出完成')


if __name__ == '__main__':
    run()
