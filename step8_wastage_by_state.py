import datetime

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


# 总表
def summary(df_list: list, state_list: list):
    temp_df_list = []
    for i, df in enumerate(df_list):
        temp_df = df.loc['Total'][['损耗产能(总量)', '数量', '损耗金额(总量)']].to_frame().T
        temp_df = temp_df.reset_index(drop=True)
        temp_df['人天'] = temp_df['损耗产能(总量)'] / temp_df['数量'] * (300000 / 264)
        temp_df['人天'] = temp_df['人天'].astype('float32').round(2)
        temp_df['人月'] = temp_df['人天'] / 22
        temp_df['人月'] = temp_df['人月'].astype('float32').round(2)
        temp_df['状态'] = state_list[i]
        temp_df[['损耗产能(总量)', '损耗金额(总量)']] = temp_df[['损耗产能(总量)', '损耗金额(总量)']].astype('float32').round(2)
        temp_df_list.append(temp_df)
    df_summary = pd.concat(temp_df_list)
    return df_summary


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
        # 导出到一张excel
        df_summary = summary(df_list, state_list)
        with pd.ExcelWriter(os.path.join(save_path, f'产能损耗统计表{y_m}.xlsx')) as writer:
            df_summary.to_excel(writer, '总表', index=False)
            for df2 in df_list:
                df2.to_excel(writer, df2['状态'][0], index=False)
            writer.save()

    info('产能损失导出完成')


if __name__ == '__main__':
    run()
