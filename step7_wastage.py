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


# 病假
def sick(df: pandas.DataFrame) -> pandas.DataFrame:
    info('正在计算-病假')
    df2 = df[df['病假'] > 0].groupby(['分运营中心', '司龄', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 产假
def maternity(df: pandas.DataFrame):
    info('正在计算-产假')
    df2 = df[df['状态'].str.contains('产假') | df['状态'].str.contains('育儿假') | df['状态'].str.contains('产检假')].groupby(
        ['分运营中心', '项目（通用/非通用）', '司龄', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 待岗
def waiting(df: pandas.DataFrame):
    info('正在计算-待岗')
    df2 = df[df['状态'].str.contains('待岗')].groupby(
        ['分运营中心', '行方级别', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 调休
def rest(df: pandas.DataFrame):
    info('正在计算-调休')
    df2 = df[df['状态'].str.contains('调休')].groupby(
        ['分运营中心', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 婚假
def marriage(df: pandas.DataFrame):
    info('正在计算-婚假')
    df2 = df[df['状态'].str.contains('婚假')].groupby(
        ['分运营中心', '项目（通用/非通用）', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 年假
def annual(df: pandas.DataFrame):
    info('正在计算-年假')
    df2 = df[df['状态'].str.contains('年假')].groupby(
        ['分运营中心', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 培训期
def training(df: pandas.DataFrame):
    info('正在计算-培训期')
    df2 = df[df['状态'].str.contains('培训期')].groupby(
        ['分运营中心', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 事假
def affairs(df: pandas.DataFrame):
    info('正在计算-事假')
    df2 = df[df['状态'].str.contains('事假')].groupby(
        ['分运营中心', '行方级别', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 离职空岗
def dimission(df: pandas.DataFrame):
    info('正在计算-离职空岗')
    df2 = df[df['状态'].str.contains('离职空岗') | df['状态'].str.contains('离职')].groupby(
        ['分运营中心', '状态', '当日产能', '当日应计产能']).size().reset_index().rename(
        columns={0: '数量'})
    df2 = append_loss(df2)
    df2 = total_by_df(df2)
    return df2


# 总表
def summary(df_list: list):
    state_list = ['病假', '产假', '待岗', '调休', '婚假', '年假', '培训期', '事假', '离职']
    temp_df_list = []
    for df in df_list:
        temp_df = df.loc['Total'][['损耗产能(总量)', '数量', '损耗金额(总量)']].to_frame().T
        temp_df = temp_df.reset_index(drop=True)
        temp_df['人天'] = temp_df['损耗产能(总量)'] / temp_df['数量'] * (300000 / 264)
        temp_df['人月'] = temp_df['人天'] * 22
        temp_df['状态'] = state_list.pop(0)
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
    df_sick = sick(df)
    df_maternity = maternity(df)
    df_waiting = waiting(df)
    df_rest = rest(df)
    df_marriage = marriage(df)
    df_annual = annual(df)
    df_training = training(df)
    df_affairs = affairs(df)
    df_dimission = dimission(df)
    #
    df_summary = summary(
        [df_sick, df_maternity, df_waiting, df_rest, df_marriage, df_annual, df_training, df_affairs, df_dimission])
    # 导出到一张excel
    with pd.ExcelWriter(os.path.join(save_path, '产能损耗统计表.xlsx')) as writer:
        df_summary.to_excel(writer, '总表', index=False)
        df_sick.to_excel(writer, '病假', index=False)
        df_maternity.to_excel(writer, '产假', index=False)
        df_waiting.to_excel(writer, '待岗', index=False)
        df_rest.to_excel(writer, '调休', index=False)
        df_marriage.to_excel(writer, '婚假', index=False)
        df_annual.to_excel(writer, '年假', index=False)
        df_training.to_excel(writer, '培训期', index=False)
        df_affairs.to_excel(writer, '事假', index=False)
        df_dimission.to_excel(writer, '离职', index=False)
        writer.save()
    info('产能损失导出完成')


if __name__ == '__main__':
    run()
