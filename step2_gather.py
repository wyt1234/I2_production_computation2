import datetime

import pandas as pd
from pathlib import Path
import yaml
from loguru import logger
from tqdm import tqdm
import warnings
import os
import threading

# 忽略警告
warnings.filterwarnings('ignore')


def info(*args):
    print(''.join([str(arg) for arg in args]))


def run_fun_in_new_thread(f, args=()):
    threading.Thread(target=f, args=args).start()


def read_config():
    with open('config.yaml', encoding='utf8') as f:
        config = yaml.full_load(f)
    return config


def all_files_path(dirs):
    if isinstance(dirs, str):
        dirs = [dirs]
    all_files = []
    for dir in dirs:
        path_list = [p for p in Path(dir).glob('*产能基础表*.xlsx')]
        all_files.extend(path_list)
    return all_files


def run(dirs=None, save_path=None):
    if not dirs:
        config = read_config()
        dirs = config['dirs']
    all_files = all_files_path(dirs)
    cols = ['姓名', '项目', '日期', '状态', '当日产能', '当日应计产能', '当日招聘人数', '当日离职绩效人数', '当日应计绩效人天', '当日考勤绩效人天', '当日扣减绩效人天']
    dflist = []
    fail_file = []
    for fi in tqdm(all_files, '正在读取文件：'):
        try:
            df = pd.read_excel(fi, sheet_name='模板表', usecols=cols)
            df = df[df['日期'].notna()]
            df['项目'] = df['项目'].fillna(method='pad')
            df['项目'] = df['项目'].fillna(method='backfill')
            dflist.append(df)
        except Exception as e:
            info('一份文件打开失败请检查', fi, e)
            fail_file.append(fi)
    info('正在合并文件')
    df_combine = pd.concat(dflist)  # 合并
    info('合并完成')
    info('文件夹内文件共：', len(all_files), '份,成功读取', len(dflist), '个，失败', len(fail_file), '个')
    if fail_file:
        info('失败文件如下:')
        for i in fail_file:
            info(i)
    # 保存
    if not save_path:
        config = read_config()
        save_path = config['save']
    save_path = Path(save_path) / datetime.datetime.now().strftime('%Y年%m月%d日')
    if not save_path.exists(): save_path.mkdir()
    save_path = save_path / '合并'
    if not save_path.exists(): save_path.mkdir()
    save_path = save_path / '基础产能表合并.xlsx'
    info('正在保存：', save_path)
    df_combine.to_excel(save_path, index=False)
    info('保存成功', save_path)
    return


if __name__ == '__main__':
    run()
