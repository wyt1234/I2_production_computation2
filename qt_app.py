import json
import os
import re
import subprocess
import sys
import time
import threading

# import decorator_libs
# from decorator_libs import keep_circulating
from utils.decorator import keep_circulating
from configobj import ConfigObj
# from concurrent.futures import ThreadPoolExecutor
# from threadpool_executor_shrink_able import CustomThreadpoolExecutor as ThreadPoolExecutor

import urllib.parse
import base64
from loguru import logger
from requests import Session
from tqdm import tqdm
import time

from qtui import Ui_MainWindow
from PyQt5 import QtGui, QtWidgets, QtCore
from PyQt5.QtCore import QObject, pyqtSignal, pyqtBoundSignal, QThread
from PyQt5.QtWidgets import QApplication, QDialog, QMainWindow, QLineEdit, QTextEdit, QPlainTextEdit

# import decorator_libs
# from nb_log import LoggerMixinDefaultWithFileHandler
# from nb_log.monkey_print import reverse_patch_print
# import nb_log
# from translate_util.translate_tool import translate_other2cn, translate_other2en

# reverse_patch_print()
# nb_log.nb_log_config_default.DEFAULUT_USE_COLOR_HANDLER = False

import step2_gather
import step3_by_pj
import step4_by_pj_month


def my_excepthook(exc_type, exc_value, tb):
    """
    异常重定向到print，print重定向到控制台，一切信息逃不出控制台。
    :param exc_type:
    :param exc_value:
    :param tb:
    :return:
    """
    msg = ' Traceback (most recent call last):\n'
    while tb:
        filename = tb.tb_frame.f_code.co_filename
        name = tb.tb_frame.f_code.co_name
        lineno = tb.tb_lineno
        msg += '   File "%.500s", line %d, in %.500s\n' % (filename, lineno, name)
        tb = tb.tb_next

    msg += ' %s: %s\n' % (exc_type.__name__, exc_value)
    print(msg)


class WindowsClient(QMainWindow):
    """
    左界面右控制台的，通用客户端基类，重点是吃力了控制台，不带其他逻辑。
    """
    _lock_for_write = threading.Lock()

    def __init__(self, *args, **kwargs):
        QMainWindow.__init__(self, *args, **kwargs)
        # 除了控制台以外，在文件中也会记录日志。
        # self.file_logger = nb_log.get_logger(f'{self.__class__.__name__}_file', is_add_stream_handler=False,
        #                                      log_filename=f'{self.__class__.__name__}_file.log', log_path='./')

        """
               # 这个用组合的形式，来访问控件。

               网上有的是用继承方式，让WindowsClient同时也继承Ui_MainWindow，那么这两行

               self.ui = Ui_MainWindow()  
               self.ui.setupUi(self)

               就成了一行，变成 self.setupUi(self) 然后用self.pushButtonxx 来访问控件。
               现在方式self.ui.pushButtonxx来访问控件，这种pycahrm自动补全范围更小，使用更清晰。

        """

        self.ui = Ui_MainWindow()
        self.ui.setupUi(self)

        self._now_is_stop_print = False
        self._len_textEdit = 0

        self.ui.pushButton_3.clicked.connect(self._stop_or_start_print)
        self.ui.pushButton_4.clicked.connect(self._clear_text_edit)

        self.config_ini = ConfigObj("qt_box_values.ini", encoding='UTF8')

        sys.excepthook = my_excepthook  # 错误重定向到print，print重定向到qt界面的控制台，使永远不会发生出错导致闪退。

        self.__init_std()
        self.custom_init()
        self.set_button_click_event()
        self.set_default_value()

        self._init_all_input_box_value()
        # 定时记录所有input的值，这边先暂停
        # keep_circulating(60, block=False)(self._save_all_input_box_value)()
        # 固定窗口大小
        self.setFixedSize(self.width(), self.height())

    def custom_init(self):
        pass

    def set_button_click_event(self):
        pass

    def set_default_value(self):
        pass

    def __init_std(self):
        sys.stdout.write = self._write
        sys.stderr.write = self._write
        print('重定向了print到textEdit ,这个print应该显示在右边黑框。')

    def _stop_or_start_print(self):
        if self._now_is_stop_print is False:
            self._now_is_stop_print = True
            self.ui.pushButton_3.setText('暂停控制台打印')
            self.ui.pushButton_3.setStyleSheet('''
            color: rgb(255, 255, 255);
            font: 9pt "楷体";
            background-color: rgb(255, 8, 61);
                        ''')
            sys.stdout.write = lambda info: self.file_logger.debug(info)
        else:
            self._now_is_stop_print = False
            self.ui.pushButton_3.setText('控制台打印中')
            self.ui.pushButton_3.setStyleSheet('''
            background-color: rgb(0, 173, 0);
            color: rgb(255, 255, 255);
            font: 9pt "楷体";
            ''')
            sys.stdout.write = self._write

    def _write(self, info):
        """
        这个是关键，普通print是如何自动显示在右边界面的黑框的。
          https://blog.csdn.net/LaoYuanPython/article/details/105317746
          :return:
        """
        # self.ui.textEdit.insertPlainText(info)
        # if len(self.ui.textEdit.toPlainText()) > 50000:
        #     self.textEdit.setPlainText('')
        with self._lock_for_write:
            self._len_textEdit += len(info)
            if self._len_textEdit > 50000:
                self.ui.textEdit.setText(' ')
                self._len_textEdit = 0
            cursor = self.ui.textEdit.textCursor()
            cursor.movePosition(QtGui.QTextCursor.End)
            cursor.insertText(info)
            self.ui.textEdit.setTextCursor(cursor)
            self.ui.textEdit.ensureCursorVisible()
            QtWidgets.qApp.processEvents(
                QtCore.QEventLoop.ExcludeUserInputEvents | QtCore.QEventLoop.ExcludeSocketNotifiers)
            # self.file_logger.debug(info)

    @staticmethod
    def _do_away_with_color(info: str):
        info = info.replace('\033[0;34m', '').replace('\033[0;30;44m', '')
        info = re.sub(r"\033\[0;.{1,7}m", '', info)
        info = info.replace('\033[0m', '')
        return info

    def _clear_text_edit(self):
        """
        清除控制台信息
        :return:
        """
        self.ui.textEdit.setText(' ')
        self._len_textEdit = 0

    def _save_all_input_box_value(self):
        # 客户端退出前保存所有输入框的值到ini文件，使下次重启时候默认加载上一次的值。
        for k, v in self.ui.__dict__.items():
            if k == 'textEdit':  # textEdit这个使代表右边那个黑框控制台，把这个排除在外
                continue
            if isinstance(v, QLineEdit):
                self.config_ini['qt_input_box_valus'][k] = v.text()
            if isinstance(v, (QTextEdit, QPlainTextEdit)):
                self.config_ini['qt_input_box_valus'][k] = v.toPlainText()
            self.config_ini.write()

    def _init_all_input_box_value(self):
        """
        初始化界面的值为上一次客户端关闭之前的值
        :return:
        """
        for k, v in self.ui.__dict__.items():
            try:
                print(f'控件的名字 {k},  控件对象 {v}')
                if isinstance(v, QLineEdit):
                    v.setText(self.config_ini['qt_input_box_valus'][k])
                if isinstance(v, (QTextEdit, QPlainTextEdit)):
                    v.setPlainText(self.config_ini['qt_input_box_valus'][k])
                print(f"成功设置 【{k}】 -- 【{self.config_ini['qt_input_box_valus'][k]}】")
            except KeyError as e:
                print(e)

    def closeEvent(self, event):
        self._save_all_input_box_value()
        reply = QtWidgets.QMessageBox.question(self, '警告', '\n你确认要退出吗？',
                                               QtWidgets.QMessageBox.Yes, QtWidgets.QMessageBox.No)
        if reply == QtWidgets.QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()


class CustomQthread(QThread):
    def __init__(self, parent=None, target=None, args=(), kwargs={}):  # noqa
        super(CustomQthread, self).__init__(parent)
        self._target = target
        self._args = args
        self._kwargs = kwargs

    def run(self):
        """Method representing the thread's activity.

        You may override this method in a subclass. The standard run() method
        invokes the callable object passed to the object's constructor as the
        target argument, if any, with sequential and keyword arguments taken
        from the args and kwargs arguments, respectively.

        """
        try:
            if self._target:
                self._target(*self._args, **self._kwargs)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._target, self._args, self._kwargs


def run_fun_in_new_thread(f, args=()):
    threading.Thread(target=f, args=args).start()


class CustomWindowsClient(WindowsClient, ):

    def info(self, *args):
        print(''.join([str(arg) for arg in args]))

    def custom_init(self):
        self._has_start_request = False
        self._request_threadppool = None

    def set_button_click_event(self):
        self.ui.toolButton.clicked.connect(lambda: run_fun_in_new_thread(self.choose_dir_from))
        self.ui.toolButton_2.clicked.connect(lambda: run_fun_in_new_thread(self.choose_dir_to))  #
        self.ui.pushButton_7.clicked.connect(lambda: run_fun_in_new_thread(self.openfolder))  # 打开资源管理器
        self.ui.pushButton.clicked.connect(lambda: run_fun_in_new_thread(self.run_step2))
        self.ui.pushButton_2.clicked.connect(lambda: run_fun_in_new_thread(self.run_step3))
        self.ui.pushButton_5.clicked.connect(lambda: run_fun_in_new_thread(self.run_step4))

        pass

    # 选择文件夹
    # 按下按键选择文件夹并显示：
    # https://www.cnblogs.com/daiguoxi/p/14012812.html
    def choose_dir_from(self):
        path = self.ui.lineEdit.text() if self.ui.lineEdit.text() else "C:/"
        directory = QtWidgets.QFileDialog.getExistingDirectory(None, "选取文件夹", path)  # 起始路径
        self.ui.lineEdit.setText(directory)
        self._save_all_input_box_value()
        return

    # 选择文件夹2
    def choose_dir_to(self):
        path = self.ui.lineEdit_2.text() if self.ui.lineEdit_2.text() else "C:/"
        directory = QtWidgets.QFileDialog.getExistingDirectory(None, "选取文件夹", path)  # 起始路径
        self.ui.lineEdit_2.setText(directory)
        self._save_all_input_box_value()
        return

    # 弹出资源管理器
    def openfolder(self):
        folder = self.ui.lineEdit_2.text() if self.ui.lineEdit_2.text() else "C:/"
        os.startfile(folder)

    # 运行step2
    def run_step2(self):
        if self.ui.lineEdit.text() and self.ui.lineEdit_2.text():
            step2_gather.run(dirs=self.ui.lineEdit.text(), save_path=self.ui.lineEdit_2.text())
        else:
            self.info('请选择源文件目录和保存目录')

    # 运行step3
    def run_step3(self):
        if self.ui.lineEdit.text() and self.ui.lineEdit_2.text():
            step3_by_pj.run(dirs=self.ui.lineEdit_2.text())
        else:
            self.info('请选择源文件目录和保存目录')

    # 运行step4
    def run_step4(self):
        if self.ui.lineEdit.text() and self.ui.lineEdit_2.text():
            step4_by_pj_month.run(dirs=self.ui.lineEdit_2.text())
        else:
            self.info('请选择源文件目录和保存目录')


if __name__ == '__main__':
    """
    参数 含义
    -F 指定打包后只生成一个exe格式的文件
    -D 创建一个目录，包含exe文件，但会依赖很多文件（默认选项）
    -c 使用控制台，无界面(默认)
    -w 使用窗口，无控制台
    -p 添加搜索路径，让其找到对应的库。
    --icon 改变生成程序的icon图标(图片必须是icon格式的，可以在线转换)

    pyuic5 -o qtui.py qtui.ui
    --add-data "F:\coding2\ydfhome\pyqt项目\pyqt5demo\logo1.ico;logo1.ico"
    pyinstaller -F -w -i logo1.ico -p F:\minicondadir\Miniconda2\envs\py36\Lib\site-packages --nowindowed  qt_app.py
    """
    # F:\Users\ydf\Desktop\oschina\ydfhome\tests\test1.py
    # from qdarkstyle import load_stylesheet_pyqt5

    myapp = QApplication(sys.argv)
    # myapp.setStyleSheet(load_stylesheet_pyqt5())
    client = CustomWindowsClient()
    client.show()
    sys.exit(myapp.exec_())
