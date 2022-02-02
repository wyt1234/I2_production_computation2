import threading
import time
import traceback
from functools import wraps


def keep_circulating(time_sleep=0.001, exit_if_function_run_sucsess=False, is_display_detail_exception=True, block=True,
                     daemon=False):
    """间隔一段时间，一直循环运行某个方法的装饰器
    :param time_sleep :循环的间隔时间
    :param exit_if_function_run_sucsess :如果成功了就退出循环
    :param is_display_detail_exception
    :param block :是否阻塞主主线程，False时候开启一个新的线程运行while 1。
    :param daemon: 如果使用线程，那么是否使用守护线程，使这个while 1有机会自动结束。
    """

    # if not hasattr(keep_circulating, 'keep_circulating_log'):
    #     keep_circulating.log = LogManager('keep_circulating').get_logger_and_add_handlers()

    def _keep_circulating(func):
        @wraps(func)
        def __keep_circulating(*args, **kwargs):

            # noinspection PyBroadException
            def ___keep_circulating():
                while 1:
                    try:
                        result = func(*args, **kwargs)
                        if exit_if_function_run_sucsess:
                            return result
                    except Exception as e:
                        msg = func.__name__ + '   运行出错\n ' + traceback.format_exc(
                            limit=10) if is_display_detail_exception else str(e)
                        # keep_circulating.log.error(msg)
                        print(msg)
                    finally:
                        time.sleep(time_sleep)

            if block:
                return ___keep_circulating()
            else:
                threading.Thread(target=___keep_circulating, daemon=daemon).start()

        return __keep_circulating

    return _keep_circulating
