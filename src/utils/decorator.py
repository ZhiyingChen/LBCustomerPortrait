import time
import logging
import functools
import sqlite3

# 定义全局变量用于存储任务及其相应的时间
tasks = []


# 函数用于添加任务及其相应的时间到全局变量中
def add_task(task_name: str, time_taken: float):
    global tasks
    tasks.append((task_name, time_taken))


def record_time_decorator(task_name: str):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            st = time.time()
            # 调用原函数
            result = func(*args, **kwargs)
            ed = time.time()
            total_time = round(ed - st, 4)
            current_task_name = task_name
            print("{}: {}".format(current_task_name, total_time))
            logging.info("{}: {}".format(current_task_name, total_time))
            if current_task_name == "添加固定约束时长":
                current_task_name = "{}（{}）".format(current_task_name, func.__name__)
            add_task(task_name=current_task_name, time_taken=total_time)
            return result

        return wrapper

    return decorator


def out_profile(output_folder: str):
    # 打开文本文件，准备写入
    with open(
        "{}time_profile.txt".format(output_folder), "w", encoding="utf-8"
    ) as file:
        # 写入任务及其相应的时间
        for task, time_taken in tasks:
            file.write(f"{task}: {time_taken}\n")

    file.close()

def safe_db_operation(func):
    """事务安全装饰器"""
    def wrapper(cur, conn, *args, **kwargs):
        try:
            with conn:
                return func(cur, conn, *args, **kwargs)
        except sqlite3.OperationalError as e:
            if 'locked' in str(e):
                print("数据库锁定，等待后重试...")
                time.sleep(random.uniform(0.1, 0.5))
                return wrapper(cur, conn, *args, **kwargs)
            raise
    return wrapper