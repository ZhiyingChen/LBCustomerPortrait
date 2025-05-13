from src.forecast_data_refresh.daily_data_refresh import ForecastDataRefresh
from src.utils.functions import connect_sqlite
from src.utils.log import setup_log
from src.utils.email_report import send_email
import os
import logging
import time


if __name__ == '__main__':

    st = time.time()
    setup_log("./output/")

    # 保持数据库连接
    db_name = 'AutoSchedule.sqlite'
    path1 = './test/'


    local_conn = connect_sqlite(os.path.join(path1, db_name))
    local_cur = local_conn.cursor()

    daily_refresh = ForecastDataRefresh(local_cur=local_cur, local_conn=local_conn)
    daily_refresh.refresh_all()

    et = time.time()
    logging.info("Total time: {}".format(et-st))

    addressee = 'chenz32@airproducts.com'
    sender = 'wangj78@airproducts.com'
    send_email(addressee=addressee, sender=sender)

