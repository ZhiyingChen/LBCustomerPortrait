from typing import Dict, List
import pandas as pd
import logging
import datetime
import sqlite3
import time
from ..utils import functions as func
from .. import domain_object as do
from ..utils import field as fd
from ..utils import enums


class LBOrderDataManager:
    def __init__(
            self
    ):
        self.conn = func.connect_sqlite('./OrderTrip.sqlite')
        self.cur = self.conn.cursor()

        self.forecast_order_dict: Dict[str, do.Order] = dict()

        self._initialize()

    # region 初始化数据区域
    def _initialize(self):
        self.check_call_log_table()
        self.update_previous_call_log_to_shared_folder()
        self.check_forecast_order_table()
        self.generate_forecast_order_dict()

    def check_call_log_table(self):
        '''
          检查OrderTrip.sqlite中是否存有 CallLog 这张表，如果没有则新建空表
        '''
        # 检查FOList表是否存在
        self.cur.execute(
            """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='{}';
            """.format(fd.Call_Log_Table)
        )

        if not self.cur.fetchone():
            logging.info('CallLog table not found, creating...')
            # 如果表不存在，则创建表
            self.create_new_call_log()

    def create_new_call_log(self):
        oh = fd.CallLogHeader

        self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.Call_Log_Table))
        self.cur.execute(
            """
                CREATE TABLE {} (
                    {} TEXT NOT NULL, -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL,   -- apex_id
                    {} TEXT NOT NULL   -- timestamp
                );
            """.format(
                fd.Call_Log_Table,
                oh.shipto,
                oh.cust_name,
                oh.apex_id,
                oh.timestamp
            )
        )
        self.conn.commit()

    def update_previous_call_log_to_shared_folder(self):
        shared_filename = r'\\shangnt\lbshell\PUAPI\PU_program\automation\autoScheduling\log_record\log_record.sqlite'

        max_retries = 5
        retry_delay = 3  # 秒

        for attempt in range(max_retries):
            try:
                shared_conn = func.connect_sqlite(shared_filename)
                shared_conn.isolation_level = None  # 设置为自动提交模式
                shared_cur = shared_conn.cursor()

                # 如果共享盘中没有 call log，生成一个新的 table
                table_exists = shared_cur.execute(
                    '''SELECT name FROM sqlite_master WHERE type='table' AND name=?;''', (fd.Call_Log_Table,)
                ).fetchone()

                if not table_exists:
                    shared_conn.execute(
                        '''CREATE TABLE {} (shipto TEXT, cust_name TEXT, apex_id TEXT, timestamp TEXT);'''.format(
                            fd.Call_Log_Table)
                    )
                    print("Table created in shared database:", fd.Call_Log_Table)  # 添加调试信息

                # 开始事务
                shared_conn.execute('BEGIN')

                # 读取今天以前的日志，并插入到共享数据库中
                today = datetime.datetime.now().strftime('%Y-%m-%d')
                sql_line = '''SELECT * FROM {} WHERE timestamp < ?'''.format(fd.Call_Log_Table)
                self.cur.execute(sql_line, (today,))
                rows = self.cur.fetchall()

                if rows:
                    shared_cur.executemany(
                        '''INSERT INTO {} (shipto, cust_name, apex_id, timestamp) VALUES (?, ?, ?, ?)'''.format(
                            fd.Call_Log_Table),
                        rows
                    )
                    print("Rows inserted into shared database:", len(rows))  # 添加调试信息

                # 删除今天以前的日志
                delete_line = '''DELETE FROM {} WHERE timestamp < ?'''.format(fd.Call_Log_Table)
                self.cur.execute(delete_line, (today,))
                self.conn.commit()
                print("Rows deleted from local database:", len(rows))  # 添加调试信息

                # 提交事务
                shared_conn.execute('COMMIT')
                break  # 如果成功，退出循环

            except sqlite3.OperationalError as e:
                if 'locked' in str(e):
                    print("Database is locked, retrying... (Attempt {})".format(attempt + 1))  # 添加调试信息
                    time.sleep(retry_delay)
                else:
                    print("Operational error:", str(e))  # 添加调试信息
                    break  # 其他 OperationalError 不重试
            except sqlite3.Error as e:
                print("Database error:", str(e))  # 添加调试信息
                break  # 数据库错误不重试
            except Exception as e:
                print("An error occurred:", str(e))  # 添加调试信息
                break  # 其他错误不重试
            finally:
                try:
                    if shared_conn:
                        shared_conn.close()
                except sqlite3.Error as e:
                    print("Error closing shared database connection:", str(e))  # 添加调试信息
        else:
            print("Failed to update shared database after {} retries.".format(max_retries))



    def create_new_fo_list(self):
        oh = fd.OrderListHeader

        self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.FO_LIST_TABLE))
        self.cur.execute(
            """
                CREATE TABLE {} (
                    {} TEXT PRIMARY KEY,   -- order_id
                    {} TEXT NOT NULL, -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL, -- corporate_idn
                    {} TEXT NOT NULL, -- product
                    {} TEXT NOT NULL, -- from_time
                    {} TEXT NOT NULL, -- to_time
                    {} REAL NOT NULL, -- drop_kg
                    {} TEXT, -- comment
                    {} TEXT NOT NULL, -- target_date
                    {} TEXT NOT NULL, -- risk_date
                    {} TEXT NOT NULL, -- run_out_date
                    {} TEXT NOT NULL, -- po_number
                    {} TEXT NOT NULL, -- in_trip_draft
                    {} TEXT NOT NULL, -- so_number
                    {} TEXT NOT NULL, -- apex_id
                    {} TEXT NOT NULL -- timestamp
                );
            """.format(
                fd.FO_LIST_TABLE,
                oh.order_id,
                oh.shipto,
                oh.cust_name,
                oh.corporate_idn,
                oh.product,
                oh.from_time,
                oh.to_time,
                oh.drop_kg,
                oh.comment,
                oh.target_date,
                oh.risk_date,
                oh.run_out_date,
                oh.po_number,
                oh.in_trip_draft,
                oh.so_number,
                oh.apex_id,
                oh.timestamp
            )
        )
        self.conn.commit()


    def check_forecast_order_table(self):
        '''
          检查OrderTrip.sqlite中是否存有 FOList 和 FORecordList 这两张表，如果没有则新建空表
        '''
        oh = fd.OrderListHeader

        # 检查FOList表是否存在
        self.cur.execute(
            """
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='{}';
            """.format(fd.FO_LIST_TABLE)
        )

        if not self.cur.fetchone():
            logging.info('FOList table not found, creating...')
            # 如果表不存在，则创建表
            self.create_new_fo_list()


    def get_forecast_order_result_list(self):
        '''
        从数据库中读取预测订单数据，生成预测订单结果列表
        '''
        oh = fd.OrderListHeader
        sql_line = '''SELECT * FROM {}'''.format(fd.FO_LIST_TABLE)
        forecast_order_df = pd.read_sql(
            sql_line,
            self.conn
        )
        forecast_order_df[oh.from_time] = pd.to_datetime(forecast_order_df[oh.from_time])
        forecast_order_df[oh.to_time] = pd.to_datetime(forecast_order_df[oh.to_time])
        forecast_order_df[oh.target_date] = pd.to_datetime(forecast_order_df[oh.target_date])
        forecast_order_df[oh.risk_date] = pd.to_datetime(forecast_order_df[oh.risk_date])
        forecast_order_df[oh.run_out_date] = pd.to_datetime(forecast_order_df[oh.run_out_date])
        forecast_order_df[oh.comment] = forecast_order_df[oh.comment].fillna('')
        forecast_order_df[oh.po_number] = forecast_order_df[oh.po_number].fillna('')
        return forecast_order_df

    def generate_forecast_order_dict(self):
        '''
        从数据库中读取预测订单数据，生成预测订单字典
        '''
        oh = fd.OrderListHeader
        forecast_order_df = self.get_forecast_order_result_list()

        for index, row in forecast_order_df.iterrows():
            forecast_order = do.Order(
                order_id=row[oh.order_id],
                shipto=row[oh.shipto],
                cust_name=row[oh.cust_name],
                corporate_idn=row[oh.corporate_idn],
                product=row[oh.product],
                from_time=row[oh.from_time],
                to_time=row[oh.to_time],
                drop_kg=row[oh.drop_kg],
                comments=row[oh.comment],
                po_number=row[oh.po_number],
                order_type=enums.OrderType.FO,
                target_date=row[oh.target_date],
                risk_date=row[oh.risk_date],
                run_out_date=row[oh.run_out_date],
                so_number=row[oh.so_number],
                is_in_trip_draft=int(row[oh.in_trip_draft])
            )
            self.forecast_order_dict[forecast_order.order_id] = forecast_order
        logging.info('Forecast order dict generated: {}'.format(len(self.forecast_order_dict)))

    # endregion

    # region calllog 操作区域
    def insert_call_log(self, shipto: str, cust_name: str):

        sql_line = '''
            INSERT INTO {} VALUES (?, ?,?, ?)
        '''.format(fd.Call_Log_Table)

        self.cur.execute(
            sql_line,
            (
                shipto,
                cust_name,
                func.get_user_name(),
                pd.to_datetime(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            )
        )
        self.conn.commit()
        logging.info('Call log added: {}, {}'.format(shipto, cust_name))

    def get_latest_call_log(self):
        oh = fd.CallLogHeader
        two_hours_ago = pd.to_datetime(datetime.datetime.now()) - datetime.timedelta(hours=2)

        sql_line = '''
              SELECT {}, {}, MAX({}) as max_timestamp
              FROM {}
              WHERE timestamp > '{}'
              GROUP BY {}, {}
              ORDER BY max_timestamp DESC
          '''.format(
            oh.shipto,
            oh.cust_name,
            oh.timestamp,
            fd.Call_Log_Table,
            two_hours_ago.strftime('%Y-%m-%d %H:%M:%S'),
            oh.shipto,
            oh.cust_name
        )
        self.cur.execute(sql_line)
        results = self.cur.fetchall()
        return results
    # endregion

    # region 订单数据操作区域

    def insert_order_in_fo_list(self, order: do.Order):
        fo_sql_line = '''
                   INSERT INTO {} VALUES 
                   (
                       ?, -- order_id
                       ?, -- shipto
                       ?, -- cust_name
                       ?, -- corporate_idn
                       ?, -- product
                       ?, -- from_time
                       ?, -- to_time
                       ?, -- drop_kg
                       ?, -- comment
                       ?, -- target_date
                       ?, -- risk_date
                       ?, -- run_out_date
                       ?, -- po_number
                       ?, -- in_trip_draft
                       ?, -- so_number
                       ?, -- apex_id
                       ? -- timestamp
                   )
               '''.format(fd.FO_LIST_TABLE)
        self.cur.execute(
            fo_sql_line,
            (
                order.order_id,
                order.shipto,
                order.cust_name,
                order.corporate_idn,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                order.target_date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(order.target_date, datetime.datetime) else '',
                order.risk_date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(order.risk_date, datetime.datetime) else '',
                order.run_out_date.strftime('%Y-%m-%d %H:%M:%S') if isinstance(order.run_out_date, datetime.datetime) else '',
                order.po_number,
                order.is_in_trip_draft,
                order.so_number,
                func.get_user_name(),
                pd.to_datetime(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
            )
        )

        self.conn.commit()
        logging.info('Order added to FOList: {}'.format(order.shipto))

    def update_forecast_order_in_fo_list(self, order: do.Order):
        oh = fd.OrderListHeader
        fo_sql_line = '''
                   UPDATE {} SET 
                   {} = ?, -- from_time
                   {} = ?, -- to_time
                   {} = ?, -- drop_kg
                   {} = ?, -- comment
                   {} = ?, -- in_trip_draft
                   {} = ? -- timestamp
                   WHERE {} = ?
                '''.format(
            fd.FO_LIST_TABLE,
                    oh.from_time,
                    oh.to_time,
                    oh.drop_kg,
                    oh.comment,
                    oh.in_trip_draft,
                    oh.timestamp,
                    oh.order_id
        )
        self.cur.execute(
            fo_sql_line,
            (
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                order.is_in_trip_draft,
                pd.to_datetime(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
                order.order_id
            )
        )
        self.conn.commit()
        logging.info('Order modified in FOList: {}'.format(order.shipto))

    def update_so_number_in_fo_list(self, order_id: str, so_number: str):
        oh = fd.OrderListHeader
        fo_sql_line = '''
                           UPDATE {} SET 
                           {} = ? -- so_number
                           WHERE {} = ?
                        '''.format(
            fd.FO_LIST_TABLE,
            oh.so_number,
            oh.order_id
        )
        self.cur.execute(
            fo_sql_line,
            (
                so_number,
                order_id
            )
        )
        self.conn.commit()
        logging.info('update so number {} of oder {} in FOList'.format(so_number, order_id))


    def add_forecast_order(
            self, order: do.Order
    ):
        # 缓存中增加一个FO订单
        self.forecast_order_dict.update({order.order_id: order})

        self.insert_order_in_fo_list(order=order)


    def delete_forecast_order_from_fo_list(
            self,
            order_id: str
    ):
        # 从数据库中删除记录
        oh = fd.OrderListHeader
        delete_sql_line = '''
                   DELETE FROM {} WHERE {} = ?;
               '''.format(fd.FO_LIST_TABLE, oh.order_id)
        self.cur.execute(
            delete_sql_line,
            (order_id,)
        )

        self.conn.commit()
        logging.info('Forecast order deleted from FOList: {}'.format(order_id))

    def remove_all_forecast_orders(self):
        '''
          3. 清空FOList和RecordList
          4. 清空缓存中的所有FO订单信息
        '''
        self.create_new_fo_list()
        self.forecast_order_dict.clear()

    def get_last_modified_time(self):
        try:
            last_modified_time = (
                self.cur.execute("SELECT max(TimeStamp) FROM FORecordList").fetchone())[0]
        except Exception as e:
            logging.error(f"查询上次修改时间失败：{e}")
            last_modified_time = ""
        if last_modified_time is None:
            last_modified_time = ""
        return last_modified_time
    # endregion