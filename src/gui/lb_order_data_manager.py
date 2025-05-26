from typing import Dict, List
import pandas as pd
import logging
import datetime
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

        self.order_only_dict: Dict[str, do.Order] = dict()
        self.forecast_order_dict: Dict[str, do.Order] = dict()

        self._initialize()

    # region 初始化数据区域
    def _initialize(self):
        self.create_order_only_list_table()
        self.generate_order_only_dict()
        self.check_forecast_order_table()
        self.generate_forecast_order_dict()

    def create_order_only_list_table(self):
        '''
        从中台读取订单数据，创建订单列表表
        '''
        pass



    def generate_order_only_dict(self):
        '''
        从数据库中读取订单数据，生成订单字典
        '''
        pass

    def create_new_fo_list(self):
        oh = fd.OrderListHeader

        self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.FO_LIST_TABLE))
        self.cur.execute(
            """
                CREATE TABLE {} (
                    {} TEXT PRIMARY KEY,   -- order_id
                    {} TEXT NOT NULL, -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL, -- product
                    {} TEXT NOT NULL, -- from_time
                    {} TEXT NOT NULL, -- to_time
                    {} REAL NOT NULL, -- drop_kg
                    {} TEXT, -- comment
                    {} TEXT NOT NULL, -- po_number
                    {} TEXT NOT NULL, -- in_trip_draft
                    {} TEXT NOT NULL, -- so_number
                    {} TEXT NOT NULL -- apex_id
                );
            """.format(
                fd.FO_LIST_TABLE,
                oh.order_id,
                oh.shipto,
                oh.cust_name,
                oh.product,
                oh.from_time,
                oh.to_time,
                oh.drop_kg,
                oh.comment,
                oh.po_number,
                oh.in_trip_draft,
                oh.so_number,
                oh.apex_id
            )
        )
        self.conn.commit()

    def create_new_fo_record_list(self):
        oh = fd.OrderListHeader
        self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.FO_RECORD_LIST_TABLE))
        self.cur.execute(
            """
                CREATE TABLE {} (
                     {} TEXT NOT NULL,   -- order_id
                    {} TEXT NOT NULL,   -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL, -- product
                    {} TEXT NOT NULL, -- from_time
                    {} TEXT NOT NULL, -- to_time
                    {} REAL NOT NULL, -- drop_kg
                    {} TEXT, -- comment
                    {} TEXT NOT NULL, -- po_number
                    {} TEXT NOT NULL, -- edit_type
                    {} TEXT NOT NULL, -- timestamp
                    {} TEXT NOT NULL, -- so_number
                    {} TEXT NOT NULL -- apex_id
                );
            """.format(
                fd.FO_RECORD_LIST_TABLE,
                oh.order_id,
                oh.shipto,
                oh.cust_name,
                oh.product,
                oh.from_time,
                oh.to_time,
                oh.drop_kg,
                oh.comment,
                oh.po_number,
                oh.edit_type,
                oh.timestamp,
                oh.so_number,
                oh.apex_id
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
            self.create_new_fo_record_list()



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
                product=row[oh.product],
                from_time=row[oh.from_time],
                to_time=row[oh.to_time],
                drop_kg=row[oh.drop_kg],
                comments=row[oh.comment],
                po_number=row[oh.po_number],
                order_type=enums.OrderType.FO,
                so_number=row[oh.so_number],
                is_in_trip_draft=int(row[oh.in_trip_draft])
            )
            self.forecast_order_dict[forecast_order.order_id] = forecast_order
        logging.info('Forecast order dict generated: {}'.format(len(self.forecast_order_dict)))

    # endregion

    # region 订单数据操作区域

    def insert_order_in_fo_list(self, order: do.Order):
        fo_sql_line = '''
                   INSERT INTO {} VALUES 
                   (
                       ?, -- order_id
                       ?, -- shipto
                       ?, -- cust_name
                       ?, -- product
                       ?, -- from_time
                       ?, -- to_time
                       ?, -- drop_kg
                       ?, -- comment
                       ?, -- po_number
                       ?, -- in_trip_draft
                       ?, -- so_number
                       ? -- apex_id
                   )
               '''.format(fd.FO_LIST_TABLE)
        self.cur.execute(
            fo_sql_line,
            (
                order.order_id,
                order.shipto,
                order.cust_name,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                order.po_number,
                order.is_in_trip_draft,
                order.so_number,
                func.get_user_name()
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
                   {} = ? -- in_trip_draft
                   WHERE {} = ?
                '''.format(
            fd.FO_LIST_TABLE,
                    oh.from_time,
                    oh.to_time,
                    oh.drop_kg,
                    oh.comment,
                    oh.in_trip_draft,
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
                order.order_id
            )
        )
        self.conn.commit()
        logging.info('Order modified in FOList: {}'.format(order.shipto))

    def insert_order_record_in_fo_record_list(self, order: do.Order, edit_type: enums.EditType):
        fo_sql_line = '''
                   INSERT INTO {} VALUES 
                   (
                       ?, -- order_id
                       ?, -- shipto
                       ?, -- cust_name
                       ?, -- product
                       ?, -- from_time
                       ?, -- to_time
                       ?, -- drop_kg
                       ?, -- comment
                       ?, -- po_number
                       ?, -- edit_type
                       ?, -- timestamp
                       ?, -- so_number
                       ? -- apex_id
                   )
               '''.format(fd.FO_RECORD_LIST_TABLE)
        self.cur.execute(
            fo_sql_line,
            (
                order.order_id,
                order.shipto,
                order.cust_name,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                order.po_number,
                edit_type,
                pd.to_datetime(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
                order.so_number,
                func.get_user_name()
            )
        )

        self.conn.commit()
        logging.info('Order record added to FORecordList: {}, {}'.format(order.shipto, edit_type))

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

    def update_so_number_in_fo_record_list(self, order_id: str, so_number: str):
        oh = fd.OrderListHeader
        fo_sql_line = '''
                           UPDATE {} SET 
                           {} = ? -- so_number
                           WHERE {} = ?
                        '''.format(
            fd.FO_RECORD_LIST_TABLE,
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
        logging.info('update so number {} of oder {} in FORecordList'.format(so_number,order_id))

    def add_forecast_order(
            self, order: do.Order
    ):
        # 缓存中增加一个FO订单
        self.forecast_order_dict.update({order.order_id: order})

        self.insert_order_in_fo_list(order=order)
        self.insert_order_record_in_fo_record_list(order=order, edit_type=enums.EditType.Create)



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
        self.create_new_fo_record_list()
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