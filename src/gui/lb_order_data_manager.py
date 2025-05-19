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
                    {} TEXT PRIMARY KEY,   -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL, -- product
                    {} TEXT NOT NULL, -- from_time
                    {} TEXT NOT NULL, -- to_time
                    {} REAL NOT NULL, -- drop_kg
                    {} TEXT NOT NULL -- comment
                );
            """.format(
                fd.FO_LIST_TABLE,
                oh.shipto,
                oh.cust_name,
                oh.product,
                oh.from_time,
                oh.to_time,
                oh.drop_kg,
                oh.comment
            )
        )
        self.conn.commit()

    def create_new_fo_record_list(self):
        oh = fd.OrderListHeader
        self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.FO_RECORD_LIST_TABLE))
        self.cur.execute(
            """
                CREATE TABLE {} (
                    {} TEXT NOT NULL,   -- shipto
                    {} TEXT NOT NULL, -- cust_name
                    {} TEXT NOT NULL, -- product
                    {} TEXT NOT NULL, -- from_time
                    {} TEXT NOT NULL, -- to_time
                    {} REAL NOT NULL, -- drop_kg
                    {} TEXT, -- comment
                    {} TEXT NOT NULL, -- edit_type
                    {} TEXT NOT NULL -- timestamp

                );
            """.format(
                fd.FO_RECORD_LIST_TABLE,
                oh.shipto,
                oh.cust_name,
                oh.product,
                oh.from_time,
                oh.to_time,
                oh.drop_kg,
                oh.comment,
                oh.edit_type,
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
        return forecast_order_df

    def generate_forecast_order_dict(self):
        '''
        从数据库中读取预测订单数据，生成预测订单字典
        '''
        oh = fd.OrderListHeader
        forecast_order_df = self.get_forecast_order_result_list()

        for index, row in forecast_order_df.iterrows():
            forecast_order = do.Order(
                shipto=row[oh.shipto],
                cust_name=row[oh.cust_name],
                product=row[oh.product],
                from_time=row[oh.from_time],
                to_time=row[oh.to_time],
                drop_kg=row[oh.drop_kg],
                comments=row[oh.comment],
                order_type=enums.OrderType.FO
            )
            self.forecast_order_dict[forecast_order.shipto] = forecast_order
        logging.info('Forecast order dict generated: {}'.format(len(self.forecast_order_dict)))

    # endregion

    # region 订单数据操作区域

    def insert_order_in_fo_list(self, order: do.Order):
        fo_sql_line = '''
                   INSERT INTO {} VALUES 
                   (
                       ?, -- shipto
                       ?, -- cust_name
                       ?, -- product
                       ?, -- from_time
                       ?, -- to_time
                       ?, -- drop_kg
                       ? -- comment
                   )
               '''.format(fd.FO_LIST_TABLE)
        self.cur.execute(
            fo_sql_line,
            (
                order.shipto,
                order.cust_name,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments
            )
        )

        self.conn.commit()
        logging.info('Order added to FOList: {}'.format(order.shipto))

    def update_forecast_order_in_fo_list(self, order: do.Order):
        oh = fd.OrderListHeader
        fo_sql_line = '''
                   UPDATE {} SET 
                   {} = ?, -- cust_name
                   {} = ?, -- product
                   {} = ?, -- from_time
                   {} = ?, -- to_time
                   {} = ?, -- drop_kg
                   {} = ? -- comment
                   WHERE {} = ?
                '''.format(
            fd.FO_LIST_TABLE,
                    oh.cust_name,
                    oh.product,
                    oh.from_time,
                    oh.to_time,
                    oh.drop_kg,
                    oh.comment,
                    oh.shipto
        )
        self.cur.execute(
            fo_sql_line,
            (
                order.cust_name,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                order.shipto
            )
        )
        self.conn.commit()
        logging.info('Order modified in FOList: {}'.format(order.shipto))

    def insert_order_record_in_fo_record_list(self, order: do.Order, edit_type: enums.EditType):
        fo_sql_line = '''
                   INSERT INTO {} VALUES 
                   (
                       ?, -- shipto
                       ?, -- cust_name
                       ?, -- product
                       ?, -- from_time
                       ?, -- to_time
                       ?, -- drop_kg
                       ?, -- comment
                       ?, -- edit_type
                       ? -- timestamp
                   )
               '''.format(fd.FO_RECORD_LIST_TABLE)
        self.cur.execute(
            fo_sql_line,
            (
                order.shipto,
                order.cust_name,
                order.product,
                order.from_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.to_time.strftime('%Y-%m-%d %H:%M:%S'),
                order.drop_kg,
                order.comments,
                edit_type,
                pd.to_datetime(datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')
            )
        )

        self.conn.commit()
        logging.info('Order record added to FORecordList: {}, {}'.format(order.shipto, edit_type))

    def add_forecast_order(
            self, order: do.Order
    ):
        if order.shipto in self.forecast_order_dict:
            return
        # 缓存中增加一个FO订单
        self.forecast_order_dict.update({order.shipto: order})

        self.delete_forecast_order_from_fo_list(shipto=order.shipto)
        self.insert_order_in_fo_list(order=order)
        self.insert_order_record_in_fo_record_list(order=order, edit_type=enums.EditType.Create)



    def delete_forecast_order_from_fo_list(
            self,
            shipto: str
    ):
        # 从数据库中删除记录
        oh = fd.OrderListHeader
        delete_sql_line = '''
                   DELETE FROM {} WHERE {} = ?;
               '''.format(fd.FO_LIST_TABLE, oh.shipto)
        self.cur.execute(
            delete_sql_line,
            (shipto,)
        )

        self.conn.commit()
        logging.info('Forecast order deleted from FOList: {}'.format(shipto))
    # endregion