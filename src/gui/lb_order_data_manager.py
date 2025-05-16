from typing import Dict, List
import pandas as pd
import logging
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

            self.cur.execute('''DROP TABLE IF EXISTS {};'''.format(fd.FO_RECORD_LIST_TABLE))
            self.cur.execute(
                """
                    CREATE TABLE {} (
                        {} TEXT PRIMARY KEY,   -- shipto
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