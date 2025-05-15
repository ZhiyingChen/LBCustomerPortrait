from typing import Dict, List
import pandas as pd
from ..utils import functions as func
from .. import domain_object as do


class LBOrderDataManager:
    def __init__(
            self
    ):
        self.conn = func.connect_sqlite('./OrderTrip.sqlite')
        self.cur = self.conn.cursor

        self.order_only_dict: Dict[str, do.Order] = dict()
        self.forecast_order_dict: Dict[str, do.Order] = dict()

        self._initialize()

    # region 初始化数据区域
    def _initialize(self):
        self.generate_order_only_dict()
        self.generate_forecast_order_dict()

    def generate_order_only_dict(self):
        '''
        从数据库中读取订单数据，生成订单字典
        '''
        pass

    def generate_forecast_order_dict(self):
        '''
        从数据库中读取预测订单数据，生成预测订单字典
        '''
        pass

    # endregion