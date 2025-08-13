import pandas as pd
import datetime
from typing import Dict


class TripShipto:
    def __init__(
            self,
            shipto_id: str,
            cust_name: str,
            location: str

    ):
        self.shipto_id = shipto_id
        self.cust_name = cust_name
        self.location = location
        self.trip_dict: Dict[str, pd.Timestamp] = dict()
        self.latest_called: pd.Timestamp = None


    def __str__(self):
        return f"TripShipto(shipto_id={self.shipto_id}, cust_name={self.cust_name})"

    @property
    def nearest_trip_start_time(self):
        """
        最近的行程开始时间
        """
        if self.nearest_trip is None:
            return None
        return self.trip_dict[self.nearest_trip]

    @property
    def nearest_trip(self):
        for trip_id, trip_start_time in self.trip_dict.items():
            if trip_start_time is None:
                continue
            if pd.Timestamp.now() <= trip_start_time <= pd.Timestamp.now() + datetime.timedelta(hours=3):
                return trip_id
        return None

    @property
    def is_trip_planned(self):
        today_trip_dict = {
            t_id: trip for t_id, trip in self.trip_dict.items()
            if trip is not None and trip.date() >= pd.Timestamp.now().date()
        }
        if len(today_trip_dict) == 0:
            return False
        return True

    @property
    def called(self):
        """
        是否已经查询到点击记录
        如果查询时间落在最近的行程开始时间-2h， 最近的行程开始时间之间，则返回True
        """
        if self.latest_called is None:
            return False
        if self.nearest_trip is None:
            return True
        if self.latest_called >= self.nearest_trip_start_time - datetime.timedelta(hours=3):
            return True
        return False


    @property
    def turn_red(
            self
    ):
        '''
        如果未查询到点击记录则变红
        '''


        #  called: 是否已经查询到 （行程开始时间-2h， 行程开始时间） 的点击记录
        if self.called:
            return False

        if self.nearest_trip is None:
            return False

        if pd.Timestamp.now() >= self.nearest_trip_start_time:
            return False

        # 行程开始时间 落在 当前时刻~当前时刻+2h 的行程，里面需要配送的客户， 如果未查询到点击记录则变红
        if pd.Timestamp.now() <= self.nearest_trip_start_time <= pd.Timestamp.now() + datetime.timedelta(hours=2):
            return True

        return False