import pandas as pd
from typing import List, Dict
from ..utils import functions as func
from .. import domain_object as do

class LBDataManager:
    def __init__(
            self
    ):
        self.conn = func.connect_sqlite('./AutoSchedule.sqlite')
        self.cur = self.conn.cursor()

    def get_history_reading(self, shipto, fromTime, toTime):
        conn = self.conn
        '''获取历史液位数据'''
        sql = '''select LocNum, ReadingDate, Reading_Gals
                 FROM historyReading
                 where ReadingDate >= '{}'
                 AND ReadingDate <= '{}'
                 AND LocNum = {};'''.format(fromTime, toTime, shipto)
        df_history = pd.read_sql(sql, conn)
        df_history.ReadingDate = pd.to_datetime(df_history.ReadingDate)
        return df_history


    def get_forecast_reading(self, shipto, fromTime, toTime):
        '''获取预测液位数据, 注意 返回的 df_forecast 长度始终大于 0；'''
        # 第一步 首先判断 该 shipto 是不是一个异常 shipto
        conn = self.conn
        sql = '''select * FROM forecastReading
             where (Forecasted_Reading=999999
                    OR Forecasted_Reading=888888
                    OR Forecasted_Reading=777777)
                    AND LocNum = {};'''.format(shipto)
        df_forecast = pd.read_sql(sql, conn)
        # 第二步 获取正常取值范围
        if len(df_forecast) > 0:
            return df_forecast
        sql = '''select * FROM forecastReading
             where Next_hr >= '{}'
             AND Next_hr <= '{}'
             AND LocNum = {};'''.format(fromTime, toTime, shipto)
        df_forecast = pd.read_sql(sql, conn)
        # 第三步 如果 df_forecast 是空集,需要一些必要元素的补充。
        if len(df_forecast) == 0:
            sql = '''select DISTINCT TargetRefillDate, TargetRiskDate,
                                     TargetRunoutDate, RiskGals
                     FROM forecastReading
                     where LocNum = {};'''.format(shipto)
            df_forecast = pd.read_sql(sql, conn)
            df_forecast['Next_hr'] = None
            df_forecast['Forecasted_Reading'] = None
            df_forecast['Hourly_Usage_Rate'] = None
        # 对上述两种情况一起处理的部分
        df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
        df_forecast.TargetRefillDate = pd.to_datetime(df_forecast.TargetRefillDate)
        df_forecast.TargetRiskDate = pd.to_datetime(df_forecast.TargetRiskDate)
        df_forecast.TargetRunoutDate = pd.to_datetime(df_forecast.TargetRunoutDate)
        return df_forecast


    def get_forecast_before_trip(self, shipto, fromTime, toTime):
        '''获取当前到送货前预测液位数据'''
        conn = self.conn
        sql = '''select LocNum, Next_hr, Forecasted_Reading
             FROM forecastBeforeTrip
             where Next_hr >= '{}'
             AND Next_hr <= '{}'
             AND LocNum = {};'''.format(fromTime, toTime, shipto)
        df_forecast = pd.read_sql(sql, conn)
        df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
        return df_forecast


    def get_before_reading(self, shipto):
        '''获取司机录入液位'''
        conn = self.conn
        sql = '''select ReadingDate, beforeKG from beforeReading
                 WHERE LocNum={};'''.format(shipto)
        df = pd.read_sql(sql, conn)
        df = df.sort_values('ReadingDate')
        return df.beforeKG.values


    def get_max_payload_by_ship2(
            self,
            ship2: str,
    ):
        conn = self.conn
        sql_statement = \
            ("SELECT CorporateIdn, LicenseFill "
             "FROM odbc_MaxPayloadByShip2 "
             "WHERE ToLocNum = '{}' ").format(ship2)
        result_df = pd.read_sql(sql_statement, conn)
        return result_df


    def get_manual_forecast(self, shipto, fromTime, toTime):
        '''get manually calculated data'''
        conn = self.conn
        sql = '''select *
                 FROM manual_forecast
                 where Next_hr >= '{}'
                 AND Next_hr <= '{}'
                 AND LocNum = {};'''.format(fromTime, toTime, shipto)
        df_manual = pd.read_sql(sql, conn)
        df_manual.Next_hr = pd.to_datetime(df_manual.Next_hr, format='mixed')
        return df_manual


    def get_customer_info(self, shipto):
        '''获取customer数据'''
        conn = self.conn
        sql = '''select *
             FROM odbc_master
             where LocNum = {};'''.format(shipto)
        df_info = pd.read_sql(sql, conn)
        return df_info

    def get_full_trycock_gals_by_shipto(self, shipto):
        '''获取完整的customer数据'''
        sql = '''select LocNum, FullTrycockGals
                     FROM odbc_master
                     where LocNum = {};'''.format(shipto)
        results = self.cur.execute(sql).fetchall()
        for (LocNum, FullTrycockGals) in results:
            return FullTrycockGals


    def get_recent_reading(self, shipto):
        '''从 historyReading 里获取最近液位读数'''
        conn = self.conn
        df_info = self.get_customer_info(shipto)
        galsPerInch = df_info.GalsPerInch.values[0]
        sql = '''select ReadingDate, Reading_Gals
                     FROM historyReading
                     where LocNum = {};'''.format(shipto)
        df1 = pd.read_sql(sql, conn).tail(24)
        df1.ReadingDate = pd.to_datetime(df1.ReadingDate)
        df1['Reading_CM'] = (df1.Reading_Gals / galsPerInch).round().astype(int)
        df1.Reading_Gals = df1.Reading_Gals.astype(int)
        df1 = df1.sort_values('ReadingDate', ascending=False).reset_index(drop=True)
        df1['cm_diff'] = df1.Reading_CM.diff(-1)
        df1['time_diff'] = df1.ReadingDate.diff(-1) / pd.Timedelta('1 hour')
        df1['Hour_CM'] = (df1.cm_diff / df1.time_diff).round(1)

        def clean_use(x):
            # 对小时用量进行清理
            if pd.isnull(x):
                return x
            if x <= 0 and x != -float('inf'):
                return -int(x)
            else:
                return None

        df1.Hour_CM = df1.Hour_CM.apply(clean_use)

        df1.ReadingDate = df1.ReadingDate.dt.strftime('%m-%d %H')
        df1['Reading_Gals'] = df1.Reading_Gals.apply(lambda x: round(x / 1000, 1))
        cols = df1.columns.tolist()

        # 去掉两个过度列
        cols.remove('cm_diff')
        cols.remove('time_diff')
        df1 = df1[cols]

        df1 = df1.rename(columns={'Reading_Gals': 'Read_Ton', 'Reading_CM': 'Read_CM'})
        return df1


    def get_delivery_window(self, shipto):
        '''从 odbc_DeliveryWindow 里获取 送货窗口数据'''
        conn = self.conn
        sql = '''select * from odbc_DeliveryWindow where LocNum={}'''.format(shipto)
        df = pd.read_sql(sql, conn)
        try:
            # 新版本
            df1 = df.loc[:, df.columns[1:]].map(lambda x: pd.to_datetime(x).strftime('%H:%M'))
        except Exception:
            # 老版本
            df1 = df.loc[:, df.columns[1:]].applymap(lambda x: pd.to_datetime(x).strftime('%H:%M'))
        data = {}
        weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        for i in weekdays:
            primary_start = 'Dlvry' + i + 'From'
            primary_end = 'Dlvry' + i + 'To'
            addtional_start = 'Dlvry' + i + 'From1'
            addtional_end = 'Dlvry' + i + 'To1'
            forms = [primary_start, primary_end, addtional_start, addtional_end]
            data[i] = [df1[j].values[0] for j in forms]
        # 再次转成dataframe
        df2 = pd.DataFrame(data)
        df2['title'] = ['PrimaryStart', 'PrimaryEnd', 'AdditionStart', 'AdditionEnd']
        cols = df2.columns.tolist()
        cols = cols[-1:] + cols[:-1]
        df2 = df2[cols]
        return df2


    def get_delivery_window_by_shipto(self, shipto: str):
        cursor = self.cur
        table_name = 'DeliveryWindowInfo'

        sql_line = '''SELECT OrdinaryDeliveryWindow, RestrictedDeliveryPeriods FROM {} WHERE LocNum = '{}' '''.format(
            table_name, shipto)
        cursor.execute(sql_line)
        results = cursor.fetchall()
        for (OrdinaryDeliveryWindow, RestrictedDeliveryPeriods) in results:
            return OrdinaryDeliveryWindow, RestrictedDeliveryPeriods
        return '', ''

    def get_production_schedule_by_shipto(self, shipto: str):
        '''获取生产计划'''
        table_name = 'ProductionSchedule'
        cursor = self.cur

        try:
            sql_line = '''
                SELECT LocNum, OrdinaryProductionSchedule, RestrictedProductionSchedule 
                FROM {} WHERE LocNum = {}
            '''.format(table_name, shipto)
            cursor.execute(sql_line)
            results = cursor.fetchall()
            for (LocNum, OrdinaryProductionSchedule, RestrictedProductionSchedule) in results:
                return OrdinaryProductionSchedule, RestrictedProductionSchedule
        except Exception as e:
            print(e)
            return '', ''


    def get_call_log_by_shipto(self, shipto: str):
        '''获取 call_log'''
        table_name = 'CallLogInfo'

        sql_line = '''SELECT LocNum, CallLog FROM {} WHERE LocNum = '{}' '''.format(
            table_name, shipto)
        cursor = self.cur
        cursor.execute(sql_line)
        results = cursor.fetchall()
        for loc_num, call_log in results:
            return call_log
        return ''

    def get_special_note_by_shipto(self, shipto: str):
        '''获取特殊说明'''
        table_name = 'SpecialNote'

        sql_line = '''SELECT LocNum, Summary FROM {} WHERE LocNum = '{}' '''.format(
            table_name, shipto)
        cursor = self.cur
        try:
            cursor.execute(sql_line)
            results = cursor.fetchall()
            for loc_num, special_note in results:
                return special_note
        except Exception as e:
            print(e)
            return ''

    def get_forecast_error(self, shipto):
        '''获取 forecastError'''
        conn = self.conn
        table_name = 'forecastError '
        sql = '''select * from {} Where LocNum={};'''.format(table_name, shipto)
        df = pd.read_sql(sql, conn)
        if len(df) == 0:
            fe = 'NotFound'
        else:
            fe = str(round(df.AverageError.values[0] * 100)) + '%'
        return fe

    def get_tr_ro_value(self, shipto):
        table_name = "odbc_master"
        sql_line = '''SELECT LocNum, TRRO FROM {} WHERE LocNum = {}'''.format(table_name, shipto)
        cursor = self.cur
        try:
            cursor.execute(sql_line)
            results = cursor.fetchall()
            for loc_num, TRRO in results:
                return TRRO
        except Exception as e:
            print(e)
            return ''

    def get_t4_t6_value(self, shipto):
        conn = self.conn
        table_name = "t4_t6_data"
        sql = '''SELECT * FROM {} WHERE LocNum = {}'''.format(table_name, shipto)
        df = pd.read_sql(sql, conn)
        t4_t6_val = "unknown"

        for i, row in df.iterrows():
            t4_t6_val = round(row['beforeToRoHours_rolling_mean'], 1)
        return t4_t6_val

    def get_forecast_customer_from_sqlite(self):
        '''对这个函数进行一下说明：
           1. 原来这个函数只针对 forecastReading 里的客户，缺点是会遗漏 有 history reading 的客户；
           2. 现在 把有 history reading 的客户 也加上去，目的是：一个客户 即便没有 forecast_data_refresh reading 的值
              也能显示 history reading；'''
        conn = self.conn
        sql = '''SELECT DISTINCT LocNum from forecastReading;'''
        forecast_shiptos = tuple(pd.read_sql(sql, conn).LocNum)
        sql = '''SELECT DISTINCT LocNum from historyReading;'''
        history_shiptos = tuple(pd.read_sql(sql, conn).LocNum)
        full_shiptos = tuple(set(forecast_shiptos + history_shiptos))
        sql = '''select LocNum, CustAcronym, TankAcronym,
                        PrimaryTerminal, SubRegion,
                        ProductClass, DemandType, GalsPerInch,
                        UnitOfLength, Subscriber
                FROM odbc_master
                WHERE
                LocNum IN {};'''.format(full_shiptos)
        df_name_forecast = pd.read_sql(sql, conn)
        df_name_forecast['Acronym'] = df_name_forecast.apply(
            lambda x: '{}, {}'.format(x['CustAcronym'], x['TankAcronym']),
            axis=1
        )

        df_name_forecast = df_name_forecast.set_index('Acronym')
        return df_name_forecast


    def get_all_customer_from_sqlite(self):
        '''get_all_customer_from_sqlite'''
        conn = self.conn
        sql = '''select odbc_master.LocNum, odbc_master.CustAcronym, TankAcronym,
                        odbc_master.PrimaryTerminal, odbc_master.SubRegion,
                        odbc_master.ProductClass, odbc_master.DemandType, odbc_master.GalsPerInch,
                        odbc_master.UnitOfLength, Subscriber, TelemetryFlag
                 FROM odbc_master
              '''

        df_name_all = pd.read_sql(sql, conn).drop_duplicates()
        df_name_all['Acronym'] = df_name_all.apply(
            lambda x: '{}, {}'.format(x['CustAcronym'], x['TankAcronym']),
            axis=1
        )
        df_name_all = df_name_all.set_index('Acronym')

        return df_name_all

    def get_primary_terminal_dtd_info(self, shipto):
        # 提取 primary DTD 信息
        cursor = self.cur
        primary_sql = '''
                    SELECT DT, Distance, Duration, DataSource FROM DTDInfo 
                    WHERE LocNum={} AND DTType='Primary'
                    '''.format(shipto)

        cursor.execute(primary_sql)
        results = cursor.fetchall()
        return results

    def get_sourcing_terminal_dtd_info(self, shipto):
        # 提取 Source DTD 信息
        cursor = self.cur
        source_sql = '''
                    SELECT DT, Distance, Duration, DataSource FROM DTDInfo 
                    WHERE LocNum={} AND DTType='Sourcing'
                    ORDER BY Rank
                    '''.format(shipto)
        cursor.execute(source_sql)
        results = cursor.fetchall()
        return results

    def get_near_customer_info(self, shipto):
        cursor = self.cur
        sql_line = '''
                   SELECT ToLocNum, ToCustAcronym, distanceKM, DDER, DataSource 
                   FROM ClusterInfo
                   WHERE LocNum={}
                   ORDER BY DDER DESC
               '''.format(shipto)

        cursor.execute(sql_line)
        results = cursor.fetchall()
        return results

    def generate_trip_shipto_dict(self):
        table_name = 'trip_shipto'

        sql_line = '''SELECT LocNum, CustAcronym, Location_x, Trip, TripStartTime
         FROM {}'''.format(table_name)

        cursor = self.cur
        cursor.execute(sql_line)
        results = cursor.fetchall()

        trip_shipto_dict = {}
        for loc_num, cust_acronym, location, trip, trip_start_time in results:
            if loc_num == '':
                continue
            if location in trip_shipto_dict and trip is None:
                continue


            trip_shipto = trip_shipto_dict.get(
                location,
                do.TripShipto(
                    shipto_id=str(loc_num),
                    cust_name=cust_acronym,
                    location=location
                )
            )
            if trip is not None:
                trip_shipto.trip_dict.update({trip: pd.to_datetime(trip_start_time)})

            trip_shipto_dict.update({trip_shipto.location: trip_shipto})
        return trip_shipto_dict

    def get_closed_trip_by_shipto(self, shipto: str, trip_list: List[str],
        need_trip_num: int = 5
    ):
        table_name = 'DropRecordSummary'

        df_drop_record = pd.read_sql(
            '''SELECT arrival_time, arrival_str, drop_ton, interval, trip_id, status, route 
                FROM {} WHERE LocNum = '{}' AND trip_id NOT IN () ORDER BY arrival_time DESC 
                '''.format(
                table_name,
                shipto,
                ','.format(trip_list)
            ),
            self.conn
        )
        df_drop_record['arrival_time'] = pd.to_datetime(df_drop_record['arrival_time'], format='mixed')
        if isinstance(need_trip_num, int):
            df_drop_record = df_drop_record.head(need_trip_num - len(trip_list))

        return df_drop_record


    def generate_view_trip_dict_by_shipto(
            self,
            shipto: str
    ):

        sql_line = '''
               WITH TS AS (
                    SELECT Trip
                    FROM trip_shipto
                    WHERE LocNum = '{}'
                )
                SELECT 
                    Trip, 
                    TripStartTime, 
                    Status, 
                    segmentNum, 
                    Type, 
                    Loc, 
                    ToLocNum, 
                    DeliveredQty, 
                    ActualArrivalTime 
                FROM view_trip
                WHERE Trip IN (SELECT Trip FROM TS);
           '''.format(
            shipto
        )

        trip_df = pd.read_sql(sql_line, self.conn)
        trip_df['TripStartTime'] = pd.to_datetime(trip_df['TripStartTime'])
        trip_df['ActualArrivalTime'] = pd.to_datetime(trip_df['ActualArrivalTime'],format='mixed', dayfirst=True)

        trip_dict = dict()
        for trip_id, segment_df in trip_df.groupby('Trip'):
            trip_id = str(trip_id)
            trip = do.Trip(
                trip_id=trip_id,
                trip_start_time=segment_df['TripStartTime'].iloc[0]
            )

            segment_dict = dict()
            for i, row in segment_df.iterrows():
                segment = do.Segment(
                    segment_num=row['segmentNum'],
                    segment_type=row['Type'],
                    location=row['Loc'],
                    segment_status=row['Status'],
                    to_loc_num=row['ToLocNum'],
                    arrival_time=row['ActualArrivalTime'],
                    drop_kg=float(row['DeliveredQty'])
                )
                segment_dict.update({segment.segment_num: segment})
            trip.segment_dict = segment_dict

            trip_dict.update({trip.trip_id: trip})
        return trip_dict

    def generate_latest_future_trip_by_shipto(
            self,
            shipto: str
    ):

        # 读取所有相关行
        sql = '''
            SELECT 
                Trip, 
                TripStartTime, 
                Status, 
                segmentNum, 
                Type, 
                Loc, 
                ToLocNum, 
                DeliveredQty, 
                ActualArrivalTime 
            FROM view_trip
            WHERE Trip IN (
                SELECT Trip FROM trip_shipto WHERE LocNum = ?
            )
            AND ToLocNum = ?
        '''
        df = pd.read_sql(sql, self.conn, params=(shipto, shipto,))

        # 解析时间格式
        df['DeliveryQty'] = df['DeliveredQty'].astype(float)
        df['ActualArrivalTime'] = pd.to_datetime(df['ActualArrivalTime'], format='%d/%m/%y %H:%M')

        # 过滤当前时间之后的记录
        df = df[df['ActualArrivalTime'] >= pd.Timestamp.now()]

        # 排序并取最新一条
        df = df.sort_values(by='ActualArrivalTime', ascending=False).head(1)

        for i, row in df.iterrows():
            return float(row['DeliveredQty']), row['ActualArrivalTime']

        return None, None

    def generate_odbc_trip_dict_by_shipto(
            self,
            shipto: str,
            latest_trip_list: List[str],
            need_trip_num: int = 12
    ):
        table_name = 'DropRecord'

        exist_trip_num = len(latest_trip_list)

        if exist_trip_num == 0:
            trip_sql = '('')'
        elif exist_trip_num == 1:
            trip_sql = "('{}')".format(latest_trip_list[0])
        else:
            trip_sql = tuple(latest_trip_list)

        sql_line = \
            '''
                SELECT Trip, LocNum, SegmentIdn, StopType, ToLocNum, Loc, DeliveredQty, ActualArrivalTime
                FROM {}
                WHERE LocNum = '{}'
                AND Trip NOT IN {}
            '''.format(
                table_name,
                shipto,
                trip_sql
            )

        trip_df = pd.read_sql(sql_line, self.conn)
        trip_df['ActualArrivalTime'] = pd.to_datetime(trip_df['ActualArrivalTime'], format='mixed')

        need_extra_trip_num = need_trip_num - exist_trip_num

        # 获取每个 trip 的最早到达时间
        trip_earliest_arrival = trip_df.groupby('Trip')['ActualArrivalTime'].min().reset_index()

        # 按照最早到达时间排序
        trip_earliest_arrival.sort_values(by='ActualArrivalTime', ascending=False, inplace=True)

        # 选择最近的 need_extra_trip_num 个 trip
        selected_trips = trip_earliest_arrival.head(need_extra_trip_num)['Trip'].tolist()

        trip_dict = dict()
        for trip_id in selected_trips:
            segment_df = trip_df[trip_df['Trip'] == trip_id]
            trip = do.Trip(
                trip_id=trip_id,
                trip_start_time=segment_df['ActualArrivalTime'].min()
            )

            segment_dict = dict()
            for i, row in segment_df.iterrows():
                segment = do.Segment(
                    segment_num=row['SegmentIdn'],
                    segment_type=row['StopType'],
                    location=row['Loc'],
                    segment_status='CLSD',
                    to_loc_num=row['ToLocNum'],
                    arrival_time=row['ActualArrivalTime'],
                    drop_kg=float(row['DeliveredQty'])
                )

                segment_dict.update({segment.segment_num: segment})
            trip.segment_dict = segment_dict
            trip_dict.update({trip.trip_id: trip})

        return trip_dict



    def get_last_refresh_time(self):
        table = 'historyReading'
        cur = self.cur

        sql_line = '''SELECT MAX(ReadingDate) FROM {}'''.format(table)
        cur.execute(sql_line)
        try:
            last_refresh_time = cur.fetchone()[0]
            last_refresh_time = pd.to_datetime(last_refresh_time)
            last_refresh_time = last_refresh_time.strftime('%Y-%m-%d %H:%M')
        except Exception as e:
            print(e)
            last_refresh_time = ''
        return last_refresh_time