import pandas as pd
from ..utils import functions as func


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
        df_manual.Next_hr = pd.to_datetime(df_manual.Next_hr)
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
            if x <= 0:
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
        # print(len(forecast_shiptos), len(history_shiptos), len(full_shiptos))
        sql = '''select LocNum, CustAcronym,
                        PrimaryTerminal, SubRegion,
                        ProductClass, DemandType, GalsPerInch,
                        UnitOfLength, Subscriber
                FROM odbc_master
                WHERE
                LocNum IN {};'''.format(full_shiptos)
        df_name_forecast = pd.read_sql(sql, conn)
        # print(df_name_forecast[df_name_forecast.CustAcronym.str.contains('潍坊')])
        df_name_forecast = df_name_forecast.set_index('CustAcronym')
        return df_name_forecast


    def get_all_customer_from_sqlite(self):
        '''get_all_customer_from_sqlite'''
        conn = self.conn
        sql = '''select odbc_master.LocNum, odbc_master.CustAcronym,
                        odbc_master.PrimaryTerminal, odbc_master.SubRegion,
                        odbc_master.ProductClass, odbc_master.DemandType, odbc_master.GalsPerInch,
                        odbc_master.UnitOfLength
                 FROM odbc_master
              '''
        df_name_all = pd.read_sql(sql, conn).drop_duplicates().set_index('CustAcronym')
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
                   ORDER BY distanceKM ASC
               '''.format(shipto)

        cursor.execute(sql_line)
        results = cursor.fetchall()
        return results

    def get_view_demand_shiptos(self):
        '''获取 view_demand_data 中的所有 shiptos'''
        try:
            cur = self.cur
            table_name = 'view_demand_data'
            cur.execute('''SELECT DISTINCT CustAcronym FROM {}'''.format(table_name))
            shiptos = [str(row[0]) for row in cur.fetchall()]
        except Exception as e:
            print(e)
            shiptos = []
        return shiptos