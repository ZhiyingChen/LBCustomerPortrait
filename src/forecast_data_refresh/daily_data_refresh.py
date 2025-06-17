from . import odbc_master
from .. import domain_object as do
from ..utils import field as fd
from ..utils import decorator
from ..utils import functions as func
import pyodbc
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from win32com.client import Dispatch
import pywintypes
from multiprocessing import  Process, Queue
import multiprocessing
import os
import re
import datetime
from typing import Dict
import time

class ForecastDataRefresh:
    def __init__(
            self,
            local_cur,
            local_conn
    ):
        self.local_cur = local_cur
        self.local_conn = local_conn

        # 使用 SQLAlchemy 创建连接
        server = 'LRPSQP05\\LRPSQP05'
        database = 'EU_LBLogist_RPT'
        odbc_conn_string = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database}'
        self.odbc_conn = create_engine(f'mssql+pyodbc:///?odbc_connect={odbc_conn_string}')

        self.dtd_shipto_dict: Dict[str, do.DTDShipto] = dict()
        self.file_dict = dict()

    def get_lb_tele_shipto_dataframe(self):
        sql_line = '''
            Select CustomerProfile.LocNum, CustomerProfile.CustAcronym, CustomerProfile.PrimaryTerminal,
            (CustomerProfile.FullTrycockGals - LBCustProfile.TargetGalsUser) AS TRA
            FROM CustomerProfile
            LEFT JOIN DemandTypesinfo
            ON CustomerProfile.LocNum=DemandTypesinfo.LocNum
            LEFT JOIN CustomerTelemetry
            ON CustomerProfile.LocNum=CustomerTelemetry.LocNum
            LEFT JOIN LBCustProfile
            ON CustomerProfile.LocNum = LBCustProfile.LocNum
            WHERE
            CustomerProfile.State='CN' AND
            (CustomerProfile.Dlvrystatus='A' OR CustomerProfile.Dlvrystatus='T') AND
            ((CustomerTelemetry.Subscriber=3) OR
            (CustomerTelemetry.Subscriber=7) OR
            (CustomerProfile.PrimaryTerminal='XZ2' AND CustomerProfile.TelemetryFlag='True'))
        '''
        df_shipto = pd.read_sql(sql_line, self.odbc_conn)
        df_shipto['LocNum'] = df_shipto['LocNum'].astype(str)
        return df_shipto

    def get_max_payload_by_ship2(self, shipto: str):
        sql_line = f'''
            SELECT 
                ToLocNum,
                LicenseFill
            FROM 
                odbc_MaxPayloadByShip2
            WHERE 
                ToLocNum = '{shipto}'
            '''
        self.local_cur.execute(sql_line)

        results = self.local_cur.fetchall()
        for (loc_num, max_payload) in results:
            return max_payload
        return 0
    
    def generate_initial_dtd_shipto_dict(self):
        df_shipto = self.get_lb_tele_shipto_dataframe()

        for idx, row in df_shipto.iterrows():
            dtd_shipto = do.DTDShipto(
                shipto=row['LocNum'],
                shipto_name=row['CustAcronym'],
                tra=row['TRA'],
                max_payload=self.get_max_payload_by_ship2(row['LocNum']),
            )
            primary_terminal_info = do.PrimaryDTInfo(
                primary_terminal=row['PrimaryTerminal']
            )
            dtd_shipto.primary_terminal_info = primary_terminal_info
            self.dtd_shipto_dict[row['LocNum']] = dtd_shipto
        logging.info('loaded: {}'.format(len(self.dtd_shipto_dict)))

    def get_source_terminal_info_for_shipto_dataframe(self):
        shipto_list = list(self.dtd_shipto_dict.keys())
        sql_line = '''
            WITH GroupedData AS (
                SELECT 
                    ToLocNum,
                    SourceOfProduct,
                    COUNT(*) AS Frequency
                FROM Segment
                WHERE 
                    ToLocNum IN {}
                    AND ActualArrivalTime >= DATEADD(year, -1, GETDATE()) 
                GROUP BY 
                    ToLocNum, 
                    SourceOfProduct
            )
            SELECT 
                ToLocNum,
                SourceOfProduct,
                Frequency,
                Rank
            FROM (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ToLocNum 
                        ORDER BY Frequency DESC
                    ) AS Rank
                FROM GroupedData
            ) AS RankedData
            WHERE Rank <= 5
            ORDER BY 
                ToLocNum, 
                Frequency DESC;
        '''.format(tuple(shipto_list))

        df_source_terminal = pd.read_sql(sql_line, self.odbc_conn)
        df_source_terminal['ToLocNum'] = df_source_terminal['ToLocNum'].astype(str)
        return df_source_terminal

    def generate_source_terminal_info_for_shipto(self):
        df_source_terminal = self.get_source_terminal_info_for_shipto_dataframe()

        for idx, row in df_source_terminal.iterrows():
            if row['ToLocNum'] in self.dtd_shipto_dict:
                dtd_shipto = self.dtd_shipto_dict[row['ToLocNum']]
                sourcing_terminal_info = do.SourcingDTInfo(
                    sourcing_terminal=row['SourceOfProduct'],
                    rank=row['Rank'],
                    frequency=row['Frequency']
                )
                dtd_shipto.sourcing_terminal_info_dict[row['SourceOfProduct']] = sourcing_terminal_info
        logging.info('updated')

    @staticmethod
    def process_trips(df, locnums):
        # 初始化结果列表

        result_df = pd.DataFrame(columns=['CorporateIdn', 'NewTripIdn', 'LocNum', 'ToLocNum', 'DDER'])
        # 遍历每个目标 LocNum
        for locnum in locnums:
            # 找出包含该 LocNum 的所有行程
            trip_ids = df[df['ToLocNum'] == locnum][['CorporateIdn', 'NewTripIdn']].drop_duplicates()
            filtered_df = df.merge(trip_ids, on=['CorporateIdn', 'NewTripIdn'], how='inner')
            filtered_df = filtered_df[filtered_df['ToLocNum'] != locnum]
            # 按照 DDER 排序，并对 ToLocNum 去重
            filtered_df = filtered_df.sort_values(by=['DDER'], ascending=False).drop_duplicates(subset='ToLocNum')
            filtered_df['LocNum'] = locnum
            filtered_df = filtered_df.head(3)
            result_df = pd.concat([result_df, filtered_df], ignore_index=True)

        return result_df

    def generate_nearby_shipto_info_for_shipto(self):
        df_nearby_shipto = self.get_nearby_shipto_odbc_df()
        result_df = self.process_trips(df_nearby_shipto, [k for k, v in self.dtd_shipto_dict.items() if not v.is_full_load])

        for shipto_id, df_shipto in result_df.groupby('LocNum'):
            for idx, row in df_shipto.iterrows():
                dtd_shipto = self.dtd_shipto_dict[row['LocNum']]
                nearby_shipto_info = do.NearbyShipToInfo(
                    nearby_shipto=row['ToLocNum'],
                    shipto_name=row['ToCustAcronym'],
                    dder=row['DDER']
                )
                dtd_shipto.nearby_shipto_info_dict[row['ToLocNum']] = nearby_shipto_info
        logging.info('updated')

    def generate_shipto_info(self):
        self.generate_initial_dtd_shipto_dict()
        self.generate_source_terminal_info_for_shipto()
        self.generate_nearby_shipto_info_for_shipto()

    @staticmethod
    def output_dtd_sharepoint_df(queue):

        # 定义常量
        SERVERUrl = "https://approd.sharepoint.com/sites/tripinfor"
        list_name = "{77a6c173-402b-4154-b5aa-562175941b2c}"

        # 连接到SharePoint
        oConn = Dispatch('ADODB.Connection')
        oConn.ConnectionString = f'''
            Provider=Microsoft.ACE.OLEDB.16.0;
            WSS;IMEX=0;RetrieveIds=Yes;
            DATABASE={SERVERUrl};
            LIST={list_name}
        '''
        oConn.Open()

        # 执行查询
        sql = '''
           SELECT 
                LEFT(FromToName, InStr(FromToName, '-') - 1) AS FromLoc,
                Mid(FromToName, InStr(FromToName, '-') + 1) AS ToLoc,
                MileKMs,
                TimeHours
            FROM DTDRecords;
        '''
        table, _ = oConn.Execute(sql)

        # 获取列名
        colsName = [table.Fields(i).Name for i in range(len(table.Fields))]

        # 读取数据
        contentsList = []
        while not table.EOF:
            item_temp = [table.Fields(i).Value for i in range(len(table.Fields))]
            item_temp1 = [
                datetime.fromisoformat(str(pd.to_datetime(v.ctime())))
                if isinstance(v, pywintypes.TimeType) else v for v in
                item_temp]
            contentsList.append(item_temp1)
            table.MoveNext()

        # 关闭连接
        oConn.Close()
        del oConn

        # 转换为DataFrame
        dtd_sharepoint_df = pd.DataFrame(contentsList, columns=colsName)

        def clean_string(text: str):
            if not isinstance(text, str):
                return pd.NA
            # 删除 Terminal/Source/customer（不区分大小写）
            text = re.sub(r'(terminal|source|customer)', '', text, flags=re.IGNORECASE)
            # 删除所有中文字符（Unicode范围）
            text = re.sub(r'[\u4e00-\u9fff]+', '', text)
            # 检查是否为空或纯空格
            stripped_text = text.strip()
            return pd.NA if len(stripped_text) == 0 else stripped_text

        dtd_sharepoint_df['FromLoc'] = dtd_sharepoint_df['FromLoc'].apply(
            lambda x: clean_string(x)
        )
        dtd_sharepoint_df['ToLoc'] = dtd_sharepoint_df['ToLoc'].apply(
            lambda x: clean_string(x)
        )
        dtd_sharepoint_df = dtd_sharepoint_df.dropna()
        dtd_sharepoint_df.reset_index(drop=True, inplace=True)
        dtd_sharepoint_df.to_feather(
            os.path.join(fd.SHAREPOINT_TEMP_DIRECTORY, fd.DTD_FILE_NAME)
        )

        queue.put(0)

    @decorator.record_time_decorator('从SharePoint获取DTD数据')
    def get_dtd_sharepoint_df(self):
        queue = Queue()
        multiprocessing.freeze_support()
        p1 = Process(target=self.output_dtd_sharepoint_df, args=(queue,))
        p1.start()
        p1.join()
        s = queue.get()
        print('dtd_sharepoint_df refresh success: {}'.format(s))

        dtd_sharepoint_df = pd.read_feather(os.path.join(fd.SHAREPOINT_TEMP_DIRECTORY, fd.DTD_FILE_NAME))

        table_name = 'DTDRecords'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()
        # 导入数据
        dtd_sharepoint_df.to_sql(table_name, con=self.local_conn, if_exists='replace', index=False)

    @decorator.record_time_decorator('从ODBC PointToPoint 表获取数据')
    def get_dtd_odbc_df(self):
        # Step 1: 先把你需要的所有 (FromLoc, ToLoc) 组合做成DataFrame
        data = []
        for shipto_id, dtd_shipto in self.dtd_shipto_dict.items():
            to_loc = shipto_id
            primary_from = dtd_shipto.primary_terminal_info.primary_terminal
            data.append({'FromLoc': primary_from, 'ToLoc': to_loc})

            for sourcing_terminal in dtd_shipto.sourcing_terminal_info_dict:
                data.append({'FromLoc': sourcing_terminal, 'ToLoc': to_loc})

            for nearby_shipto in dtd_shipto.nearby_shipto_info_dict:
                data.append({'FromLoc': shipto_id, 'ToLoc': nearby_shipto})
                data.append({'FromLoc': nearby_shipto, 'ToLoc': shipto_id})
        df_pairs = pd.DataFrame(data).drop_duplicates()

        # Step 2: 从数据库提取清洗后的PointToPoint数据
        sql = f'''
        SELECT 
            CASE 
                WHEN LEFT(FromLoc, 1) = '0' THEN SUBSTRING(FromLoc, 2, LEN(FromLoc) - 1)
                WHEN LEFT(FromLoc, 1) = '2' THEN LTRIM(SUBSTRING(FromLoc, 10, LEN(FromLoc) - 9))
                ELSE FromLoc
            END AS FromLoc,

            CASE 
                WHEN LEFT(ToLoc, 1) = '0' THEN SUBSTRING(ToLoc, 2, LEN(ToLoc) - 1)
                WHEN LEFT(ToLoc, 1) = '2' THEN LTRIM(SUBSTRING(ToLoc, 10, LEN(ToLoc) - 9))
                ELSE ToLoc
            END AS ToLoc,

            FORMAT((TravelMatrixDefaultDuration)/60.0, 'N2') AS duration,
            TravelMatrixDefaultDistance AS distance

        FROM PointToPoint
        WHERE (TravelMatrixDefaultDuration IS NOT NULL OR TravelMatrixDefaultDistance IS NOT NULL)
        '''

        df_point_to_point = pd.read_sql(sql, self.odbc_conn)

        # Step 3: 内连接，只保留需要的组合
        df_result = pd.merge(df_pairs, df_point_to_point, on=['FromLoc', 'ToLoc'], how='left')

        # Step 4: drop table if exists
        table_name = 'PointToPoint'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

        # Step 5: 保存到本地数据库
        df_result.to_sql(table_name, con=self.local_conn, if_exists='replace', index=False)

        return df_result

    def get_distance_and_duration_from_sharepoint(self, from_loc: str, to_loc: str):
        sql_line = '''
            SELECT 
                MileKMs,
                TimeHours
            FROM DTDRecords
            WHERE FromLoc LIKE '%{}%' AND ToLoc LIKE '%{}%';
        '''.format(from_loc, to_loc)
        self.local_cur.execute(sql_line)

        results = self.local_cur.fetchall()
        for (mile_kms, time_hours) in results:
            return mile_kms, time_hours
        return None, None

    def get_distance_and_duration_from_local_p2p(self, from_loc: str, to_loc: str):
        sql_line = '''
                    SELECT 
                        distance,
                        duration
                    FROM PointToPoint
                    WHERE (FromLoc LIKE '%{}%' AND ToLoc LIKE '%{}%') 
                    OR (ToLoc LIKE '%{}%' AND FromLoc LIKE '%{}%');
                '''.format(from_loc, to_loc, to_loc, from_loc)
        self.local_cur.execute(sql_line)

        results = self.local_cur.fetchall()
        for (mile_kms, time_hours) in results:
            return mile_kms, time_hours
        return None, None

    @decorator.record_time_decorator('设置primary和source terminal的距离和时间')
    def set_distance_and_duration_of_primary_and_source_terminal(self):
        for shipto_id, dtd_shipto in self.dtd_shipto_dict.items():
            # 补充信息给 primary terminal
            from_loc = dtd_shipto.primary_terminal_info.primary_terminal
            to_loc = shipto_id

            # 从 dtd_sharepoint_df 中获取数据
            dtd_shipto.primary_terminal_info.distance_km, dtd_shipto.primary_terminal_info.duration_hours = (
                self.get_distance_and_duration_from_sharepoint(from_loc, to_loc))
            dtd_shipto.primary_terminal_info.distance_data_source = 'DTD'
            if (dtd_shipto.primary_terminal_info.distance_km is None or
                    dtd_shipto.primary_terminal_info.duration_hours is None):
                # 从 odbc 的 PointToPoint 表中获取数据
                dtd_shipto.primary_terminal_info.distance_km, dtd_shipto.primary_terminal_info.duration_hours = (
                    self.get_distance_and_duration_from_local_p2p(from_loc, to_loc))
                dtd_shipto.primary_terminal_info.distance_data_source = 'LBShell'

            # 补充信息给 sourcing terminal
            for sourcing_terminal, sourcing_terminal_info in dtd_shipto.sourcing_terminal_info_dict.items():
                from_loc = sourcing_terminal
                to_loc = shipto_id

                # 从 dtd_sharepoint_df 中获取数据
                sourcing_terminal_info.distance_km, sourcing_terminal_info.duration_hours = (
                    self.get_distance_and_duration_from_sharepoint(from_loc, to_loc))
                sourcing_terminal_info.distance_data_source = 'DTD'
                if sourcing_terminal_info.distance_km is None or sourcing_terminal_info.duration_hours is None:
                    # 从 odbc 的 PointToPoint 表中获取数据
                    sourcing_terminal_info.distance_km, sourcing_terminal_info.duration_hours = (
                        self.get_distance_and_duration_from_local_p2p(from_loc, to_loc))
                    sourcing_terminal_info.distance_data_source = 'LBShell'

    def output_primary_and_source_dtd_df(self):
        cols = ['LocNum', 'CustAcronym', 'DTType', 'DT', 'Distance', 'Duration', 'Rank', 'Frequency', 'DataSource']
        record_lt = []
        for shipto_id, dtd_shipto in self.dtd_shipto_dict.items():
            primary_record = {
                    'LocNum': shipto_id,
                    'CustAcronym': dtd_shipto.shipto_name,
                    'DTType': 'Primary',
                    'DT': dtd_shipto.primary_terminal_info.primary_terminal,
                    'Distance': dtd_shipto.primary_terminal_info.distance_km
                    if dtd_shipto.primary_terminal_info.distance_km is not None else 'unknown',
                    'Duration': dtd_shipto.primary_terminal_info.duration_hours
                    if dtd_shipto.primary_terminal_info.duration_hours is not None else 'unknown',
                    'DataSource': dtd_shipto.primary_terminal_info.distance_data_source,
                }
            record_lt.append(primary_record)

            for sourcing_terminal, sourcing_terminal_info in dtd_shipto.sourcing_terminal_info_dict.items():
                if sourcing_terminal_info.distance_km is None or sourcing_terminal_info.duration_hours is None:
                    continue
                source_record = {
                    'LocNum': shipto_id,
                    'CustAcronym': dtd_shipto.shipto_name,
                    'DTType': 'Sourcing',
                    'DT': sourcing_terminal,
                    'Distance': sourcing_terminal_info.distance_km,
                    'Duration': sourcing_terminal_info.duration_hours,
                    'Rank': int(sourcing_terminal_info.rank),
                    'Frequency': int(sourcing_terminal_info.frequency),
                    'DataSource': sourcing_terminal_info.distance_data_source
                }
                record_lt.append(source_record)

        df_dtd = pd.DataFrame(record_lt, columns=cols)

        now = datetime.datetime.now()
        df_dtd['refresh_date'] = now

        table_name = 'DTDInfo'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

        df_dtd.to_sql(
            'DTDInfo',
            self.local_conn,
            if_exists='replace',
            index=False
        )

    def prepare_dtd_data(self):
        self.get_dtd_sharepoint_df()
        self.get_dtd_odbc_df()

    def refresh_dtd_data(self):
        self.set_distance_and_duration_of_primary_and_source_terminal()
        self.output_primary_and_source_dtd_df()

    @decorator.record_time_decorator('从ODBC Segment 表获取 NearbyShipTo 数据')
    def get_nearby_shipto_odbc_df(self):
        non_full_load_shiptos = [
            s for s, dtd_shipto in self.dtd_shipto_dict.items()
            if not dtd_shipto.is_full_load
        ]
        sql_line = '''
        WITH SegmentWithFlags AS (
            SELECT *,
                   CASE WHEN StopType = 3 THEN 1 ELSE 0 END AS IsSplitPoint
            FROM Segment
            WHERE ActualArrivalTime >= DATEADD(YEAR, -1, GETDATE())
        ),
        SegmentWithGroup AS (
            SELECT *,
                   SUM(IsSplitPoint) OVER (
                       PARTITION BY CorporateIdn, TripIdn 
                       ORDER BY SegmentIdn 
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS GroupNum
            FROM SegmentWithFlags
        ),
        FinalResult AS (
            SELECT *,
                   CONCAT(TripIdn, '_', GroupNum + 1) AS NewTripIdn
            FROM SegmentWithGroup
        ),
        TripWithTwoDelv AS (
            SELECT 
                CorporateIdn,
                TripIdn,
                NewTripIdn
            FROM FinalResult
            GROUP BY CorporateIdn, TripIdn, NewTripIdn
            HAVING SUM(CASE WHEN StopType = 0 THEN 1 ELSE 0 END) = 2
        ),
        TripsWithTargetLoc AS (
            SELECT DISTINCT
                s.CorporateIdn,
                s.TripIdn,
                s.NewTripIdn,
                s.ToLocNum AS LocNum
            FROM 
                FinalResult s
            WHERE 
                s.ToLocNum IN {}  -- <<<<<< 多个 LocNum
        ),
        FinalWithDDER AS (
            SELECT 
                f.CorporateIdn,
                f.TripIdn,
                f.SegmentIdn,
                f.StopType,
                f.NewTripIdn,
                f.ToLocNum,
                c.CustAcronym AS ToCustAcronym,
                f.ActualArrivalTime,
                1 - (ISNULL(t.ActualDIPDeliveryComponent, 0) + ISNULL(t.ActualDIPClusteringComponent, 0)) / NULLIF(t.ActualDIPTotalCost, 0) AS DDER
            FROM FinalResult f
            JOIN TripWithTwoDelv twd
              ON f.CorporateIdn = twd.CorporateIdn
             AND f.TripIdn = twd.TripIdn
             AND f.NewTripIdn = twd.NewTripIdn
            LEFT JOIN Trip t
              ON f.CorporateIdn = t.CorporateIdn
             AND f.TripIdn = t.TripIdn
            INNER JOIN TripsWithTargetLoc twtl
            ON f.CorporateIdn = twtl.CorporateIdn
             AND f.NewTripIdn = twtl.NewTripIdn
            INNER JOIN CustomerProfile c
	          ON f.ToLocNum = c.LocNum 
        )
        SELECT DISTINCT*
        FROM FinalWithDDER
        WHERE StopType = 0 AND DDER > 0
        ORDER BY DDER DESC, CorporateIdn, TripIdn, SegmentIdn;
        '''.format(tuple(non_full_load_shiptos))
        df_nearby_shipto = pd.read_sql(sql_line, self.odbc_conn)
        df_nearby_shipto['ToLocNum'] = df_nearby_shipto['ToLocNum'].astype(str)

        return df_nearby_shipto

    @decorator.record_time_decorator('设置每个客户nearby shipto的距离')
    def set_nearby_shipto_distance_for_shipto(self):
        for shipto_id, dtd_shipto in self.dtd_shipto_dict.items():
            if dtd_shipto.is_full_load:
                continue

            for nearby_shipto_id in dtd_shipto.nearby_shipto_info_dict:
                nearby_shipto_info = dtd_shipto.nearby_shipto_info_dict[nearby_shipto_id]

                mile_kms, time_hours = self.get_distance_and_duration_from_sharepoint(shipto_id, nearby_shipto_id)
                source = 'DTD'
                if mile_kms is None or time_hours is None:
                    mile_kms, time_hours = self.get_distance_and_duration_from_sharepoint(shipto_id, nearby_shipto_id)
                if mile_kms is None or time_hours is None:
                    mile_kms, time_hours = self.get_distance_and_duration_from_local_p2p(shipto_id, nearby_shipto_id)
                    source = 'LBShell'
                if mile_kms is None or time_hours is None:
                    mile_kms, time_hours = self.get_distance_and_duration_from_local_p2p(nearby_shipto_id, shipto_id)
                    source = 'LBShell'
                nearby_shipto_info.distance_km = mile_kms
                nearby_shipto_info.distance_data_source = source

    def output_cluster_df(self):
        record_lt = []
        cols = ['LocNum', 'CustAcronym','ToLocNum', 'ToCustAcronym', 'distanceKM','DDER', 'DataSource']
        for shipto_id, dtd_shipto in self.dtd_shipto_dict.items():
            if dtd_shipto.is_full_load:
                record = {
                    'LocNum': shipto_id,
                    'CustAcronym': dtd_shipto.shipto_name,
                    'ToLocNum': '整车卸货',
                    'ToCustAcronym': '整车卸货',
                    'distanceKM': '',
                    'DDER': '',
                    'Rank': 1,
                }
                record_lt.append(record)
                continue
            for to_loc_num, nearby_shipto_info in dtd_shipto.nearby_shipto_info_dict.items():
                record = {
                    'LocNum': shipto_id,
                    'CustAcronym': dtd_shipto.shipto_name,
                    'ToLocNum': to_loc_num,
                    'ToCustAcronym': nearby_shipto_info.shipto_name,
                    'distanceKM': nearby_shipto_info.distance_km,
                    'DDER': nearby_shipto_info.dder,
                    'DataSource': nearby_shipto_info.distance_data_source
                }
                record_lt.append(record)
        df_cluster = pd.DataFrame(record_lt, columns=cols)
        df_cluster = df_cluster.sort_values(['LocNum', 'distanceKM']).reset_index(drop=True)
        now = datetime.datetime.now()
        df_cluster['refresh_date'] = now
        table_name = 'ClusterInfo'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

        df_cluster.to_sql(
            table_name, con=self.local_conn, if_exists='replace', index=False)


    def refresh_cluster_data(self):
        self.set_nearby_shipto_distance_for_shipto()
        self.output_cluster_df()

    def drop_local_table(self, table_name: str):
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

    def drop_local_tables(self):
        table_names = ['PointToPoint', 'DTDRecords']
        for table_name in table_names:
            self.drop_local_table(table_name)

    def get_delivery_window(self):
        sql_line = '''
         SELECT CP.LocNum, CP.DlvryMonFrom, CP.DlvryMonTo, CP.DlvryTueFrom,CP.DlvryTueTo,
                CP.DlvryWedFrom, CP.DlvryWedTo, CP.DlvryThuFrom, CP.DlvryThuTo,
                CP.DlvryFriFrom, CP.DlvryFriTo, CP.DlvrySatFrom, CP.DlvrySatTo,
                CP.DlvrySunFrom, CP.DlvrySunTo
         FROM CustomerProfile CP 
         Where CP.PrimaryTerminal like 'x%'
                AND CP.CustAcronym not like '1%'
                AND CP.DlvryStatus = 'A'
        '''

        delivery_window_df = pd.read_sql(sql_line, self.odbc_conn)
        delivery_window_df['LocNum'] = delivery_window_df['LocNum'].astype(str)

        delivery_window_df[delivery_window_df.columns[1:]] = delivery_window_df.loc[:, delivery_window_df.columns[1:]].applymap(
            lambda x: pd.to_datetime(x).strftime('%H:%M'))

        def df_to_delivery_times(df):
            delivery_times = {
                "周一": (df['DlvryMonFrom'].iloc[0], df['DlvryMonTo'].iloc[0]),
                "周二": (df['DlvryTueFrom'].iloc[0], df['DlvryTueTo'].iloc[0]),
                "周三": (df['DlvryWedFrom'].iloc[0], df['DlvryWedTo'].iloc[0]),
                "周四": (df['DlvryThuFrom'].iloc[0], df['DlvryThuTo'].iloc[0]),
                "周五": (df['DlvryFriFrom'].iloc[0], df['DlvryFriTo'].iloc[0]),
                "周六": (df['DlvrySatFrom'].iloc[0], df['DlvrySatTo'].iloc[0]),
                "周日": (df['DlvrySunFrom'].iloc[0], df['DlvrySunTo'].iloc[0])
            }
            return delivery_times

        ordinary_delivery_text_by_shipto = dict()
        for shipto, delivery_df in delivery_window_df.groupby('LocNum'):
            delivery_times = df_to_delivery_times(delivery_df)
            ordinary_delivery_window_text = func.summarize_delivery_times(delivery_times)
            ordinary_delivery_text_by_shipto[shipto] = ordinary_delivery_window_text

        return ordinary_delivery_text_by_shipto


    def get_restricted_delivery_periods(self):
        sql_line = '''
            SELECT 
            RDP.LocNum,
            CP.CustAcronym,
            RDP.FromDateTime,
            RDP.ToDateTime,
            RDP.Comment
            FROM RestrictedDeliveryPeriods AS RDP
            INNER JOIN CustomerProfile CP on RDP.LocNum = CP.LocNum
            Where CP.PrimaryTerminal LIKE 'x%'
            AND CP.CustAcronym NOT LIKE '1%'
            AND CP.DlvryStatus = 'A'
            AND RDP.ToDateTime >= GETDATE()
        '''

        restricted_delivery_periods_df = pd.read_sql(sql_line, self.odbc_conn)
        restricted_delivery_periods_df['LocNum'] = restricted_delivery_periods_df['LocNum'].astype(str)

        time_cols = ['FromDateTime', 'ToDateTime']
        restricted_delivery_periods_df[time_cols] = restricted_delivery_periods_df[time_cols].applymap(
                lambda x: pd.to_datetime(x).strftime('%m-%d')
        )
        restricted_delivery_periods_by_shipto = dict()
        for shipto, delivery_df in restricted_delivery_periods_df.groupby('LocNum'):
            text_lt = []
            idx = 1
            for _, row in delivery_df.iterrows():
                from_date = row['FromDateTime']
                to_date = row['ToDateTime']
                comment = row['Comment']
                text = '{}. {}-{}'.format(idx,from_date, to_date)
                if comment:
                    text += '({})'.format(comment)
                text_lt.append(text)
                idx += 1

            restricted_delivery_periods_by_shipto[shipto] = '; '.join(text_lt)
        return restricted_delivery_periods_by_shipto

    def refresh_delivery_window_and_restricted_delivery_periods(self):
        ordinary_delivery_text_by_shipto = self.get_delivery_window()
        restricted_delivery_periods_by_shipto = self.get_restricted_delivery_periods()

        shipto_lt = list(
            set(ordinary_delivery_text_by_shipto.keys()) | set(restricted_delivery_periods_by_shipto.keys())
        )

        cols = [
            'LocNum',
            'OrdinaryDeliveryWindow',
            'RestrictedDeliveryPeriods'
        ]
        record_lt = []
        for shipto in shipto_lt:
            ordinary_delivery_window_text = ordinary_delivery_text_by_shipto.get(shipto, '')
            restricted_delivery_periods_text = restricted_delivery_periods_by_shipto.get(shipto, '')
            record = {
                'LocNum': shipto,
                'OrdinaryDeliveryWindow': ordinary_delivery_window_text,
                'RestrictedDeliveryPeriods': restricted_delivery_periods_text
            }
            record_lt.append(record)
        df_delivery_window = pd.DataFrame(record_lt, columns=cols)

        now = datetime.datetime.now()
        df_delivery_window['refresh_date'] = now
        table_name = 'DeliveryWindowInfo'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        df_delivery_window.to_sql(
            table_name, con=self.local_conn, if_exists='replace', index=False)
        self.local_conn.commit()

    def get_call_log(self):
        sql_line = '''
        WITH RankedCalls AS (
            SELECT 
                CL.CallLogID,
                CL.LocNum,
                CL.DateEntered,
                CL.Comment,
                ROW_NUMBER() OVER (PARTITION BY CL.LocNum ORDER BY CL.DateEntered DESC) AS rn
            FROM CustomerCallLog CL
            INNER JOIN CustomerProfile CP ON CL.LocNum = CP.LocNum
            WHERE 
              CP.PrimaryTerminal LIKE 'x%'
              AND CP.CustAcronym NOT LIKE '1%'
              AND CP.DlvryStatus = 'A'
        )
        SELECT 
            CallLogID,
            LocNum,
            DateEntered,
            Comment
        FROM RankedCalls
        WHERE rn <= 2
        '''
        call_log_df = pd.read_sql(sql_line, self.odbc_conn)
        call_log_df['LocNum'] = call_log_df['LocNum'].astype(str)
        call_log_df['DateEntered'] = pd.to_datetime(call_log_df['DateEntered'])
        call_log_df = call_log_df.sort_values(by=['LocNum', 'DateEntered'], ascending=False).reset_index(drop=True)

        call_log_dict = dict()

        for shipto, df_shipto in call_log_df.groupby('LocNum'):
            idx = 1
            text_lt = []
            for _, row in df_shipto.iterrows():

                text = '{}. {}'.format(idx, row['Comment'])
                text_lt.append(text)
                idx += 1
            call_log_dict[shipto] = '; '.join(text_lt)
        return call_log_dict


    def refresh_call_log(self):
        '''
        更新最新联络
        '''
        call_log_dict = self.get_call_log()

        cols = [
            'LocNum',
            'CallLog'
        ]
        record_lt = []

        for shipto, call_log_text in call_log_dict.items():
            record = {
                'LocNum': shipto,
                'CallLog': call_log_text
            }
            record_lt.append(record)
        df_call_log = pd.DataFrame(record_lt, columns=cols)
        now = datetime.datetime.now()
        df_call_log['refresh_date'] = now
        table_name = 'CallLogInfo'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        df_call_log.to_sql(
            table_name, con=self.local_conn, if_exists='replace', index=False)
        self.local_conn.commit()


    def refresh_lb_daily_data(self):
        """
        这些都是冬亮之前写的，单独抽出来，不改了
        """
        odbc_master.refresh_odbcMasterData(cur=self.local_cur, conn=self.local_conn)
        odbc_master.refresh_beforeReading(conn=self.local_conn)
        odbc_master.refresh_max_payload_by_ship2(cur=self.local_cur, conn=self.local_conn)
        odbc_master.refresh_t4_t6_data(cur=self.local_cur, conn=self.local_conn)
        odbc_master.refresh_DeliveryWindow(cur=self.local_cur, conn=self.local_conn)
        '''
        新增 生产计划，delivery window，最新联络，特殊备注 相关的
        '''
        self.refresh_delivery_window_and_restricted_delivery_periods()
        self.refresh_call_log()

        '''
        以下是新增的刷新代码，增加 dtd 和 cluster 相关的
        '''
        self.generate_shipto_info()
        self.prepare_dtd_data()
        self.refresh_dtd_data()
        self.refresh_cluster_data()
        self.drop_local_tables()

    def refresh_all(self):
        self.refresh_lb_daily_data()
        self.refresh_lb_hourly_data()

    # refresh hourly data
    def refresh_lb_hourly_data(self):
        self.get_filename()
        self.refresh_history_data()
        self.refresh_forecast_data()
        self.refresh_forecast_beforeTrip_data()
        self.refresh_fe()
        self.refresh_view_demand_data()

    def get_filename(
            self,
            path1='//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling',
            purpose='LB_LCT'
    ):
        '''get filename and backup filename, modifid at 20220520'''
        file_dict = {}
        if purpose == 'autoScheduling':
            # 仅供 成都 Terminal
            name = 'chengdu'
            forecast_file = os.path.join(path1, 'Sample forecasted reading.csv')
            history_file = os.path.join(path1, 'Sample history reading.csv')
            drop_file = os.path.join(path1, 'Sample forecasted reading_drop.csv')
            file_dict[name] = [forecast_file, history_file, drop_file]
        else:
            # For Forecast Project
            regions = ['LB_LCT', 'CNS', 'CNCE', 'CNNW']
            path2 = os.path.join(path1, 'ForecastingInputOutput')
            for region in regions:
                file_dict[region] = []
                # 主文件名夹
                path3 = os.path.join(path2, region)
                # 备份文件名夹
                path_backup = os.path.join(path3, 'Backup')
                # 三个只要文件名：预测,历史,drop信息
                # 2024-08-30 新增
                if region == 'LB_LCT':
                    files = ['Sample_forecasted_reading.csv',
                             'Sample_history_reading.csv', 'Sample_forecasted_reading_drop.csv']
                else:
                    files = ['Sample forecasted reading.csv',
                             'Sample history reading.csv', 'Sample forecasted reading_drop.csv']
                for file in files:
                    if os.path.exists(os.path.join(path3, file)):
                        filename = os.path.join(path3, file)
                    else:
                        filename = None
                    # 主文件名加入列表
                    file_dict[region].append(filename)
                    if os.path.exists(os.path.join(path_backup, file)):
                        filename_back = os.path.join(path_backup, file)
                    else:
                        filename_back = None
                    # 备份文件名加入列表
                    file_dict[region].append(filename_back)
        self.file_dict = file_dict

    def refresh_history_data(self):
        '''刷新 历史数据,区分AS用,还是 Forecasting 用'''
        cur = self.local_cur
        conn = self.local_conn
        file_dict = self.file_dict

        start_time = time.time()
        if len(file_dict) == 1:
            # 用于 AS
            filename = file_dict['chengdu'][1]
            df_history = pd.read_csv(filename)
        else:
            # 用于 Forecasting
            df_history = pd.DataFrame()
            for region in file_dict:
                filename = file_dict[region][2]
                filename_back = file_dict[region][3]
                try:
                    df_temp = pd.read_csv(filename)
                    if len(df_temp) <= 10000:
                        # 说明数据正在写,有缺失
                        print('file {} is missing data, use backup.'.format(filename))
                        df_temp = pd.read_csv(filename_back)
                except Exception as e:
                    print('cannot read file {} , use backup.'.format(filename), e)
                    df_temp = pd.read_csv(filename_back)
                # 20220622 modify
                if len(df_temp) > 0:
                    df_history = pd.concat([df_history, df_temp], ignore_index=True)
        end_time = time.time()
        print('refresh history {} seconds'.format(round(end_time - start_time)))
        df_history.ReadingDate = pd.to_datetime(df_history.ReadingDate, format='mixed')
        df_history = df_history.sort_values(['LocNum', 'ReadingDate']).reset_index(drop=True)
        df_history.Reading_Gals = (df_history.Reading_Gals).round()
        table_name = 'historyReading'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df_history[['LocNum', 'ReadingDate', 'Reading_Gals']].to_sql(
            table_name, con=conn, if_exists='replace', index=False)

    def refresh_forecast_data(self):
        '''刷新 预测数据,区分AS用,还是 Forecasting 用'''
        cur = self.local_cur
        conn = self.local_conn
        file_dict = self.file_dict

        start_time = time.time()
        if len(file_dict) == 1:
            # 用于 AS
            filename = file_dict['chengdu'][0]
            df_forecast = pd.read_csv(filename)
        else:
            # 用于 Forecasting
            df_forecast = pd.DataFrame()
            for region in file_dict:
                filename = file_dict[region][0]
                filename_back = file_dict[region][1]
                try:
                    df_temp = pd.read_csv(filename)
                    if len(df_temp) <= 1500:
                        print('file {} is missing data, use backup.'.format(filename))
                        # 说明数据正在写,有缺失
                        df_temp = pd.read_csv(filename_back)
                except Exception:
                    print('cannot read file {} , use backup.'.format(filename))
                    df_temp = pd.read_csv(filename_back)
                if region == 'LB_LCT':
                    # 2024-09-02 新增防止 DOL 中 混入 LCT 数据影响
                    lb_lct_shiptos = list(df_temp.LocNum.unique())
                # print(filename, df_temp.shape)
                # df_temp.to_excel('{}.xlsx'.format(name))
                # 20220622 modify
                # df_forecast = df_forecast.append(df_temp)
                if len(df_temp) > 0:
                    df_forecast = pd.concat([df_forecast, df_temp], ignore_index=True)
        end_time = time.time()
        print('refresh forecast_data_refresh {} seconds'.format(round(end_time - start_time)))
        df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
        # 2024-09-02 新增：去除 DOL 中 的 LCT 数据
        f1 = df_forecast.Next_hr.isna()
        f2 = df_forecast.LocNum.isin(lb_lct_shiptos)
        # 这一步的意思是说： 如果是一个 LCT 客户，并且 Next_hr 为空，就剔除出去；
        df_forecast = df_forecast[~(f1 & f2)]
        df_forecast = df_forecast.sort_values(['LocNum', 'Next_hr'])
        df_forecast.Forecasted_Reading = (df_forecast.Forecasted_Reading).astype(float).round()
        use_cols = ['LocNum', 'Next_hr', 'Hourly_Usage_Rate', 'Forecasted_Reading', 'RiskGals',
                    'TargetRefillDate', 'TargetRiskDate', 'TargetRunoutDate']
        df_forecast1 = df_forecast.loc[df_forecast.Forecasted_Reading.notna(
        ), use_cols].reset_index(drop=True).copy()
        # df_forecast1.to_excel('aaa.xlsx')
        table_name = 'forecastReading'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df_forecast1.to_sql(table_name, con=conn, if_exists='replace', index=False)

    def refresh_forecast_beforeTrip_data(self):
        '''刷新 送货前数据,区分AS用,还是 Forecasting 用'''
        cur = self.local_cur
        conn = self.local_conn
        file_dict = self.file_dict

        start_time = time.time()
        if len(file_dict) == 1:
            # 用于 AS
            filename = file_dict['chengdu'][2]
            df_forecast = pd.read_csv(filename)
        else:
            # 用于 Forecasting
            df_forecast = pd.DataFrame()
            for region in file_dict:
                filename = file_dict[region][4]
                filename_back = file_dict[region][5]
                try:
                    df_temp = pd.read_csv(filename)
                except Exception:
                    print('cannot read file {} , use backup.'.format(filename))
                    df_temp = pd.read_csv(filename_back)
                # 20220622 modify
                # df_forecast = df_forecast.append(df_temp)
                if len(df_temp) > 0:
                    df_forecast = pd.concat([df_forecast, df_temp], ignore_index=True)
        end_time = time.time()
        print('refresh drop {} seconds'.format(round(end_time - start_time)))
        df_forecast.Next_hr = pd.to_datetime(df_forecast.Next_hr)
        df_forecast = df_forecast.sort_values(['LocNum', 'Next_hr'])
        df_forecast.Forecasted_Reading = (df_forecast.Forecasted_Reading).astype(float).round()
        df_forecast1 = df_forecast.loc[df_forecast.Forecasted_Reading.notna(
        ), ['LocNum', 'Next_hr', 'Forecasted_Reading']].reset_index(drop=True).copy()
        table_name = 'forecastBeforeTrip'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df_forecast1.to_sql(table_name, con=conn, if_exists='replace', index=False)

    def refresh_fe(self):
        '''刷新 forecast_data_refresh error'''
        # 2023-03-06 dongliang modified
        cur = self.local_cur
        conn = self.local_conn

        filepath = '//shangnt\\Lbshell\\PUAPI\\PU_program\\automation\\autoScheduling\\ForecastingInputOutput\\ErrorRecording'
        filename = os.path.join(filepath, 'Error Result.csv')
        df_fe = pd.read_csv(filename)
        df_fe = df_fe[df_fe.AverageError_SEH.notna()].reset_index(drop=True)
        if len(df_fe) > 0:
            df_fe['AverageError'] = df_fe.apply(lambda row: min(row['AverageError_SEH'], row['AverageError_ARIMA']),
                                                axis=1)
        else:
            df_fe['AverageError'] = None
        use_cols = ['LocNum', 'AverageError']
        df_fe = df_fe[use_cols]
        table_name = 'forecastError'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df_fe.to_sql(table_name, con=conn, if_exists='replace', index=False)

    def refresh_view_demand_data(self):
        '''刷新 view_demand_data'''
        cur = self.local_cur
        conn = self.local_conn

        start_time = time.time()
        filepath = r'\\shangnt\lbshell\PUAPI\PU_program\automation'
        filename = os.path.join(filepath, 'view_demand.xlsx')

        df_demand = pd.read_excel(filename)
        df_demand['LocNum'] = df_demand['LocNum'].astype(str)
        cols = [
            'LocNum', 'CustAcronym'
        ]
        df_demand = df_demand[cols]
        table_name = 'view_demand_data'
        cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        conn.commit()
        df_demand.to_sql(table_name, con=conn, if_exists='replace', index=False)
        end_time = time.time()
        print('refresh view_demand_data {} seconds'.format(round(end_time - start_time)))
