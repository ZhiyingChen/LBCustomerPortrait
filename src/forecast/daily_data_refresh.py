from . import odbc_master
from .. import domain_object as do
from ..utils import field as fd
from ..utils import decorator
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


class DataRefresh:
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

    def refresh_earliest_part_data(
            self,
    ):
        """
        这些都是冬亮之前写的，单独抽出来，不改了
        """
        try:
            if odbc_master.check_refresh(table_name='odbc_master', cur=self.local_cur):
                print('今日 Master 已刷新！')
            else:
                odbc_master.refresh_odbcMasterData(self.local_cur, self.local_conn)

            if odbc_master.check_refresh(table_name='beforeReading', cur=self.local_cur):
                print('今日 before 已刷新！')
            else:
                odbc_master.refresh_beforeReading(self.local_conn)

            if odbc_master.check_refresh(table_name='odbc_MaxPayloadByShip2', cur=self.local_cur):
                print('今日 MaxPayloadByShip2 已刷新！')
            else:
                odbc_master.refresh_max_payload_by_ship2(cur=self.local_cur, conn=self.local_conn)

            if odbc_master.check_refresh(table_name='t4_t6_data', cur=self.local_cur):
                print('今日 t4_t6 已刷新')
            else:
                odbc_master.refresh_t4_t6_data(cur=self.local_cur, conn=self.local_conn)


        except Exception as e:
            print(e)
            odbc_master.refresh_odbcMasterData(cur=self.local_cur, conn=self.local_conn)
            odbc_master.refresh_beforeReading(conn=self.local_conn)
            odbc_master.refresh_max_payload_by_ship2(cur=self.local_cur, conn=self.local_conn)
            odbc_master.refresh_t4_t6_data(cur=self.local_cur, conn=self.local_conn)

    def get_lb_tele_shipto_dataframe(self):
        sql_line = '''
            Select CustomerProfile.LocNum, CustomerProfile.CustAcronym, DemandTypesinfo.DemandType, CustomerProfile.PrimaryTerminal
            FROM CustomerProfile
            LEFT JOIN DemandTypesinfo
            ON CustomerProfile.LocNum=DemandTypesinfo.LocNum
            LEFT JOIN CustomerTelemetry
            ON CustomerProfile.LocNum=CustomerTelemetry.LocNum
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

    def generate_initial_dtd_shipto_dict(self):
        df_shipto = self.get_lb_tele_shipto_dataframe()

        for idx, row in df_shipto.iterrows():
            dtd_shipto = do.DTDShipto(
                shipto=row['LocNum'],
                shipto_name=row['CustAcronym']
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

    @decorator.record_time_decorator('从SharePoint获取数据')
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

            TravelMatrixDefaultDuration AS duration,
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
                        duration,
                        distance
                    FROM PointToPoint
                    WHERE FromLoc LIKE '%{}%' AND ToLoc LIKE '%{}%';
                '''.format(from_loc, to_loc)
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
            if (dtd_shipto.primary_terminal_info.distance_km is None or
                    dtd_shipto.primary_terminal_info.duration_hours is None):
                # 从 odbc 的 PointToPoint 表中获取数据
                dtd_shipto.primary_terminal_info.distance_km, dtd_shipto.primary_terminal_info.duration_hours = (
                    self.get_distance_and_duration_from_local_p2p(from_loc, to_loc))

            # 补充信息给 sourcing terminal
            for sourcing_terminal, sourcing_terminal_info in dtd_shipto.sourcing_terminal_info_dict.items():
                from_loc = sourcing_terminal
                to_loc = shipto_id

                # 从 dtd_sharepoint_df 中获取数据
                sourcing_terminal_info.distance_km, sourcing_terminal_info.duration_hours = (
                    self.get_distance_and_duration_from_sharepoint(from_loc, to_loc))
                if sourcing_terminal_info.distance_km is None or sourcing_terminal_info.duration_hours is None:
                    # 从 odbc 的 PointToPoint 表中获取数据
                    sourcing_terminal_info.distance_km, sourcing_terminal_info.duration_hours = (
                        self.get_distance_and_duration_from_local_p2p(from_loc, to_loc))

    def output_primary_and_source_dtd_df(self):
        cols = ['LocNum', 'CustAcronym', 'DTType', 'DT', 'Distance', 'Duration', 'Rank', 'Frequency']
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
                }
            record_lt.append(primary_record)

            for sourcing_terminal, sourcing_terminal_info in dtd_shipto.sourcing_terminal_info_dict.items():
                source_record = {
                    'LocNum': shipto_id,
                    'CustAcronym': dtd_shipto.shipto_name,
                    'DTType': 'Sourcing',
                    'DT': sourcing_terminal,
                    'Distance': sourcing_terminal_info.distance_km
                    if sourcing_terminal_info.distance_km is not None else 'unknown',
                    'Duration': sourcing_terminal_info.duration_hours
                    if sourcing_terminal_info.duration_hours is not None else 'unknown',
                    'Rank': int(sourcing_terminal_info.rank),
                    'Frequency': int(sourcing_terminal_info.frequency)
                }
                record_lt.append(source_record)

        df_dtd = pd.DataFrame(record_lt, columns=cols)

        table_name = 'DTDInfo'
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

        df_dtd.to_sql(
            'DTDInfo',
            self.local_conn,
            if_exists='replace',
            index=False
        )

    def refresh_dtd_data(self):
        self.get_dtd_sharepoint_df()
        self.get_dtd_odbc_df()
        self.set_distance_and_duration_of_primary_and_source_terminal()
        self.output_primary_and_source_dtd_df()

    def refresh_cluster_data(self):
        pass

    def drop_local_table(self, table_name: str):
        self.local_cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
        self.local_conn.commit()

    def drop_local_tables(self):
        table_names = ['PointToPoint', 'DTDRecords']
        for table_name in table_names:
            self.drop_local_table(table_name)

    def refresh_all(self):
        self.refresh_earliest_part_data()

        self.generate_initial_dtd_shipto_dict()
        self.generate_source_terminal_info_for_shipto()

        self.refresh_dtd_data()
        self.refresh_cluster_data()

        self.drop_local_tables()


