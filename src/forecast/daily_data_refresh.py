from . import odbc_master
from .. import domain_object as do
import pyodbc
import pandas as pd
import logging
from sqlalchemy import create_engine


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
    def get_dtd_sharepoint_df():
        from win32com.client import Dispatch
        import pywintypes

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
                LEFT(FromToName, InStr(FromToName, '-') - 1) AS FromName,
                Mid(FromToName, InStr(FromToName, '-') + 1) AS ToName,
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

        return dtd_sharepoint_df

    def refresh_dtd_data(self):
        dtd_sharepoint_df = self.get_dtd_sharepoint_df()


    def refresh_cluster_data(self):
        pass


    def refresh_all(self):
        self.refresh_earliest_part_data()

        self.generate_initial_dtd_shipto_dict()
        self.generate_source_terminal_info_for_shipto()

        self.refresh_dtd_data()
        self.refresh_cluster_data()
