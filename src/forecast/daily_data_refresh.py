from . import odbc_master
import pyodbc



class DataRefresh:
    def __init__(
            self,
            local_cur,
            local_conn
    ):
        self.local_cur = local_cur
        self.local_conn = local_conn

        server = 'LRPSQP05\\LRPSQP05'
        database = 'EU_LBLogist_RPT'

        self.odbc_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + database + '')

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

    def refresh_dtd_data(self):
        pass


    def refresh_all(self):
        self.refresh_earliest_part_data()