import pandas as pd
import pyodbc
from datetime import datetime
from datetime import timedelta
from win32com.client import Dispatch
import pywintypes
from multiprocessing import  Process, Queue
import multiprocessing
import os
from ..utils import field as fd

# 连接ODBC

def check_refresh(table_name: str, cur):
    sql = '''select refresh_date from {}'''.format(table_name)
    cur.execute(sql)
    refresh_time = pd.to_datetime(cur.fetchone()[0])
    if refresh_time.date() == datetime.now().date() and refresh_time.hour > 6:
        print('今日{}已刷新！'.format(table_name))
        return True
    return False

def check_refresh_deliveryWindow(cur, conn):
    '''检查并刷新 odbc_DeliveryWindow'''
    table_name = 'odbc_DeliveryWindow'
    sql = '''select refresh_date from {}'''.format(table_name)
    try:
        cur.execute(sql)
        refresh_time = pd.to_datetime(cur.fetchone()[0])
        if (refresh_time.date() == datetime.now().date() and refresh_time.hour > 6):
            print('今日 odbc_DeliveryWindow 已刷新！')
        else:
            refresh_DeliveryWindow(cur, conn)
        # print(x, type(x))
    except Exception:
        refresh_DeliveryWindow(cur, conn)

def connect_odbc(server, database):
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+'')
    return cnxn


def odbc_masterData():
    # odbc 基础数据
    # 以下内容为2021-11-29 修改
    # 2024-09-03 新增 CustomerTelemetry.Subscriber
    server = 'LRPSQP05\\LRPSQP05'
    database = 'EU_LBLogist_RPT'
    cnxn = connect_odbc(server, database)

    sql = '''SELECT
            CustomerProfile.LocNum,
            LBCustProfile.CustAcronym,
            CustomerProfile.TankAcronym,
            CustomerProfile.FullTrycockGals, CustomerProfile.FullTrycockInches,
            CustomerProfile.TypicalTrycock,  LBCustProfile.TargetGalsUser,
            CustomerProfile.RunoutInch, CustomerProfile.RunoutGals,
            CustomerProfile.GalsPerInch, CustomerProfile.DemandType,
            CustomerProfile.VehicleSize, CustomerProfile.ProductClass, CustomerProfile.UnitOfLength,
            CustomerProfile.ClusteringZone,CustomerProfile.PrimaryTerminal,
            rtrim(CustomerProfile.Name) as'CustFullName',
            ltrim(rtrim(right(Terminal.Name,4))) as 'SubRegion',
            CustomerProfile.TelemetryFlag,
            CustomerTelemetry.Subscriber,
            CustomerForecastParams.TargetWODlvryUser, 
            CustomerForecastParams.FcstRunoutDate,
            DATEDIFF(MINUTE, CustomerForecastParams.TargetWODlvryUser, CustomerForecastParams.FcstRunoutDate) / 60 AS TRRO
        FROM
            EU_LBLogist_Rpt.dbo.LBCustProfile LBCustProfile,
            EU_LBLogist_Rpt.dbo.CustomerProfile
            LEFT JOIN CustomerTelemetry
            ON CustomerProfile.LocNum=CustomerTelemetry.LocNum
            LEFT JOIN CustomerForecastParams
            ON CustomerProfile.LocNum = CustomerForecastParams.LocNum,
            EU_LBLogist_Rpt.dbo.Terminal
        WHERE
            LBCustProfile.LocNum = CustomerProfile.LocNum
            AND  CustomerProfile.PrimaryTerminal=Terminal.CorporateIdn
            AND (CustomerProfile.PrimaryTerminal like 'x%')
            AND (CustomerProfile.CustAcronym not like '1%')
            AND (CustomerProfile.DlvryStatus = 'A')
        '''
    now = datetime.now()
    df = pd.read_sql(sql, cnxn)
    df['refresh_date'] = now
    df.CustAcronym = df.CustAcronym.str.strip()
    print(df.shape)
    return df


def odbc_Vehicle():
    # odbc 基础数据
    server = 'LRPSQP05\\LRPSQP05'
    database = 'EU_LBLogist_RPT'
    cnxn = connect_odbc(server, database)
    sql = '''SELECT CarrierVehicle.VehicleIdn, CarrierVehicle.VehicleNumber,
                        CarrierVehicle.LicensePlateNumber, CarrierVehicle.CorporateIdn,
                        CarrierVehicle.VehicleType, CarrierVehicle.ProductClass,
                        CarrierVehicle.TrailerTargetFillLvl, CarrierVehicle.MaxLegalWeight,
                        CarrierVehicle.WaterVolumeMea, CarrierVehicle.UnladenWeight,
                        CarrierVehicle.TareWt,  CarrierVehicle.Size,
                        CarrierVehicle.KingpinWeight, CarrierVehicle.HeelGals,
                        CarrierVehicle.CatchPrimeGals, CarrierVehicle.Comments,
                        CarrierVehicle.CoolDownTime, CarrierVehicle.PumpRate, CarrierVehicle.EquipCpcty
                        FROM EU_LBLogist_Rpt.dbo.CarrierVehicle CarrierVehicle
                        WHERE (CarrierVehicle.Status Like 'A')
                        AND (CarrierVehicle.CorporateIdn Like 'X%')
						AND (CarrierVehicle.VehicleType not in  (79, 82) )
            '''
    now = datetime.now()
    df = pd.read_sql(sql, cnxn)
    df['refresh_date'] = now
    df.VehicleIdn = df.VehicleIdn.str.strip()
    print(df.shape)
    return df


def odbc_DeliveryWindow():
    # odbc 基础数据
    server = 'LRPSQP05\\LRPSQP05'
    database = 'EU_LBLogist_RPT'
    cnxn = connect_odbc(server, database)
    sql = '''SELECT CP.LocNum, CP.DlvryMonFrom, CP.DlvryMonTo, CP.DlvryTueFrom,CP.DlvryTueTo,
                CP.DlvryWedFrom, CP.DlvryWedTo, CP.DlvryThuFrom, CP.DlvryThuTo,
                CP.DlvryFriFrom, CP.DlvryFriTo, CP.DlvrySatFrom, CP.DlvrySatTo,
                CP.DlvrySunFrom, CP.DlvrySunTo,
                AD.DlvryMonFrom1, AD.DlvryMonTo1,
                AD.DlvryTueFrom1, AD.DlvryTueTo1, AD.DlvryWedFrom1, AD.DlvryWedTo1,
                AD.DlvryThuFrom1, AD.DlvryThuTo1, AD.DlvryFriFrom1, AD.DlvryFriTo1,
                AD.DlvrySatFrom1, AD.DlvrySatTo1, AD.DlvrySunFrom1, AD.DlvrySunTo1
         FROM AlternateDlvry AD
         inner join CustomerProfile CP on AD.LocNum = CP.LocNum
         Where CP.PrimaryTerminal like 'x%'
                AND CP.CustAcronym not like '1%'
                AND CP.DlvryStatus = 'A';
            '''
    now = datetime.now()
    df = pd.read_sql(sql, cnxn)
    df['refresh_date'] = now
    print(df.shape)
    return df


def get_last_fiscal_year_start_date(current_date=None):
    if current_date is None:
        current_date = datetime.now()

    # 财年从10月1日开始
    fiscal_year_start_month = 10
    fiscal_year_start_day = 1

    # 如果当前日期在10月1日之前，则当前财年从上一年的10月1日开始
    if (current_date.month < fiscal_year_start_month) or \
            (current_date.month == fiscal_year_start_month and current_date.day < fiscal_year_start_day):
        last_fiscal_year_start = datetime(current_date.year - 1, fiscal_year_start_month, fiscal_year_start_day)
    else:
        last_fiscal_year_start = datetime(current_date.year - 1, fiscal_year_start_month,
                                          fiscal_year_start_day)

    return pd.to_datetime(last_fiscal_year_start)

def odbc_segment():
    # odbc 基础数据
    server = 'LRPSQP05\\LRPSQP05'
    database = 'EU_LBLogist_RPT'
    cnxn = connect_odbc(server, database)

    last_fiscal_year_start_date = get_last_fiscal_year_start_date()

    sql = '''
       SELECT Segment.ToLocNum, CP.ProductClass, Segment.CorporateIdn, Segment.AssignedTrailerIdn
        FROM Segment
        RIGHT JOIN 
        (
            SELECT 
                ToLocNum, 
                CorporateIdn
                FROM (
                    SELECT 
                        Seg.ToLocNum, 
                        Seg.CorporateIdn, 
                        COUNT(*) AS count,
                        ROW_NUMBER() OVER (PARTITION BY Seg.ToLocNum ORDER BY COUNT(*) DESC) AS rn
                    FROM Segment Seg
                    INNER JOIN LBCustProfile CP
                    ON CP.LocNum = Seg.ToLocNum
                    WHERE ActualDepartTime > '{}' AND CP.State = 'CN'
                    GROUP BY Seg.ToLocNum, Seg.CorporateIdn
                ) subquery
            WHERE rn = 1
        ) subquery2
        ON subquery2.ToLocNum = Segment.ToLocNum AND subquery2.CorporateIdn = Segment.CorporateIdn
        LEFT JOIN LBCustProfile CP
        ON CP.LocNum = Segment.ToLocNum
        WHERE Segment.AssignedTrailerIdn != 'NULL'
    '''.format(
        last_fiscal_year_start_date.strftime('%Y-%m-%d')
    )
    df = pd.read_sql(sql, cnxn)
    return df

def sharepoint_equipment_list(queue):
   try:
        # 定义常量
        SERVERUrl = "https://approd.sharepoint.com/sites/CN_IG_Fleet"
        list_name = "{3b03a9a2-1a4d-4438-93f7-8ed5db3838cb}"  # 你需要替换为实际的List GUID

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
        # EquipClass 51 或者 52 为大车
        sql = '''
            SELECT LBID, Product, LicenseFill, CarrierID
            FROM Equipment1
            WHERE (EquipClass = 51 OR EquipClass = 52)
            AND Status = 'A'
            AND (CarrierID = 'APEP' OR (CarrierID <> 'APEP' AND LBID LIKE '%QL%'))
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
        equipment_list_df = pd.DataFrame(contentsList, columns=colsName)

        equipment_list_df = equipment_list_df.rename(columns={
            'LBID': 'AssignedTrailerIdn', 'Product': 'ProductClass',
        })
        equipment_list_df.to_feather(
            os.path.join(
                fd.SHAREPOINT_TEMP_DIRECTORY, fd.EQUIPMENT_FILE_NAME
            )
        )
        print('Equipment List data is ready.')
        queue.put(0)
   except Exception as e:
        print(e)
        queue.put(1)


def refresh_odbcMasterData(cur, conn):
    '''上传 odbc 的数据到 sqlite'''
    df = odbc_masterData()
    table_name = 'odbc_master'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    # 导入数据
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC Customer data is ready.')


def refresh_odbcVehicle(cur, conn):
    '''上传 odbc 的数据到 sqlite'''
    df = odbc_Vehicle()
    table_name = 'odbc_Vehicle'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    # 导入数据
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC Vehicle data is ready.')


def refresh_DeliveryWindow(cur, conn):
    '''上传 odbc 的数据到 sqlite'''
    df = odbc_DeliveryWindow()
    table_name = 'odbc_DeliveryWindow'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    # 导入数据
    df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC DeliveryWindow data is ready.')


def refresh_max_payload_by_ship2(cur, conn):
    # 获取 odbc 中 segment 的数据
    segment_df = odbc_segment()

    # 获取sharepointList中 equipmentList 的数据
    queue = Queue()
    multiprocessing.freeze_support()

    p1 = Process(target=sharepoint_equipment_list, args=(queue, ))  # 实例化进程对象
    p1.start()

    equipment_list_df = pd.read_feather(os.path.join(fd.SHAREPOINT_TEMP_DIRECTORY, fd.EQUIPMENT_FILE_NAME))

    merged_df = pd.merge(segment_df, equipment_list_df,
                         on=['AssignedTrailerIdn', 'ProductClass'], how='left')
    merged_df['LicenseFill'] = merged_df['LicenseFill'].fillna(0)

    # 创建一个空的 DataFrame 来存储结果
    new_df = pd.DataFrame()
    # 遍历每组 ['ToLocNum', 'ProductClass', 'CorporateIdn']
    for key, group in merged_df.groupby(['ToLocNum', 'ProductClass', 'CorporateIdn']):
        # 检查是否存在 APEP 车
        apep_group = group[group['CarrierID'] == 'APEP']
        if not apep_group.empty:
            # 取 APEP 车中 LicenseFill 最大的
            max_apep = apep_group.loc[apep_group['LicenseFill'].idxmax()]
            new_df = pd.concat([new_df, max_apep.to_frame().T], ignore_index=True)
        else:
            # 筛选出 LBID 包含 'QL' 的车
            ql_group = group[group['AssignedTrailerIdn'].str.contains('QL')]
            if not ql_group.empty:
                # 取 LBID 包含 'QL' 的车中 LicenseFill 最大的
                max_ql = ql_group.loc[ql_group['LicenseFill'].idxmax()]
                new_df = pd.concat([new_df, max_ql.to_frame().T], ignore_index=True)

    # 重置索引
    new_df.reset_index(drop=True, inplace=True)
    new_df['ToLocNum'] = new_df['ToLocNum'].astype(str)
    now = datetime.now()
    new_df['refresh_date'] = now

    table_name = 'odbc_MaxPayloadByShip2'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    # 导入数据
    new_df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC max_payload_by_ship2 data is ready.')

def get_LB_TeleShiptos(cnxn):
    '''查询所有 LB 远控 shiptos'''
    sql = '''Select CustomerProfile.LocNum,
                    DemandTypesinfo.DemandType
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
                    (CustomerProfile.PrimaryTerminal='XZ2' AND CustomerProfile.TelemetryFlag='True'));
                '''
    df_shipto = pd.read_sql(sql, cnxn)
    dol_shiptos = tuple(df_shipto.loc[df_shipto.DemandType != 'D038', 'LocNum'].tolist())
    return dol_shiptos


def refresh_beforeReading(conn):
    '''获取司机输入的 before 液位；'''
    # odbc driver reading
    server = 'LRPSQP05\\LRPSQP05'
    database = 'EU_LBLogist_RPT'
    cnxn = connect_odbc(server, database)
    now = datetime.now()
    time_ago = (now - timedelta(days=182)).strftime("%Y-%m-%d")
    shiptos = get_LB_TeleShiptos(cnxn)
    # print('check： --- ', 10925719 in shiptos)

    sql_BeforeReading = '''SELECT LocNum,
                            ReadingDate, ReadingLevel
                            FROM Readings
                            WHERE
                            ReadingType = 2 AND
                            ReadingDate > '{}' AND
                            LocNum IN {};                  
                        '''.format(time_ago, shiptos)
    # starttime = time.time()
    df_BeforeReading = pd.read_sql(
        sql_BeforeReading, cnxn)
    df_BeforeReading = df_BeforeReading.sort_values(by=['LocNum', 'ReadingDate'])
    # 获取 master data
    sql = '''select LocNum, GalsPerInch from odbc_master'''
    df_uom = pd.read_sql(sql, conn)
    #  合并
    df = pd.merge(df_BeforeReading, df_uom, on='LocNum', how='left')
    df['beforeKG'] = df.ReadingLevel * df.GalsPerInch
    df = df[df.beforeKG.notna()].reset_index(drop=True)
    df.beforeKG = df.beforeKG.round()
    df['refresh_date'] = now
    use_cols = ['LocNum', 'ReadingDate', 'beforeKG', 'refresh_date']
    table_name = 'beforeReading'
    df[use_cols].to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC Before Reading is ready.')

def load_t6_info():
    t6_info_df = pd.read_excel(
        r'\\shangnt\lbshell\PUAPI\PU_program\automation\telemetry\BeforeToRoTime\program\t6_info.xlsx')

    # 使用正则表达式过滤 beforeToRoHours 列，保留纯数字的值
    t6_info_df = t6_info_df[t6_info_df['beforeToRoHours'].astype(str).str.match(r'^\d+$')]

    # 将 beforeToRoHours 转换为数值类型
    t6_info_df['beforeToRoHours'] = t6_info_df['beforeToRoHours'].astype(float)

    # 将 ro_time 转换为日期时间格式
    t6_info_df['ro_time'] = pd.to_datetime(t6_info_df['ro_time'])

    # 按 LocNum 和 ro_time 排序
    t6_info_df = t6_info_df.sort_values(by=['LocNum', 'ro_time'], ascending=[True, False])

    return t6_info_df

def refresh_t4_t6_data(cur, conn):

    t6_info_df = load_t6_info()

    # 计算每个 LocNum 最近三次 ro_time 的 beforeToRoHours 的均值
    t6_info_df['beforeToRoHours_rolling_mean'] = t6_info_df.groupby('LocNum')['beforeToRoHours'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean())
    t6_info_df = t6_info_df.dropna(subset=['beforeToRoHours_rolling_mean'])
    # 只保留每个 LocNum 的最近一次记录
    t6_info_df = t6_info_df.drop_duplicates(subset='LocNum', keep='first')

    # 保留需要的列
    t6_info_df = t6_info_df[['LocNum', 'beforeToRoHours_rolling_mean']]
    now = datetime.now()
    t6_info_df['refresh_date'] = now

    table_name = 't4_t6_data'
    cur.execute('''DROP TABLE IF EXISTS {};'''.format(table_name))
    conn.commit()
    # 导入数据
    t6_info_df.to_sql(table_name, con=conn, if_exists='replace', index=False)
    print('ODBC odbc_t4_t6 data is ready.')

