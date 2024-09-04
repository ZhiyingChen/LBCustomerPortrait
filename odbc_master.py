import pandas as pd
import pyodbc
from datetime import datetime
from datetime import timedelta
# 连接ODBC


def connect_odbc(server, database):
    # cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server +
    #                       ';DATABASE='+database+';UID='+username+';PWD=' + password + ';Trusted_Connection=yes')
    # 这句话是2021-11-29 修改，请注意：这里的 DRIVER 的参数变化了，这是一个很重要的变化！
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
         CustomerTelemetry.Subscriber
        FROM
         EU_LBLogist_Rpt.dbo.LBCustProfile LBCustProfile,
         EU_LBLogist_Rpt.dbo.CustomerProfile
         LEFT JOIN CustomerTelemetry
         ON CustomerProfile.LocNum=CustomerTelemetry.LocNum,
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
