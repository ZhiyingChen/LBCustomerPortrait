SHAREPOINT_TEMP_DIRECTORY = r'\\shangnt\Lbshell\PUAPI\PU_program\automation\autoScheduling\DTDTemp'
DTD_FILE_NAME = 'dtd_sharepoint_df.feather'
EQUIPMENT_FILE_NAME = 'equipment_sharepoint_df.feather'

Call_Log_Table = "CallLog"
FO_LIST_TABLE = "FOList"
FO_RECORD_LIST_TABLE = "FORecordList"
OO_LIST_TABLE = "OOList"

class OrderListHeader:
    order_id = "OrderID"
    shipto = "ShipTo"
    cust_name = "CustName"
    corporate_idn = "CorporateIdn"
    product = "Product"
    from_time = "FromTime"
    to_time = "ToTime"
    drop_kg = "DropKG"
    comment = "Comment"
    target_date = "TargetDate"
    risk_date = "RiskDate"
    run_out_date = "RunOutDate"
    po_number = "PONumber"
    edit_type = "EditType"
    timestamp = "Timestamp"
    so_number = "SONumber"
    apex_id = "ApexID"
    in_trip_draft = 'InTripDraft'

class CallLogHeader:
    shipto = "ShipTo"
    cust_name = "CustName"
    apex_id = "ApexID"
    timestamp = "Timestamp"

class FOTableHeader:
    order_id = "订单"
    order_type = "类型"
    corporate_id = "DT"
    product = "产品"
    shipto = "ShipTo"
    cust_name = "客户简称"
    order_from = "订单从"
    order_to = "订单到"
    ton = "吨"
    comment = "备注"
    target_date = "目标充装"
    risk_date = "最佳充装"
    run_out_date = "断气"