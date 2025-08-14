unitOfLength_dict = {1: 'CM', 2: 'Inch', 3: 'M', 4: 'MM', 5: 'Percent', 6: 'Liters'}
ORDER_ATTR_MAP = {
    '订单从': 'from_time',
    '订单到': 'to_time',
    '吨': 'drop_kg',
    "备注": 'comments',
    "行程草稿？": 'is_in_trip_draft',
    'PO号': 'po_number',
    "订单": "order_id",
    "类型": "order_type",
    "DT": "corporate_idn",
    "产品": "product",
    "ShipTo": "shipto",
    "客户简称": "cust_name",
    "目标充装": "target_date",
    "最佳充装": "risk_date",
    "断气": "run_out_date"
}