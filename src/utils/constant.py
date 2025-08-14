from .field import FOTableHeader

unitOfLength_dict = {1: 'CM', 2: 'Inch', 3: 'M', 4: 'MM', 5: 'Percent', 6: 'Liters'}
ORDER_ATTR_MAP = {
    FOTableHeader.order_from: 'from_time',
    FOTableHeader.order_to: 'to_time',
    FOTableHeader.ton: 'drop_kg',
    FOTableHeader.comment: 'comments',
    "行程草稿？": 'is_in_trip_draft',
    'PO号': 'po_number',
    FOTableHeader.order_id: "order_id",
    FOTableHeader.order_type: "order_type",
    FOTableHeader.corporate_id: "corporate_idn",
    FOTableHeader.product: "product",
    FOTableHeader.shipto: "shipto",
    FOTableHeader.cust_name: "cust_name",
    FOTableHeader.target_date: "target_date",
    FOTableHeader.risk_date: "risk_date",
    FOTableHeader.run_out_date: "run_out_date"
}