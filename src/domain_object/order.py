import pandas as pd


class Order:
    def __init__(
            self,
            order_id: str,
            shipto: str,
            cust_name: str,
            corporate_idn: str,
            product: str,
            from_time: pd.Timestamp,
            to_time: pd.Timestamp,
            drop_kg: float,
            comments: str,
            order_type: str,
            target_date: pd.Timestamp = None,
            risk_date: pd.Timestamp = None,
            run_out_date: pd.Timestamp = None,
            po_number: str = '',
            so_number: str = '',
            is_in_trip_draft: int = 0,
    ):
        self.order_id = order_id
        self.shipto = shipto
        self.cust_name = cust_name
        self.corporate_idn = corporate_idn
        self.product = product
        self.from_time = pd.to_datetime(from_time) if isinstance(from_time, str) else from_time
        self.to_time = pd.to_datetime(to_time) if isinstance(to_time, str) else to_time
        self.drop_kg = float(drop_kg)
        self.comments = comments
        self.po_number = po_number
        self.order_type = order_type
        self.target_date = target_date
        self.risk_date = risk_date
        self.run_out_date = run_out_date
        self.so_number = so_number
        self.is_in_trip_draft = is_in_trip_draft

    def __str__(self):
        return f"{self.order_type}({self.order_id}, {self.cust_name}, {self.drop_kg} kg)"


    def complete_so_number(self, so_number: str):
        if self.is_so_number_valid(so_number):
            self.so_number = so_number
        else:
            self.so_number = ''

    @property
    def has_valid_so_number(self):
        return self.is_so_number_valid(self.so_number)

    @staticmethod
    def is_so_number_valid(so_number: str):
        return (
                isinstance(so_number, str) and
                (so_number.startswith('SO') or so_number == 'Onstop')
        )
