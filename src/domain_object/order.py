import pandas as pd


class Order:
    def __init__(
            self,
            order_id: str,
            shipto: str,
            cust_name: str,
            product: str,
            from_time: pd.Timestamp,
            to_time: pd.Timestamp,
            drop_kg: float,
            comments: str,
            order_type: str
    ):
        self.order_id = order_id
        self.shipto = shipto
        self.cust_name = cust_name
        self.product = product
        self.from_time = pd.to_datetime(from_time) if isinstance(from_time, str) else from_time
        self.to_time = pd.to_datetime(to_time) if isinstance(to_time, str) else to_time
        self.drop_kg = drop_kg
        self.comments = comments
        self.order_type = order_type
        self.so_number = ''

    def __str__(self):
        return f"{self.order_type}({self.order_id}, {self.cust_name}, {self.drop_kg} kg)"

    def is_in_trip(self):
        return False

    def complete_so_number(self, so_number: str):
        if isinstance(so_number, str) and so_number.startswith('SO'):
            self.so_number = so_number
        else:
            self.so_number = ''