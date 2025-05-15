import pandas as pd


class Order:
    def __init__(
            self,
            shipto: str,
            cust_name: str,
            product: str,
            from_time: pd.Timestamp,
            to_time: pd.Timestamp,
            drop_kg: float,
            comments: str,
            order_type: str
    ):
        self.shipto = shipto
        self.cust_name = cust_name
        self.product = product
        self.drop_kg = drop_kg
        self.comments = comments
        self.order_type = order_type

    def __str__(self):
        return f"{self.order_type}({self.cust_name}, {self.drop_kg} kg)"

    def is_in_trip(self):
        return False