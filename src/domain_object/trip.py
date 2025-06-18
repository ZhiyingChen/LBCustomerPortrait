import pandas as pd


class Trip:
    def __init__(
            self,
            trip_id: str,
            trip_start_time: pd.Timestamp,
    ):
        self.trip_id = trip_id
        self.trip_start_time = trip_start_time

    def __repr__(self):
        return f"Trip(trip_id={self.trip_id}, trip_start_time={self.trip_start_time})"