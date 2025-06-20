import pandas as pd
from typing import Dict
from .segment import Segment

class Trip:
    def __init__(
            self,
            trip_id: str,
            trip_start_time: pd.Timestamp,

    ):
        self.trip_id = trip_id
        self.trip_start_time = trip_start_time
        self.segment_dict: Dict[int, Segment] = dict()

    def __repr__(self):
        return f"Trip(trip_id={self.trip_id}, trip_start_time={self.trip_start_time})"

    @property
    def display_trip_route(self):
        display_lt = [
            segment.location
            for segment_id, segment in self.segment_dict.items()
        ]
        return '->'.join(display_lt)

    def find_segment_by_shipto(self, shipto: str):
        for i, segment in self.segment_dict.items():
            if segment.to_loc_num == shipto:
                return segment
        return None