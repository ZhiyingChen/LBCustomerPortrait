
class Segment:
    def __init__(
            self,
            segment_num: int,
            segment_type: str,
            segment_status: str,
            to_loc_num: str,
            location: str,
            arrival_time: str,
            drop_kg: int
    ):
        self.segment_num = segment_num
        self.segment_type = segment_type
        self.segment_status = segment_status
        self.to_loc_num = to_loc_num
        self.location = location
        self.arrival_time = arrival_time
        self.drop_kg = drop_kg

    def __str__(self):
        return f"Segment {self.segment_num} ({self.segment_type}, {self.segment_status}, {self.location})"