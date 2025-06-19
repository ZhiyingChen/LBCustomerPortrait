
class Segment:
    def __init__(
            self,
            segment_num: int,
            segment_type: str,
            segment_status: str,
            to_loc_num: str,
            location: str
    ):
        self.segment_num = segment_num
        self.segment_type = segment_type
        self.segment_status = segment_status
        self.to_loc_num = to_loc_num
        self.location = location

    def __str__(self):
        return f"Segment {self.segment_num} ({self.segment_type}, {self.segment_status}, {self.location})"