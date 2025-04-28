from typing import Dict


class PrimaryDTInfo:
    def __init__(
            self,
            primary_terminal: str,
            distance_km: float = None,
            duration_hours: float = None
    ):
        self.primary_terminal = primary_terminal
        self.distance_km = distance_km
        self.duration_hours = duration_hours

    def __str__(self):
        return self.primary_terminal

class SourcingDTInfo:
    def __init__(
            self,
            sourcing_terminal: str,
            rank: int,
            frequency: int,
            distance_km: float = None,
            duration_hours: float = None
    ):
        self.sourcing_terminal = sourcing_terminal
        self.rank = rank
        self.frequency = frequency
        self.distance_km = distance_km
        self.duration_hours = duration_hours

    def __str__(self):
        return '{}-Rank({})-Frequency({})'.format(
            self.sourcing_terminal,
            self.rank,
            self.frequency
        )

class NearbyShipToInfo:
    def __init__(
            self,
            nearby_shipto: str,
            shipto_name: str,
            distance_km: float,
            dder: float,
            rank: int,
    ):
        self.nearby_shipto = nearby_shipto
        self.shipto_name = shipto_name
        self.distance_km = distance_km
        self.dder = dder
        self.rank = rank

    def __str__(self):
        return self.nearby_shipto

class DTDShipto:
    def __init__(
            self,
            shipto: str,
            shipto_name: str,
            tra: float,
            max_payload: float,
    ):
        self.shipto = shipto
        self.shipto_name = shipto_name
        self.tra = tra
        self.max_payload = max_payload
        self.primary_terminal_info: PrimaryDTInfo = None
        self.sourcing_terminal_info_dict: Dict[str, SourcingDTInfo] = dict()
        self.nearby_shipto_info_dict: Dict[str, NearbyShipToInfo] = dict()

    def __str__(self):
        return f"DTDShipto(shipto={self.shipto}, name={self.shipto_name}, is_full_load={self.is_full_load})"

    @property
    def is_full_load(self):
        if self.tra < self.max_payload:
            return False
        else:
            return True