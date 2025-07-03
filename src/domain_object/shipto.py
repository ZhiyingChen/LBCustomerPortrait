
class ShipTo:
    def __init__(
            self,
            loc_num: str,
            cust_acronym: str,
            tank_acronym: str,
            primary_terminal: str,
            sub_region: str,
            product_class: str,
            demand_type: str,
            gals_per_inch: float,
            unit_of_length: float,
            subscriber: str,
            is_in_forcast: bool = False,

    ):
        self.loc_num = loc_num
        self.cust_acronym = cust_acronym
        self.tank_acronym = tank_acronym
        self.primary_terminal = primary_terminal
        self.sub_region = sub_region
        self.product_class = product_class
        self.demand_type = demand_type
        self.gals_per_inch = gals_per_inch
        self.unit_of_length = unit_of_length
        self.subscriber = subscriber
        self.is_in_forcast = is_in_forcast

    def __str__(self):
        return f"shipto: {self.loc_num}\n" \
               f"Acronym: {self.acronym}\n"

    @property
    def acronym(self):
        return f"{self.cust_acronym}, {self.tank_acronym}"