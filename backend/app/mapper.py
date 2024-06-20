from datetime import datetime, timedelta


def date_from_int96_timestamp(int96_timestamp):
    if isinstance(int96_timestamp, str):
        int96_timestamp = int(int96_timestamp)
    julian_calendar_days = int96_timestamp >> 8 * 8
    time = int((int96_timestamp & 0xFFFFFFFFFFFFFFFF) / 1_000)
    linux_epoch = 2_440_588
    return datetime(1970, 1, 1) + timedelta(days=julian_calendar_days - linux_epoch,
                                            microseconds=time)


class Mapper:
    _model = {
        "open_time": (datetime, date_from_int96_timestamp),
        "open": (float, float),
        "close": (float, float),
        "high": (float, float),
        "low": (float, float)
    }

    _ordered_list = [i for i in _model.keys()]

    def produce_query(self):
        return ",".join([f'"{item.upper()}"' for item in self._ordered_list])

    def get_order(self):
        return [item for item in self._ordered_list]

    def convert_string_to_object(self, string: str, _index: int):
        def get_value(index: int, attribute):
            _, conversion = self._model[self._ordered_list[index]]
            return conversion(attribute)
        return [get_value(index, attr) if attr != '' else None for index, attr in enumerate(string.split(","))]
