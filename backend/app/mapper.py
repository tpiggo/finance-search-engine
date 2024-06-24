from datetime import datetime, timedelta


def date_from_int96_timestamp(int96_timestamp):
    if isinstance(int96_timestamp, str):
        int96_timestamp = int(int96_timestamp)
    julian_calendar_days = int96_timestamp >> 8 * 8
    time = int((int96_timestamp & 0xFFFFFFFFFFFFFFFF) / 1_000)
    linux_epoch = 2_440_588
    return datetime(1970, 1, 1) + timedelta(days=julian_calendar_days - linux_epoch,
                                            microseconds=time)

