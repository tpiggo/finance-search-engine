from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Union, Any
import numpy as np
import pandas as pd

from app.mapper import date_from_int96_timestamp
from app.util import field, Field

_KNOWN_EXTENSIONS = {
    'parquet': "Parquet",
    'csv': "CSV",
    'json': "JSON",
}


def apply_on_field(fields_: Union[list[Field], dict[str, Field]], index: Union[str, int], attribute: Any):
    field = fields_[index]
    if isinstance(attribute, field.type):
        return attribute
    if isinstance(field, Field):
        return field.mapper(attribute)
    return field.type(attribute)


@dataclass
class S3Key:
    key: str
    name: str
    extension: str
    load_time: datetime

    def __eq__(self, other):
        return self.key == other.key and self.load_time == other.load_time

    def __hash__(self):
        return hash(self.key) + hash(self.load_time)

    @property
    def extension_name(self):
        return _KNOWN_EXTENSIONS[self.extension]

    @classmethod
    def from_info(cls, key_string: str, prefix: str, load_time: datetime):
        name, holds_extension = tuple(i for i in key_string.replace(prefix, "").split("/") if i != '')
        ext = holds_extension.split('.')[1]
        return cls(key_string, name, ext, load_time)


def update_dataframe_for_date_limits(df: pd.DataFrame):
    df['open_time_ts'] = df['open_time'].values.astype(np.int64) // 10 ** 9
    return df.loc[df['open_time_ts'].mod(86400).eq(0)]


@dataclass
class Tick:
    open: float
    close: float
    high: float
    low: float
    volume: float
    open_time: datetime = field(mapper=date_from_int96_timestamp)

    @classmethod
    def produce_query(cls):
        return ",".join([f'"{item.name.upper()}"' for item in fields(cls)])

    @classmethod
    def get_order(cls):
        return [item.name for item in fields(cls)]

    @classmethod
    def convert_string_to_object(cls, string: str, _index: int):
        fields_ = [field_ for field_ in fields(cls)]
        return [apply_on_field(fields_, index, attr) if attr != '' else None
                for index, attr in enumerate(string.split(","))]


def gte(value: float):
    def comparison(compared_to: float):
        return compared_to >= value

    return comparison


def lte(value: float):
    def comparison(compared_to: float):
        return compared_to <= value

    return comparison


class Direction(Enum):
    UP = ("UP", gte)
    DOWN = ("DOWN", lte)

    def get_fn(self, value: float):
        return self.value[1](value)


@dataclass
class Query:
    size: float
    num_days: int
    direction: Direction = field(mapper=lambda x: Direction[x])
    items: list[str] = field(default_factory=list)

    @classmethod
    def build(cls, input_dict: dict):
        fields_ = {field_.name: field_ for field_ in fields(cls)}
        return cls(**{field_: apply_on_field(fields_, field_, value) for field_, value in input_dict.items()})


if __name__ == '__main__':
    for i in Direction:
        print(f'{i.name}')
