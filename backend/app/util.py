import dataclasses
import enum
from datetime import datetime
from typing import Type, get_args, get_origin

from flask_restx import Namespace, fields


def identity(x):
    return lambda _: x


def convert_enum(e: enum.Enum):
    return [value.name for value in e]


TYPE_MAP = {
    str: identity(fields.String),
    int: identity(fields.Integer),
    float: identity(fields.Float),
    bool: identity(fields.Boolean),
    datetime: identity(fields.DateTime),
    list: lambda x: fields.List(x),
    enum.EnumType: lambda x: fields.String(enum=convert_enum(x))
}


def convert_type(_type: Type):
    generic_type = get_origin(_type)
    inner_type = None
    if isinstance(_type, enum.EnumType):
        generic_type = enum.EnumType
        inner_type = _type
    elif generic_type not in TYPE_MAP:
        generic_type = _type
    else:
        inner_type = convert_type(get_args(_type)[0])
    return TYPE_MAP[generic_type](inner_type)


def convert_class_to_model(namespace: Namespace, clazz: Type):
    return namespace.model(clazz.__name__,
                           {field.name: convert_type(field.type) for field in dataclasses.fields(clazz)})


class Field(dataclasses.Field):
    def __init__(self, *args, mapper: callable = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.mapper = mapper


def field(*args, mapper: callable = None, **kwargs) -> dataclasses.Field:
    field_: dataclasses.Field = dataclasses.field(*args, **kwargs)
    return Field(field_.default, field_.default_factory, field_.init, field_.repr, field_.hash, field_.compare,
                 field_.metadata, field_.kw_only, mapper=mapper)
