import dataclasses
from datetime import datetime
from typing import Type

from flask_restx import Namespace, fields

TYPE_MAP = {
    str: fields.String,
    int: fields.Integer,
    float: fields.Float,
    bool: fields.Boolean,
    datetime: fields.DateTime,
}


def convert_class_to_model(namespace: Namespace, clazz: Type):
    def convert_data_class_to_dict_repr():
        dict_repr = {}
        for field in dataclasses.fields(clazz):
            dict_repr[field.name] = TYPE_MAP[field.type]
        return dict_repr

    return namespace.model(clazz.__name__, convert_data_class_to_dict_repr())


class Field(dataclasses.Field):

    def __init__(self, *args, mapper: callable = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.mapper = mapper


def field(*args, mapper: callable = None, **kwargs) -> dataclasses.Field:
    field_: dataclasses.Field = dataclasses.field(*args, **kwargs)
    return Field(field_.default, field_.default_factory, field_.init, field_.repr, field_.hash, field_.compare,
                 field_.metadata, field_.kw_only, mapper=mapper)
