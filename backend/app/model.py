from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd

_KNOWN_EXTENSIONS = {
    'parquet': "Parquet",
    'csv': "CSV",
    'json': "JSON",
}


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
