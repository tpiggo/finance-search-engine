from dataclasses import dataclass

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

    @property
    def extension_name(self):
        return _KNOWN_EXTENSIONS[self.extension]

    @classmethod
    def from_string(cls, key_string: str, prefix: str):
        name, holds_extension = tuple(i for i in key_string.replace(prefix, "").split("/") if i != '')
        ext = holds_extension.split('.')[1]
        return cls(key_string, name, ext)
