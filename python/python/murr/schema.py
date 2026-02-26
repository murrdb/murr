from enum import Enum

from pydantic import BaseModel


class DType(str, Enum):
    FLOAT32 = "float32"
    UTF8 = "utf8"


class ColumnSchema(BaseModel):
    dtype: DType
    nullable: bool = True


class TableSchema(BaseModel):
    key: str
    columns: dict[str, ColumnSchema]
