import pandas as pd
from pydantic.dataclasses import dataclass
from pydantic import ConfigDict


@dataclass(config=ConfigDict(arbitrary_types_allowed=True))
class Run:
    name: str
    data: pd.DataFrame
    metadata: pd.DataFrame
