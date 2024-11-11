from pydantic import BaseModel
from typing import Literal, Optional, Dict


class Change(BaseModel):
    action: Literal["create", "delete", "update"]
    type: Literal["schema", "state"]
    timestamp: int
    payload: Dict
    version: Optional[str] = None
