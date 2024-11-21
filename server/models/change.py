from pydantic import BaseModel
from typing import Literal, Optional, Dict, Union


class Change(BaseModel):
    action: Literal["create", "delete", "update", "bulk_create", "bulk_delete", "bulk_update"]
    type: Literal["schema", "state"]
    timestamp: int
    payload: Union[Dict, list[Dict]] 
    version: Optional[str] = None
