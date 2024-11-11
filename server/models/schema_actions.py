from enum import Enum
from pydantic import BaseModel
from typing import Dict, Optional, Union, List


class SchemaActionType(str, Enum):
    ADD_UNITS = "add_units"
    REMOVE_UNITS = "remove_units"
    UPDATE_PROPERTIES = "update_properties"


class SchemaAction(BaseModel):
    action_type: SchemaActionType
    node_id: str
    node_type: str
    units: Optional[int] = None
    properties: Optional[Dict] = None
