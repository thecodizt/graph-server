from pydantic import BaseModel, validator, ValidationError
from typing import Literal, Optional, Dict, Union
import logging

logger = logging.getLogger(__name__)

class Change(BaseModel):
    action: Literal[
        "create",
        "delete",
        "update",
        "bulk_create",
        "bulk_delete",
        "bulk_update",
        "direct_create",
    ]
    type: Literal["schema", "state"]
    timestamp: int
    payload: Union[Dict, list[Dict]]
    version: Optional[str] = None

    @validator("action")
    def validate_action(cls, v, values):
        try:
            return v
        except Exception as e:
            logger.error(f"Invalid action value: {v}. Error: {str(e)}")
            raise

    @validator("type")
    def validate_type(cls, v, values):
        try:
            return v
        except Exception as e:
            logger.error(f"Invalid type value: {v}. Error: {str(e)}")
            raise

    @validator("timestamp")
    def validate_timestamp(cls, v):
        if v < 0:
            msg = f"Invalid timestamp: {v} (must be non-negative)"
            logger.error(msg)
            raise ValueError(msg)
        return v

    @validator("payload")
    def validate_payload(cls, v, values):
        try:
            action = values.get("action", "")
            
            # Validate payload type based on action
            if action.startswith("bulk_"):
                if not isinstance(v, list):
                    msg = f"Bulk action '{action}' requires list payload, got {type(v).__name__}"
                    logger.error(msg)
                    raise ValueError(msg)
                if not v:
                    msg = f"Bulk action '{action}' requires non-empty payload list"
                    logger.error(msg)
                    raise ValueError(msg)
            else:
                if not isinstance(v, dict):
                    msg = f"Action '{action}' requires dict payload, got {type(v).__name__}"
                    logger.error(msg)
                    raise ValueError(msg)
                if not v:
                    msg = f"Action '{action}' requires non-empty payload dict"
                    logger.error(msg)
                    raise ValueError(msg)
            
            return v
        except Exception as e:
            if not isinstance(e, ValueError):  # Don't log twice for our own ValueError
                logger.error(f"Payload validation error for action '{values.get('action', '')}': {str(e)}")
            raise

    @validator("version", always=True)
    def validate_version(cls, v, values):
        if v is None:
            logger.warning("No version provided in change request")
        return v
