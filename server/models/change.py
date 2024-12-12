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
        logger.info(f"Validating action: {v}")
        return v

    @validator("type")
    def validate_type(cls, v, values):
        logger.info(f"Validating type: {v}")
        return v

    @validator("timestamp")
    def validate_timestamp(cls, v):
        logger.info(f"Validating timestamp: {v}")
        if v <= 0:
            msg = f"Invalid timestamp: {v} (must be positive)"
            logger.error(msg)
            raise ValueError(msg)
        return v

    @validator("payload")
    def validate_payload(cls, v, values):
        action = values.get("action", "")
        logger.info(f"Validating payload for action '{action}': {str(v)[:200]}...")  # Log first 200 chars of payload
        
        # Validate payload type based on action
        if action.startswith("bulk_"):
            if not isinstance(v, list):
                msg = f"Bulk action '{action}' requires list payload, got {type(v)}"
                logger.error(msg)
                raise ValueError(msg)
            if not v:
                msg = f"Bulk action '{action}' requires non-empty payload list"
                logger.error(msg)
                raise ValueError(msg)
            logger.info(f"Bulk payload contains {len(v)} items")
        else:
            if not isinstance(v, dict):
                msg = f"Action '{action}' requires dict payload, got {type(v)}"
                logger.error(msg)
                raise ValueError(msg)
            if not v:
                msg = f"Action '{action}' requires non-empty payload dict"
                logger.error(msg)
                raise ValueError(msg)
        
        return v

    @validator("version", always=True)
    def validate_version(cls, v, values):
        logger.info(f"Validating version: {v}")
        if v is None:
            logger.warning("No version provided in change request")
        return v
