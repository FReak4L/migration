# migration/core/schema_compatibility.py

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union, Tuple
from enum import Enum
from dataclasses import dataclass

from pydantic import BaseModel, Field, field_validator
from core.logging_setup import get_logger

logger = get_logger()


class FieldType(Enum):
    """Supported field types for schema compatibility."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME_ISO = "datetime_iso"
    DATETIME_UNIX = "datetime_unix"
    UUID = "uuid"
    JSON = "json"


class ConversionStrategy(Enum):
    """Strategies for handling field conversion."""
    STRICT = "strict"  # Fail on conversion errors
    LENIENT = "lenient"  # Use fallback values on errors
    SKIP = "skip"  # Skip fields that can't be converted


@dataclass
class FieldMapping:
    """Defines how to map a field from source to target schema."""
    source_field: str
    target_field: str
    source_type: FieldType
    target_type: FieldType
    required: bool = True
    default_value: Any = None
    conversion_strategy: ConversionStrategy = ConversionStrategy.LENIENT


class SchemaCompatibilityEngine:
    """Enhanced schema compatibility engine for migration between platforms."""
    
    def __init__(self, conversion_strategy: ConversionStrategy = ConversionStrategy.LENIENT):
        self.conversion_strategy = conversion_strategy
        self.conversion_errors: List[Dict[str, Any]] = []
        self.conversion_warnings: List[Dict[str, Any]] = []
        
    def _convert_datetime_iso_to_unix(self, value: Optional[str]) -> Optional[int]:
        """Convert ISO 8601 datetime string to Unix timestamp."""
        if not value:
            return None
            
        try:
            # Handle 'Z' suffix and various ISO formats
            dt_str = value.replace('Z', '+00:00')
            
            # Handle microseconds truncation if needed
            if '.' in dt_str:
                dt_part, _, micro_part_full = dt_str.rpartition('.')
                micro_part_no_tz = micro_part_full.split('+')[0].split('-')[0]
                if len(micro_part_no_tz) > 6:
                    micro_part_trimmed = micro_part_no_tz[:6]
                    tz_suffix = ""
                    if '+' in micro_part_full:
                        tz_suffix = '+' + micro_part_full.split('+', 1)[1]
                    elif '-' in micro_part_full and not micro_part_full.startswith(micro_part_no_tz):
                        potential_tz_part = micro_part_full[len(micro_part_no_tz):]
                        if potential_tz_part.startswith("-"):
                            tz_suffix = potential_tz_part
                    dt_str = f"{dt_part}.{micro_part_trimmed}{tz_suffix}"
            
            dt = datetime.fromisoformat(dt_str)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
                
            return int(dt.timestamp())
            
        except (ValueError, TypeError) as e:
            error_info = {
                "field_value": value,
                "error": str(e),
                "conversion": "iso_to_unix"
            }
            self.conversion_errors.append(error_info)
            logger.warning(f"Failed to convert ISO datetime '{value}' to Unix timestamp: {e}")
            return None
    
    def _convert_unix_to_datetime_iso(self, value: Optional[Union[int, float]]) -> Optional[str]:
        """Convert Unix timestamp to ISO 8601 datetime string."""
        if value is None:
            return None
            
        try:
            if isinstance(value, (int, float)) and value >= 0:
                dt = datetime.fromtimestamp(value, tz=timezone.utc)
                return dt.isoformat().replace('+00:00', 'Z')
        except (OSError, OverflowError, ValueError) as e:
            error_info = {
                "field_value": value,
                "error": str(e),
                "conversion": "unix_to_iso"
            }
            self.conversion_errors.append(error_info)
            logger.warning(f"Failed to convert Unix timestamp '{value}' to ISO datetime: {e}")
            
        return None
    
    def _convert_uuid_format(self, value: Optional[str], add_dashes: bool = True) -> Optional[str]:
        """Convert UUID between formats (with/without dashes)."""
        if not value or not isinstance(value, str):
            return None
            
        try:
            # Remove existing dashes
            clean_uuid = value.replace('-', '')
            
            # Validate it's a 32-character hex string
            if len(clean_uuid) != 32:
                raise ValueError(f"UUID must be 32 characters, got {len(clean_uuid)}")
                
            int(clean_uuid, 16)  # Validate hex
            
            if add_dashes:
                # Format to XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
                return f"{clean_uuid[:8]}-{clean_uuid[8:12]}-{clean_uuid[12:16]}-{clean_uuid[16:20]}-{clean_uuid[20:]}"
            else:
                return clean_uuid
                
        except (ValueError, TypeError) as e:
            error_info = {
                "field_value": value,
                "error": str(e),
                "conversion": "uuid_format"
            }
            self.conversion_errors.append(error_info)
            logger.warning(f"Failed to convert UUID format '{value}': {e}")
            return None
    
    def _apply_field_conversion(self, value: Any, mapping: FieldMapping) -> Any:
        """Apply conversion based on field mapping."""
        if value is None and not mapping.required:
            return mapping.default_value
            
        # Type-specific conversions
        if mapping.source_type == FieldType.DATETIME_ISO and mapping.target_type == FieldType.DATETIME_UNIX:
            return self._convert_datetime_iso_to_unix(value)
        elif mapping.source_type == FieldType.DATETIME_UNIX and mapping.target_type == FieldType.DATETIME_ISO:
            return self._convert_unix_to_datetime_iso(value)
        elif mapping.source_type == FieldType.UUID and mapping.target_type == FieldType.UUID:
            return self._convert_uuid_format(value, add_dashes=True)
        elif mapping.target_type == FieldType.STRING:
            return str(value) if value is not None else mapping.default_value
        elif mapping.target_type == FieldType.INTEGER:
            try:
                return int(value) if value is not None else mapping.default_value
            except (ValueError, TypeError):
                if mapping.conversion_strategy == ConversionStrategy.STRICT:
                    raise
                return mapping.default_value
        elif mapping.target_type == FieldType.FLOAT:
            try:
                return float(value) if value is not None else mapping.default_value
            except (ValueError, TypeError):
                if mapping.conversion_strategy == ConversionStrategy.STRICT:
                    raise
                return mapping.default_value
        elif mapping.target_type == FieldType.BOOLEAN:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            elif isinstance(value, (int, float)):
                return bool(value)
            else:
                return mapping.default_value
                
        return value
    
    def transform_record(self, source_record: Dict[str, Any], field_mappings: List[FieldMapping]) -> Dict[str, Any]:
        """Transform a single record using field mappings."""
        target_record = {}
        
        for mapping in field_mappings:
            source_value = source_record.get(mapping.source_field)
            
            try:
                converted_value = self._apply_field_conversion(source_value, mapping)
                target_record[mapping.target_field] = converted_value
                
            except Exception as e:
                error_info = {
                    "source_field": mapping.source_field,
                    "target_field": mapping.target_field,
                    "source_value": source_value,
                    "error": str(e)
                }
                self.conversion_errors.append(error_info)
                
                if mapping.conversion_strategy == ConversionStrategy.STRICT:
                    raise
                elif mapping.conversion_strategy == ConversionStrategy.LENIENT:
                    target_record[mapping.target_field] = mapping.default_value
                # SKIP strategy: don't add the field
                
        return target_record
    
    def get_marzneshin_to_marzban_user_mappings(self) -> List[FieldMapping]:
        """Get field mappings for Marzneshin to Marzban user transformation."""
        return [
            FieldMapping("username", "username", FieldType.STRING, FieldType.STRING),
            FieldMapping("data_limit", "data_limit", FieldType.INTEGER, FieldType.INTEGER, default_value=0),
            FieldMapping("used_traffic", "used_traffic", FieldType.INTEGER, FieldType.INTEGER, default_value=0),
            FieldMapping("expire_date", "expire", FieldType.DATETIME_ISO, FieldType.DATETIME_UNIX),
            FieldMapping("created_at", "created_at", FieldType.DATETIME_ISO, FieldType.DATETIME_ISO),
            FieldMapping("updated_at", "edit_at", FieldType.DATETIME_ISO, FieldType.DATETIME_ISO),
            FieldMapping("note", "note", FieldType.STRING, FieldType.STRING, required=False),
            FieldMapping("sub_revoked_at", "sub_revoked_at", FieldType.DATETIME_ISO, FieldType.DATETIME_ISO, required=False),
            FieldMapping("sub_updated_at", "sub_updated_at", FieldType.DATETIME_ISO, FieldType.DATETIME_ISO, required=False),
            FieldMapping("sub_last_user_agent", "sub_last_user_agent", FieldType.STRING, FieldType.STRING, required=False),
            FieldMapping("online_at", "online_at", FieldType.DATETIME_ISO, FieldType.DATETIME_ISO, required=False),
            FieldMapping("data_limit_reset_strategy", "data_limit_reset_strategy", FieldType.STRING, FieldType.STRING, default_value="no_reset"),
            FieldMapping("key", "uuid_key", FieldType.UUID, FieldType.UUID, required=False),
        ]
    
    def get_conversion_report(self) -> Dict[str, Any]:
        """Get a report of conversion errors and warnings."""
        return {
            "total_errors": len(self.conversion_errors),
            "total_warnings": len(self.conversion_warnings),
            "errors": self.conversion_errors,
            "warnings": self.conversion_warnings
        }
    
    def clear_conversion_log(self):
        """Clear conversion errors and warnings."""
        self.conversion_errors.clear()
        self.conversion_warnings.clear()


class SchemaValidator(BaseModel):
    """Validates schema compatibility between source and target."""
    
    source_schema: Dict[str, str] = Field(description="Source schema field types")
    target_schema: Dict[str, str] = Field(description="Target schema field types")
    
    def validate_compatibility(self, field_mappings: List[FieldMapping]) -> Tuple[bool, List[str]]:
        """Validate if field mappings are compatible with schemas."""
        issues = []
        
        for mapping in field_mappings:
            # Check if source field exists in source schema
            if mapping.source_field not in self.source_schema and mapping.required:
                issues.append(f"Required source field '{mapping.source_field}' not found in source schema")
            
            # Check if target field is valid for target schema
            if mapping.target_field in self.target_schema:
                expected_type = self.target_schema[mapping.target_field]
                if expected_type != mapping.target_type.value:
                    issues.append(f"Target field '{mapping.target_field}' type mismatch: expected {expected_type}, got {mapping.target_type.value}")
        
        return len(issues) == 0, issues