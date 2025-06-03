# migration/core/schema_analyzer.py

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass
from enum import Enum

from core.logging_setup import get_logger
from utils.datetime_utils import iso_format_to_datetime, datetime_to_unix_timestamp

logger = get_logger()


class FieldType(Enum):
    """Enumeration of supported field types for schema analysis."""
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATETIME_ISO = "datetime_iso"
    DATETIME_UNIX = "datetime_unix"
    JSON = "json"
    BIGINT = "bigint"
    FLOAT = "float"
    UUID = "uuid"


@dataclass
class FieldIncompatibility:
    """Represents a schema incompatibility between source and target fields."""
    field_name: str
    source_type: FieldType
    target_type: FieldType
    source_sample: Any
    target_expected: Any
    conversion_function: Optional[str] = None
    severity: str = "medium"  # low, medium, high, critical


@dataclass
class SchemaAnalysisResult:
    """Results of schema compatibility analysis."""
    incompatibilities: List[FieldIncompatibility]
    compatible_fields: List[str]
    missing_fields: List[str]
    extra_fields: List[str]
    total_fields_analyzed: int
    compatibility_score: float


class SchemaAnalyzer:
    """Analyzes schema compatibility between Marzneshin and Marzban data structures."""
    
    def __init__(self):
        self.marzban_user_schema = {
            "username": FieldType.STRING,
            "status": FieldType.STRING,
            "used_traffic": FieldType.BIGINT,
            "data_limit": FieldType.BIGINT,
            "expire": FieldType.DATETIME_UNIX,  # Unix timestamp
            "created_at": FieldType.DATETIME_ISO,  # ISO datetime
            "edit_at": FieldType.DATETIME_ISO,
            "note": FieldType.STRING,
            "sub_revoked_at": FieldType.DATETIME_ISO,
            "sub_updated_at": FieldType.DATETIME_ISO,
            "sub_last_user_agent": FieldType.STRING,
            "online_at": FieldType.DATETIME_ISO,
            "data_limit_reset_strategy": FieldType.STRING,
            "admin_id": FieldType.INTEGER,
            "on_hold_timeout": FieldType.DATETIME_ISO,
            "on_hold_expire_duration": FieldType.INTEGER,
            "auto_delete_in_days": FieldType.INTEGER,
            "last_status_change": FieldType.DATETIME_ISO
        }
        
        self.marzban_admin_schema = {
            "username": FieldType.STRING,
            "hashed_password": FieldType.STRING,
            "is_sudo": FieldType.BOOLEAN,
            "created_at": FieldType.DATETIME_ISO,
            "password_reset_at": FieldType.DATETIME_ISO,
            "telegram_id": FieldType.BIGINT,
            "discord_webhook": FieldType.STRING,
            "users_usage": FieldType.BIGINT
        }
        
        self.marzban_proxy_schema = {
            "user_id": FieldType.INTEGER,
            "type": FieldType.STRING,
            "settings": FieldType.JSON
        }

    def _detect_field_type(self, value: Any) -> FieldType:
        """Detect the type of a field value."""
        if value is None:
            return FieldType.STRING  # Default for nullable fields
        
        if isinstance(value, bool):
            return FieldType.BOOLEAN
        elif isinstance(value, int):
            if value > 2147483647:  # Beyond 32-bit int
                return FieldType.BIGINT
            return FieldType.INTEGER
        elif isinstance(value, float):
            return FieldType.FLOAT
        elif isinstance(value, str):
            # Check if it's a datetime string
            if self._is_iso_datetime(value):
                return FieldType.DATETIME_ISO
            elif self._is_uuid_format(value):
                return FieldType.UUID
            return FieldType.STRING
        elif isinstance(value, dict) or isinstance(value, list):
            return FieldType.JSON
        else:
            return FieldType.STRING

    def _is_iso_datetime(self, value: str) -> bool:
        """Check if a string is in ISO datetime format."""
        try:
            iso_format_to_datetime(value)
            return True
        except:
            return False

    def _is_uuid_format(self, value: str) -> bool:
        """Check if a string is in UUID format."""
        import re
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        return bool(re.match(uuid_pattern, value.lower())) if isinstance(value, str) else False

    def _is_unix_timestamp(self, value: Union[int, float]) -> bool:
        """Check if a numeric value is a valid Unix timestamp."""
        if not isinstance(value, (int, float)):
            return False
        # Check if it's in a reasonable range (1970-2100)
        return 0 <= value <= 4102444800

    def analyze_user_schema_compatibility(self, marzneshin_users: List[Dict[str, Any]]) -> SchemaAnalysisResult:
        """Analyze compatibility between Marzneshin user data and Marzban user schema."""
        if not marzneshin_users:
            return SchemaAnalysisResult([], [], [], [], 0, 1.0)

        incompatibilities = []
        compatible_fields = []
        missing_fields = []
        extra_fields = []
        
        # Analyze a sample of users to detect patterns
        sample_user = marzneshin_users[0]
        
        # Check for missing required fields
        for required_field, expected_type in self.marzban_user_schema.items():
            if required_field not in sample_user:
                missing_fields.append(required_field)
                continue
                
            sample_value = sample_user[required_field]
            detected_type = self._detect_field_type(sample_value)
            
            if detected_type != expected_type:
                # Special case: expire field conversion from ISO to Unix timestamp
                if required_field == "expire" and detected_type == FieldType.DATETIME_ISO:
                    incompatibility = FieldIncompatibility(
                        field_name="expire",
                        source_type=FieldType.DATETIME_ISO,
                        target_type=FieldType.DATETIME_UNIX,
                        source_sample=sample_value,
                        target_expected=datetime_to_unix_timestamp(iso_format_to_datetime(sample_value)),
                        conversion_function="datetime_to_unix_timestamp",
                        severity="high"
                    )
                    incompatibilities.append(incompatibility)
                else:
                    incompatibility = FieldIncompatibility(
                        field_name=required_field,
                        source_type=detected_type,
                        target_type=expected_type,
                        source_sample=sample_value,
                        target_expected=None,
                        severity="medium"
                    )
                    incompatibilities.append(incompatibility)
            else:
                compatible_fields.append(required_field)
        
        # Check for extra fields in source data
        for field_name in sample_user.keys():
            if field_name not in self.marzban_user_schema:
                extra_fields.append(field_name)
        
        total_fields = len(self.marzban_user_schema)
        compatibility_score = len(compatible_fields) / total_fields if total_fields > 0 else 0.0
        
        return SchemaAnalysisResult(
            incompatibilities=incompatibilities,
            compatible_fields=compatible_fields,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
            total_fields_analyzed=total_fields,
            compatibility_score=compatibility_score
        )

    def analyze_admin_schema_compatibility(self, marzneshin_admins: List[Dict[str, Any]]) -> SchemaAnalysisResult:
        """Analyze compatibility between Marzneshin admin data and Marzban admin schema."""
        if not marzneshin_admins:
            return SchemaAnalysisResult([], [], [], [], 0, 1.0)

        incompatibilities = []
        compatible_fields = []
        missing_fields = []
        extra_fields = []
        
        sample_admin = marzneshin_admins[0]
        
        for required_field, expected_type in self.marzban_admin_schema.items():
            if required_field not in sample_admin:
                missing_fields.append(required_field)
                continue
                
            sample_value = sample_admin[required_field]
            detected_type = self._detect_field_type(sample_value)
            
            if detected_type != expected_type:
                incompatibility = FieldIncompatibility(
                    field_name=required_field,
                    source_type=detected_type,
                    target_type=expected_type,
                    source_sample=sample_value,
                    target_expected=None,
                    severity="medium"
                )
                incompatibilities.append(incompatibility)
            else:
                compatible_fields.append(required_field)
        
        for field_name in sample_admin.keys():
            if field_name not in self.marzban_admin_schema:
                extra_fields.append(field_name)
        
        total_fields = len(self.marzban_admin_schema)
        compatibility_score = len(compatible_fields) / total_fields if total_fields > 0 else 0.0
        
        return SchemaAnalysisResult(
            incompatibilities=incompatibilities,
            compatible_fields=compatible_fields,
            missing_fields=missing_fields,
            extra_fields=extra_fields,
            total_fields_analyzed=total_fields,
            compatibility_score=compatibility_score
        )

    def generate_compatibility_report(self, user_analysis: SchemaAnalysisResult, 
                                    admin_analysis: SchemaAnalysisResult) -> str:
        """Generate a comprehensive compatibility report."""
        report = []
        report.append("=" * 80)
        report.append("SCHEMA COMPATIBILITY ANALYSIS REPORT")
        report.append("=" * 80)
        report.append("")
        
        # User schema analysis
        report.append("USER SCHEMA ANALYSIS:")
        report.append(f"  Compatibility Score: {user_analysis.compatibility_score:.2%}")
        report.append(f"  Compatible Fields: {len(user_analysis.compatible_fields)}")
        report.append(f"  Incompatible Fields: {len(user_analysis.incompatibilities)}")
        report.append(f"  Missing Fields: {len(user_analysis.missing_fields)}")
        report.append(f"  Extra Fields: {len(user_analysis.extra_fields)}")
        report.append("")
        
        if user_analysis.incompatibilities:
            report.append("  INCOMPATIBILITIES:")
            for incomp in user_analysis.incompatibilities:
                report.append(f"    - {incomp.field_name}: {incomp.source_type.value} -> {incomp.target_type.value} ({incomp.severity})")
                if incomp.conversion_function:
                    report.append(f"      Conversion: {incomp.conversion_function}")
                report.append(f"      Sample: {incomp.source_sample} -> {incomp.target_expected}")
        
        if user_analysis.missing_fields:
            report.append(f"  MISSING FIELDS: {', '.join(user_analysis.missing_fields)}")
        
        if user_analysis.extra_fields:
            report.append(f"  EXTRA FIELDS: {', '.join(user_analysis.extra_fields)}")
        
        report.append("")
        
        # Admin schema analysis
        report.append("ADMIN SCHEMA ANALYSIS:")
        report.append(f"  Compatibility Score: {admin_analysis.compatibility_score:.2%}")
        report.append(f"  Compatible Fields: {len(admin_analysis.compatible_fields)}")
        report.append(f"  Incompatible Fields: {len(admin_analysis.incompatibilities)}")
        report.append(f"  Missing Fields: {len(admin_analysis.missing_fields)}")
        report.append(f"  Extra Fields: {len(admin_analysis.extra_fields)}")
        
        if admin_analysis.incompatibilities:
            report.append("  INCOMPATIBILITIES:")
            for incomp in admin_analysis.incompatibilities:
                report.append(f"    - {incomp.field_name}: {incomp.source_type.value} -> {incomp.target_type.value} ({incomp.severity})")
        
        report.append("")
        report.append("=" * 80)
        
        return "\n".join(report)

    def get_conversion_recommendations(self, analysis_result: SchemaAnalysisResult) -> List[str]:
        """Get recommendations for fixing schema incompatibilities."""
        recommendations = []
        
        for incomp in analysis_result.incompatibilities:
            if incomp.field_name == "expire" and incomp.conversion_function:
                recommendations.append(
                    f"Convert {incomp.field_name} from ISO datetime to Unix timestamp using {incomp.conversion_function}"
                )
            elif incomp.source_type == FieldType.STRING and incomp.target_type == FieldType.INTEGER:
                recommendations.append(
                    f"Convert {incomp.field_name} from string to integer with validation"
                )
            elif incomp.source_type == FieldType.DATETIME_ISO and incomp.target_type == FieldType.DATETIME_UNIX:
                recommendations.append(
                    f"Convert {incomp.field_name} from ISO datetime to Unix timestamp"
                )
            else:
                recommendations.append(
                    f"Review and fix {incomp.field_name} type conversion: {incomp.source_type.value} -> {incomp.target_type.value}"
                )
        
        return recommendations