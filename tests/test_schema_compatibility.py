# migration/tests/test_schema_compatibility.py

import pytest
from datetime import datetime, timezone
from typing import Dict, Any

from core.schema_compatibility import (
    SchemaCompatibilityEngine, FieldMapping, FieldType, ConversionStrategy,
    SchemaValidator
)


class TestSchemaCompatibilityEngine:
    """Test suite for SchemaCompatibilityEngine."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.engine = SchemaCompatibilityEngine(ConversionStrategy.LENIENT)
    
    def test_datetime_iso_to_unix_conversion(self):
        """Test ISO 8601 to Unix timestamp conversion."""
        # Test standard ISO format
        iso_date = "2024-01-15T10:30:00Z"
        result = self.engine._convert_datetime_iso_to_unix(iso_date)
        assert result is not None
        assert isinstance(result, int)
        
        # Test ISO format with timezone
        iso_date_tz = "2024-01-15T10:30:00+02:00"
        result_tz = self.engine._convert_datetime_iso_to_unix(iso_date_tz)
        assert result_tz is not None
        
        # Test ISO format with microseconds
        iso_date_micro = "2024-01-15T10:30:00.123456Z"
        result_micro = self.engine._convert_datetime_iso_to_unix(iso_date_micro)
        assert result_micro is not None
        
        # Test ISO format with excessive microseconds (should be truncated)
        iso_date_excess_micro = "2024-01-15T10:30:00.123456789Z"
        result_excess = self.engine._convert_datetime_iso_to_unix(iso_date_excess_micro)
        assert result_excess is not None
        
        # Test None input
        assert self.engine._convert_datetime_iso_to_unix(None) is None
        
        # Test invalid format
        assert self.engine._convert_datetime_iso_to_unix("invalid-date") is None
        assert len(self.engine.conversion_errors) > 0
    
    def test_unix_to_datetime_iso_conversion(self):
        """Test Unix timestamp to ISO 8601 conversion."""
        # Test valid timestamp
        timestamp = 1705312200  # 2024-01-15T10:30:00Z
        result = self.engine._convert_unix_to_datetime_iso(timestamp)
        assert result is not None
        assert result.endswith('Z')
        assert '2024-01-15' in result
        
        # Test None input
        assert self.engine._convert_unix_to_datetime_iso(None) is None
        
        # Test negative timestamp
        assert self.engine._convert_unix_to_datetime_iso(-1) is None
        
        # Test invalid timestamp
        assert self.engine._convert_unix_to_datetime_iso("invalid") is None
    
    def test_uuid_format_conversion(self):
        """Test UUID format conversion."""
        # Test 32-char hex string to UUID with dashes
        uuid_no_dash = "550e8400e29b41d4a716446655440000"
        result = self.engine._convert_uuid_format(uuid_no_dash, add_dashes=True)
        expected = "550e8400-e29b-41d4-a716-446655440000"
        assert result == expected
        
        # Test UUID with dashes to clean format
        uuid_with_dash = "550e8400-e29b-41d4-a716-446655440000"
        result_clean = self.engine._convert_uuid_format(uuid_with_dash, add_dashes=False)
        assert result_clean == uuid_no_dash
        
        # Test invalid UUID
        assert self.engine._convert_uuid_format("invalid-uuid") is None
        assert self.engine._convert_uuid_format("") is None
        assert self.engine._convert_uuid_format(None) is None
    
    def test_field_conversion_string(self):
        """Test string field conversion."""
        mapping = FieldMapping("source", "target", FieldType.INTEGER, FieldType.STRING)
        
        # Test integer to string
        result = self.engine._apply_field_conversion(123, mapping)
        assert result == "123"
        
        # Test None with default
        mapping.default_value = "default"
        mapping.required = False
        result = self.engine._apply_field_conversion(None, mapping)
        assert result == "default"
    
    def test_field_conversion_integer(self):
        """Test integer field conversion."""
        mapping = FieldMapping("source", "target", FieldType.STRING, FieldType.INTEGER, default_value=0)
        
        # Test valid string to integer
        result = self.engine._apply_field_conversion("123", mapping)
        assert result == 123
        
        # Test invalid string to integer (lenient mode)
        result = self.engine._apply_field_conversion("invalid", mapping)
        assert result == 0  # default value
        
        # Test None
        result = self.engine._apply_field_conversion(None, mapping)
        assert result == 0
    
    def test_field_conversion_boolean(self):
        """Test boolean field conversion."""
        mapping = FieldMapping("source", "target", FieldType.STRING, FieldType.BOOLEAN, default_value=False)
        
        # Test string to boolean
        assert self.engine._apply_field_conversion("true", mapping) is True
        assert self.engine._apply_field_conversion("false", mapping) is False
        assert self.engine._apply_field_conversion("1", mapping) is True
        assert self.engine._apply_field_conversion("0", mapping) is False
        
        # Test integer to boolean
        assert self.engine._apply_field_conversion(1, mapping) is True
        assert self.engine._apply_field_conversion(0, mapping) is False
        
        # Test invalid value
        result = self.engine._apply_field_conversion("invalid", mapping)
        assert result is False  # default value
    
    def test_transform_record(self):
        """Test complete record transformation."""
        source_record = {
            "username": "testuser",
            "expire_date": "2024-12-31T23:59:59Z",
            "data_limit": "1000000000",
            "enabled": "true"
        }
        
        mappings = [
            FieldMapping("username", "username", FieldType.STRING, FieldType.STRING),
            FieldMapping("expire_date", "expire", FieldType.DATETIME_ISO, FieldType.DATETIME_UNIX),
            FieldMapping("data_limit", "data_limit", FieldType.STRING, FieldType.INTEGER, default_value=0),
            FieldMapping("enabled", "is_active", FieldType.STRING, FieldType.BOOLEAN, default_value=False)
        ]
        
        result = self.engine.transform_record(source_record, mappings)
        
        assert result["username"] == "testuser"
        assert isinstance(result["expire"], int)
        assert result["data_limit"] == 1000000000
        assert result["is_active"] is True
    
    def test_marzneshin_to_marzban_user_mappings(self):
        """Test predefined Marzneshin to Marzban user mappings."""
        mappings = self.engine.get_marzneshin_to_marzban_user_mappings()
        
        assert len(mappings) > 0
        
        # Check that essential fields are mapped
        field_names = [m.target_field for m in mappings]
        assert "username" in field_names
        assert "expire" in field_names
        assert "data_limit" in field_names
        assert "edit_at" in field_names
    
    def test_conversion_report(self):
        """Test conversion error reporting."""
        # Trigger some conversion errors
        self.engine._convert_datetime_iso_to_unix("invalid-date")
        self.engine._convert_uuid_format("invalid-uuid")
        
        report = self.engine.get_conversion_report()
        
        assert report["total_errors"] >= 2
        assert len(report["errors"]) >= 2
        assert isinstance(report["errors"], list)
        
        # Test clearing log
        self.engine.clear_conversion_log()
        report_after_clear = self.engine.get_conversion_report()
        assert report_after_clear["total_errors"] == 0
    
    def test_strict_conversion_strategy(self):
        """Test strict conversion strategy."""
        strict_engine = SchemaCompatibilityEngine(ConversionStrategy.STRICT)
        mapping = FieldMapping("source", "target", FieldType.STRING, FieldType.INTEGER, 
                              conversion_strategy=ConversionStrategy.STRICT)
        
        # Should raise exception in strict mode
        with pytest.raises(ValueError):
            strict_engine._apply_field_conversion("invalid", mapping)


class TestSchemaValidator:
    """Test suite for SchemaValidator."""
    
    def test_schema_validation(self):
        """Test schema compatibility validation."""
        source_schema = {
            "username": "string",
            "expire_date": "datetime_iso",
            "data_limit": "integer"
        }
        
        target_schema = {
            "username": "string",
            "expire": "datetime_unix",
            "data_limit": "integer"
        }
        
        validator = SchemaValidator(source_schema=source_schema, target_schema=target_schema)
        
        mappings = [
            FieldMapping("username", "username", FieldType.STRING, FieldType.STRING),
            FieldMapping("expire_date", "expire", FieldType.DATETIME_ISO, FieldType.DATETIME_UNIX),
            FieldMapping("data_limit", "data_limit", FieldType.INTEGER, FieldType.INTEGER)
        ]
        
        is_valid, issues = validator.validate_compatibility(mappings)
        
        # Should have one issue: target field type mismatch for expire
        assert not is_valid
        assert len(issues) > 0


# Integration test
def test_end_to_end_transformation():
    """Test complete end-to-end transformation scenario."""
    engine = SchemaCompatibilityEngine(ConversionStrategy.LENIENT)
    
    # Sample Marzneshin user data
    marzneshin_user = {
        "username": "john_doe",
        "expire_date": "2024-12-31T23:59:59.123456Z",
        "data_limit": 5000000000,
        "used_traffic": 1000000000,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-06-01T12:00:00Z",
        "note": "Premium user",
        "key": "550e8400e29b41d4a716446655440000",
        "enabled": True,
        "data_limit_reset_strategy": "monthly"
    }
    
    # Transform using predefined mappings
    mappings = engine.get_marzneshin_to_marzban_user_mappings()
    result = engine.transform_record(marzneshin_user, mappings)
    
    # Verify transformation
    assert result["username"] == "john_doe"
    assert isinstance(result["expire"], int)
    assert result["data_limit"] == 5000000000
    assert result["used_traffic"] == 1000000000
    assert result["note"] == "Premium user"
    assert result["data_limit_reset_strategy"] == "monthly"
    assert "550e8400-e29b-41d4-a716-446655440000" in result["uuid_key"]
    
    # Check conversion report
    report = engine.get_conversion_report()
    print(f"Conversion completed with {report['total_errors']} errors and {report['total_warnings']} warnings")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])