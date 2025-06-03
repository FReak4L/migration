# migration/tests/test_schema_analyzer.py

import unittest
from datetime import datetime, timezone
from core.schema_analyzer import SchemaAnalyzer, FieldType, FieldIncompatibility


class TestSchemaAnalyzer(unittest.TestCase):
    """Test cases for schema compatibility analysis."""
    
    def setUp(self):
        self.analyzer = SchemaAnalyzer()
        
        # Sample Marzneshin user data with schema incompatibilities
        self.sample_marzneshin_users = [
            {
                "id": 1,
                "username": "test_user",
                "enabled": True,
                "expired": False,
                "expire_date": "2024-12-31T23:59:59Z",  # ISO format (incompatible)
                "data_limit": 1073741824,
                "used_traffic": 536870912,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-15T12:00:00Z",
                "note": "Test user",
                "key": "1234567890abcdef1234567890abcdef",
                "proxies": {
                    "vless": {"id": "12345678-1234-1234-1234-123456789abc"}
                }
            }
        ]
        
        self.sample_marzneshin_admins = [
            {
                "id": 1,
                "username": "admin_user",
                "is_sudo": True,
                "created_at": "2024-01-01T00:00:00Z",
                "telegram_id": 123456789,
                "discord_webhook": "https://discord.com/webhook"
            }
        ]

    def test_detect_field_type(self):
        """Test field type detection."""
        self.assertEqual(self.analyzer._detect_field_type("test"), FieldType.STRING)
        self.assertEqual(self.analyzer._detect_field_type(123), FieldType.INTEGER)
        self.assertEqual(self.analyzer._detect_field_type(True), FieldType.BOOLEAN)
        self.assertEqual(self.analyzer._detect_field_type(2147483648), FieldType.BIGINT)
        self.assertEqual(self.analyzer._detect_field_type("2024-01-01T00:00:00Z"), FieldType.DATETIME_ISO)
        self.assertEqual(self.analyzer._detect_field_type({"key": "value"}), FieldType.JSON)

    def test_is_iso_datetime(self):
        """Test ISO datetime detection."""
        self.assertTrue(self.analyzer._is_iso_datetime("2024-01-01T00:00:00Z"))
        self.assertTrue(self.analyzer._is_iso_datetime("2024-01-01T00:00:00+00:00"))
        self.assertFalse(self.analyzer._is_iso_datetime("2024-01-01"))
        self.assertFalse(self.analyzer._is_iso_datetime("not a date"))

    def test_is_uuid_format(self):
        """Test UUID format detection."""
        self.assertTrue(self.analyzer._is_uuid_format("12345678-1234-1234-1234-123456789abc"))
        self.assertFalse(self.analyzer._is_uuid_format("1234567890abcdef1234567890abcdef"))
        self.assertFalse(self.analyzer._is_uuid_format("not-a-uuid"))

    def test_analyze_user_schema_compatibility(self):
        """Test user schema compatibility analysis."""
        result = self.analyzer.analyze_user_schema_compatibility(self.sample_marzneshin_users)
        
        # Should detect expire field incompatibility (ISO -> Unix timestamp)
        expire_incompatibility = next(
            (incomp for incomp in result.incompatibilities if incomp.field_name == "expire"), 
            None
        )
        self.assertIsNotNone(expire_incompatibility)
        self.assertEqual(expire_incompatibility.source_type, FieldType.DATETIME_ISO)
        self.assertEqual(expire_incompatibility.target_type, FieldType.DATETIME_UNIX)
        self.assertEqual(expire_incompatibility.conversion_function, "datetime_to_unix_timestamp")
        
        # Should detect missing fields
        self.assertIn("status", result.missing_fields)
        self.assertIn("admin_id", result.missing_fields)
        
        # Should detect extra fields
        self.assertIn("id", result.extra_fields)
        self.assertIn("enabled", result.extra_fields)
        self.assertIn("expired", result.extra_fields)

    def test_analyze_admin_schema_compatibility(self):
        """Test admin schema compatibility analysis."""
        result = self.analyzer.analyze_admin_schema_compatibility(self.sample_marzneshin_admins)
        
        # Should detect missing fields
        self.assertIn("hashed_password", result.missing_fields)
        self.assertIn("users_usage", result.missing_fields)
        
        # Should detect extra fields
        self.assertIn("id", result.extra_fields)

    def test_generate_compatibility_report(self):
        """Test compatibility report generation."""
        user_analysis = self.analyzer.analyze_user_schema_compatibility(self.sample_marzneshin_users)
        admin_analysis = self.analyzer.analyze_admin_schema_compatibility(self.sample_marzneshin_admins)
        
        report = self.analyzer.generate_compatibility_report(user_analysis, admin_analysis)
        
        self.assertIn("SCHEMA COMPATIBILITY ANALYSIS REPORT", report)
        self.assertIn("USER SCHEMA ANALYSIS:", report)
        self.assertIn("ADMIN SCHEMA ANALYSIS:", report)
        self.assertIn("Compatibility Score:", report)

    def test_get_conversion_recommendations(self):
        """Test conversion recommendations."""
        user_analysis = self.analyzer.analyze_user_schema_compatibility(self.sample_marzneshin_users)
        recommendations = self.analyzer.get_conversion_recommendations(user_analysis)
        
        # Should recommend expire field conversion
        expire_recommendation = next(
            (rec for rec in recommendations if "expire" in rec and "Unix timestamp" in rec),
            None
        )
        self.assertIsNotNone(expire_recommendation)

    def test_empty_data_handling(self):
        """Test handling of empty data."""
        user_result = self.analyzer.analyze_user_schema_compatibility([])
        admin_result = self.analyzer.analyze_admin_schema_compatibility([])
        
        self.assertEqual(user_result.compatibility_score, 1.0)
        self.assertEqual(admin_result.compatibility_score, 1.0)
        self.assertEqual(len(user_result.incompatibilities), 0)
        self.assertEqual(len(admin_result.incompatibilities), 0)


if __name__ == "__main__":
    unittest.main()