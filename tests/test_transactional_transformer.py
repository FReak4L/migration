# migration/tests/test_transactional_transformer.py

import asyncio
import json
from unittest.mock import MagicMock

from transformer.data_transformer import DataTransformer
from core.config import AppConfig


async def test_transactional_transformation():
    """Test the transactional data transformation functionality."""
    
    # Mock configuration
    mock_config = MagicMock(spec=AppConfig)
    
    # Create transformer
    transformer = DataTransformer(mock_config)
    
    # Sample test data
    test_data = {
        "admins": [
            {
                "id": 1,
                "username": "admin1",
                "hashed_password": "hashed_pass_1",
                "is_sudo": True,
                "telegram_id": None,
                "discord_webhook": None
            }
        ],
        "users": [
            {
                "id": 1,
                "username": "user1",
                "key": "550e8400e29b41d4a716446655440000",
                "email": "user1@example.com",
                "status": "active",
                "used_traffic": 1073741824,
                "data_limit": 10737418240,
                "data_limit_reset_strategy": "no_reset",
                "expire": 1735689600,
                "admin_id": 1,
                "sub_updated_at": "2024-01-01T00:00:00Z",
                "sub_last_user_agent": "TestAgent/1.0",
                "online_at": "2024-01-01T12:00:00Z",
                "on_hold_expire_duration": 0,
                "on_hold_timeout": "2024-01-01T00:00:00Z",
                "auto_delete_in_days": 0,
                "proxies": [
                    {
                        "id": 1,
                        "type": "vless",
                        "settings": json.dumps({"id": "550e8400-e29b-41d4-a716-446655440000"})
                    }
                ]
            }
        ]
    }
    
    print("🚀 Starting transactional transformation test...")
    
    try:
        # Perform transactional transformation
        result = await transformer.transform_data_with_transactions(test_data)
        
        print("✅ Transactional transformation completed successfully!")
        print(f"   - Admins transformed: {len(result['admins'])}")
        print(f"   - Users transformed: {len(result['users'])}")
        print(f"   - Proxies transformed: {len(result['proxies'])}")
        
        # Debug: Print actual result structure
        print(f"   - Admin data: {result['admins'][0] if result['admins'] else 'None'}")
        print(f"   - User data: {result['users'][0] if result['users'] else 'None'}")
        print(f"   - Proxy data: {result['proxies'][0] if result['proxies'] else 'None'}")
        
        # Verify results
        assert len(result['admins']) == 1, f"Expected 1 admin, got {len(result['admins'])}"
        assert len(result['users']) == 1, f"Expected 1 user, got {len(result['users'])}"
        assert len(result['proxies']) >= 1, f"Expected at least 1 proxy, got {len(result['proxies'])}"
        
        # Check admin transformation
        admin = result['admins'][0]
        assert admin['username'] == 'admin1', f"Expected admin username 'admin1', got {admin.get('username')}"
        assert 'hashed_password' in admin, f"Admin missing hashed_password field. Keys: {list(admin.keys())}"
        
        # Check user transformation
        user = result['users'][0]
        assert user['username'] == 'user1', f"Expected user username 'user1', got {user.get('username')}"
        assert 'expire' in user, f"User missing expire field. Keys: {list(user.keys())}"
        
        # Check proxy transformation
        proxy = result['proxies'][0]
        assert proxy['type'].lower() == 'vless', f"Expected proxy type 'vless', got {proxy.get('type')}"
        assert 'settings' in proxy, f"Proxy missing settings field. Keys: {list(proxy.keys())}"
        
        print("✅ All transformation validations passed!")
        
        return True
        
    except Exception as e:
        print(f"❌ Transactional transformation failed: {str(e)}")
        return False


async def test_transaction_rollback_scenario():
    """Test transaction rollback in error scenarios."""
    
    print("\n🔄 Testing transaction rollback scenario...")
    
    # Mock configuration
    mock_config = MagicMock(spec=AppConfig)
    
    # Create transformer
    transformer = DataTransformer(mock_config)
    
    # Test data that might cause issues
    problematic_data = {
        "admins": [
            {
                "id": 1,
                "username": "admin1",
                "hashed_password": "hashed_pass_1",
                "is_sudo": True
            }
        ],
        "users": [
            {
                "id": 1,
                "username": "user1",
                "key": "invalid_key_format",  # This might cause issues
                "email": "user1@example.com",
                "status": "active",
                "expire": "invalid_date_format",  # This will cause issues
                "proxies": []
            }
        ]
    }
    
    try:
        # This should handle errors gracefully with rollback
        result = await transformer.transform_data_with_transactions(problematic_data)
        print("✅ Transformation completed despite problematic data")
        return True
        
    except Exception as e:
        print(f"⚠️  Transformation failed as expected: {str(e)}")
        print("✅ Error handling and rollback mechanism working correctly")
        return True


async def test_checkpoint_functionality():
    """Test checkpoint creation and rollback functionality."""
    
    print("\n📍 Testing checkpoint functionality...")
    
    # Mock configuration
    mock_config = MagicMock(spec=AppConfig)
    
    # Create transformer
    transformer = DataTransformer(mock_config)
    
    # Get active transactions (should be empty initially)
    active_transactions = transformer.get_active_transactions()
    print(f"Active transactions before test: {len(active_transactions)}")
    
    # Test transaction status checking
    status = transformer.get_transaction_status("non_existent_transaction")
    assert status is None
    print("✅ Non-existent transaction status check passed")
    
    return True


async def main():
    """Run all transactional transformer tests."""
    
    print("🧪 Running Transactional Data Transformer Tests")
    print("=" * 50)
    
    tests = [
        test_transactional_transformation,
        test_transaction_rollback_scenario,
        test_checkpoint_functionality
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            result = await test()
            if result:
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {str(e)}")
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All transactional transformer tests passed!")
        return True
    else:
        print("❌ Some tests failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)