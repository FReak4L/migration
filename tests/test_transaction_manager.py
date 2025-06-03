# migration/tests/test_transaction_manager.py

import asyncio
import unittest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from core.transaction_manager import (
    TransactionManager, TransactionState, CheckpointType,
    TransactionOperation, TransactionCheckpoint, TransactionLog
)
from core.rollback_handlers import DatabaseRollbackHandlers, DatabaseConnection


class TestTransactionManager(unittest.TestCase):
    """Test cases for transaction management functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.transaction_manager = TransactionManager(
            checkpoint_interval=5,
            max_operations_per_transaction=100,
            enable_persistence=True,
            transaction_log_dir=self.temp_dir
        )
        
        # Mock rollback handler
        self.mock_handler = AsyncMock(return_value=True)
        self.transaction_manager.register_rollback_handler("test_operation", self.mock_handler)
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir)
    
    def test_transaction_manager_initialization(self):
        """Test transaction manager initialization."""
        self.assertEqual(self.transaction_manager.checkpoint_interval, 5)
        self.assertEqual(self.transaction_manager.max_operations_per_transaction, 100)
        self.assertTrue(self.transaction_manager.enable_persistence)
        self.assertEqual(self.transaction_manager.transaction_log_dir, self.temp_dir)
        self.assertTrue(self.temp_dir.exists())
    
    async def test_basic_transaction_lifecycle(self):
        """Test basic transaction creation, operation, and commit."""
        async with self.transaction_manager.transaction() as transaction_id:
            self.assertIsNotNone(transaction_id)
            self.assertIn(transaction_id, self.transaction_manager.get_active_transactions())
            
            # Add an operation
            operation_id = await self.transaction_manager.add_operation(
                transaction_id=transaction_id,
                operation_type="test_operation",
                target_table="test_table",
                operation_data={"key": "value"}
            )
            
            self.assertIsNotNone(operation_id)
            
            # Check transaction status
            status = self.transaction_manager.get_transaction_status(transaction_id)
            self.assertIsNotNone(status)
            self.assertEqual(status["state"], TransactionState.ACTIVE.value)
            self.assertEqual(status["operation_count"], 1)
        
        # Transaction should be completed and removed from active list
        self.assertNotIn(transaction_id, self.transaction_manager.get_active_transactions())
    
    async def test_transaction_rollback_on_exception(self):
        """Test that transaction is rolled back when exception occurs."""
        transaction_id = None
        
        try:
            async with self.transaction_manager.transaction() as txn_id:
                transaction_id = txn_id
                
                # Add an operation
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": "value"}
                )
                
                # Raise an exception to trigger rollback
                raise ValueError("Test exception")
        
        except ValueError:
            pass  # Expected exception
        
        # Verify rollback handler was called
        self.mock_handler.assert_called_once()
        
        # Transaction should be removed from active list
        self.assertNotIn(transaction_id, self.transaction_manager.get_active_transactions())
    
    async def test_automatic_checkpoints(self):
        """Test automatic checkpoint creation."""
        async with self.transaction_manager.transaction() as transaction_id:
            # Add operations to trigger automatic checkpoint
            for i in range(7):  # More than checkpoint_interval (5)
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": f"value_{i}"}
                )
            
            status = self.transaction_manager.get_transaction_status(transaction_id)
            self.assertGreater(status["checkpoint_count"], 0)
    
    async def test_manual_checkpoint_creation(self):
        """Test manual checkpoint creation."""
        async with self.transaction_manager.transaction() as transaction_id:
            # Add some operations
            for i in range(3):
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": f"value_{i}"}
                )
            
            # Create manual checkpoint
            checkpoint_id = await self.transaction_manager.create_manual_checkpoint(
                transaction_id,
                {"test": "metadata"}
            )
            
            self.assertIsNotNone(checkpoint_id)
            
            status = self.transaction_manager.get_transaction_status(transaction_id)
            self.assertEqual(status["checkpoint_count"], 1)
            self.assertEqual(status["last_checkpoint"], checkpoint_id)
    
    async def test_rollback_to_checkpoint(self):
        """Test rollback to a specific checkpoint."""
        async with self.transaction_manager.transaction() as transaction_id:
            # Add some operations
            for i in range(3):
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": f"value_{i}"}
                )
            
            # Create checkpoint
            checkpoint_id = await self.transaction_manager.create_manual_checkpoint(transaction_id)
            
            # Add more operations after checkpoint
            for i in range(3, 6):
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": f"value_{i}"}
                )
            
            # Verify we have 6 operations
            status = self.transaction_manager.get_transaction_status(transaction_id)
            self.assertEqual(status["operation_count"], 6)
            
            # Rollback to checkpoint
            success = await self.transaction_manager.rollback_to_checkpoint(
                transaction_id, checkpoint_id
            )
            
            self.assertTrue(success)
            
            # Verify operations after checkpoint were rolled back
            status = self.transaction_manager.get_transaction_status(transaction_id)
            self.assertEqual(status["operation_count"], 3)
    
    async def test_force_rollback_transaction(self):
        """Test forced rollback of active transaction."""
        async with self.transaction_manager.transaction() as transaction_id:
            # Add some operations
            await self.transaction_manager.add_operation(
                transaction_id=transaction_id,
                operation_type="test_operation",
                target_table="test_table",
                operation_data={"key": "value"}
            )
            
            # Force rollback
            success = await self.transaction_manager.force_rollback_transaction(
                transaction_id, "Test forced rollback"
            )
            
            self.assertTrue(success)
            self.mock_handler.assert_called()
    
    async def test_operation_limit_enforcement(self):
        """Test that operation limit is enforced."""
        # Create transaction manager with low limit
        limited_manager = TransactionManager(max_operations_per_transaction=2)
        
        async with limited_manager.transaction() as transaction_id:
            # Add operations up to limit
            await limited_manager.add_operation(
                transaction_id=transaction_id,
                operation_type="test_operation",
                target_table="test_table",
                operation_data={"key": "value1"}
            )
            
            await limited_manager.add_operation(
                transaction_id=transaction_id,
                operation_type="test_operation",
                target_table="test_table",
                operation_data={"key": "value2"}
            )
            
            # Third operation should raise exception
            with self.assertRaises(ValueError):
                await limited_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": "value3"}
                )
    
    async def test_transaction_persistence(self):
        """Test transaction log persistence."""
        transaction_id = None
        
        async with self.transaction_manager.transaction() as txn_id:
            transaction_id = txn_id
            
            await self.transaction_manager.add_operation(
                transaction_id=transaction_id,
                operation_type="test_operation",
                target_table="test_table",
                operation_data={"key": "value"}
            )
        
        # Check that log file was created
        log_file = self.temp_dir / f"{transaction_id}.json"
        self.assertTrue(log_file.exists())
        
        # Verify log file content
        import json
        with open(log_file) as f:
            log_data = json.load(f)
        
        self.assertEqual(log_data["transaction_id"], transaction_id)
        self.assertEqual(log_data["state"], TransactionState.COMMITTED.value)
        self.assertEqual(len(log_data["operations"]), 1)
    
    def test_rollback_handler_registration(self):
        """Test rollback handler registration."""
        handler = AsyncMock()
        self.transaction_manager.register_rollback_handler("new_operation", handler)
        
        # Verify handler is registered
        self.assertIn("new_operation", self.transaction_manager._rollback_handlers)
        self.assertEqual(self.transaction_manager._rollback_handlers["new_operation"], handler)
    
    async def test_concurrent_transactions(self):
        """Test handling of concurrent transactions."""
        async def create_transaction(transaction_id):
            async with self.transaction_manager.transaction(transaction_id=transaction_id):
                await self.transaction_manager.add_operation(
                    transaction_id=transaction_id,
                    operation_type="test_operation",
                    target_table="test_table",
                    operation_data={"key": f"value_{transaction_id}"}
                )
                await asyncio.sleep(0.01)  # Simulate some work
        
        # Create multiple concurrent transactions
        tasks = [
            create_transaction(f"txn_{i}")
            for i in range(3)
        ]
        
        await asyncio.gather(*tasks)
        
        # All transactions should complete successfully
        self.assertEqual(len(self.transaction_manager.get_active_transactions()), 0)


class TestDatabaseRollbackHandlers(unittest.TestCase):
    """Test cases for database rollback handlers."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_connection = DatabaseConnection("test://connection")
        self.handlers = DatabaseRollbackHandlers(self.db_connection)
    
    async def test_rollback_insert_user(self):
        """Test user insert rollback."""
        operation = TransactionOperation(
            operation_id="test_op",
            operation_type="insert_user",
            target_table="users",
            operation_data={"id": 1, "username": "testuser", "email": "test@example.com"}
        )
        
        success = await self.handlers.rollback_insert_user(operation)
        self.assertTrue(success)
    
    async def test_rollback_update_user(self):
        """Test user update rollback."""
        operation = TransactionOperation(
            operation_id="test_op",
            operation_type="update_user",
            target_table="users",
            operation_data={"id": 1, "username": "testuser", "email": "newemail@example.com"},
            rollback_data={"id": 1, "username": "testuser", "email": "oldemail@example.com"}
        )
        
        success = await self.handlers.rollback_update_user(operation)
        self.assertTrue(success)
    
    async def test_rollback_delete_user(self):
        """Test user delete rollback."""
        operation = TransactionOperation(
            operation_id="test_op",
            operation_type="delete_user",
            target_table="users",
            operation_data={"id": 1},
            rollback_data={"id": 1, "username": "testuser", "email": "test@example.com"}
        )
        
        success = await self.handlers.rollback_delete_user(operation)
        self.assertTrue(success)
    
    async def test_rollback_insert_proxy(self):
        """Test proxy insert rollback."""
        operation = TransactionOperation(
            operation_id="test_op",
            operation_type="insert_proxy",
            target_table="proxies",
            operation_data={"id": 1, "tag": "test_proxy", "type": "vless"}
        )
        
        success = await self.handlers.rollback_insert_proxy(operation)
        self.assertTrue(success)
    
    async def test_get_rollback_handlers(self):
        """Test getting all rollback handlers."""
        handlers = self.handlers.get_rollback_handlers()
        
        self.assertIn("insert_user", handlers)
        self.assertIn("update_user", handlers)
        self.assertIn("delete_user", handlers)
        self.assertIn("insert_proxy", handlers)
        self.assertIn("update_proxy", handlers)
        self.assertIn("delete_proxy", handlers)
        self.assertIn("insert_admin", handlers)
        self.assertIn("batch_operation", handlers)


# Async test runner
def run_async_test(test_func):
    """Helper to run async test functions."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(test_func())
    finally:
        loop.close()


if __name__ == "__main__":
    # Run async tests
    test_instance = TestTransactionManager()
    test_instance.setUp()
    
    print("Running async transaction manager tests...")
    
    try:
        run_async_test(test_instance.test_basic_transaction_lifecycle)
        print("✅ Basic transaction lifecycle test passed")
        
        run_async_test(test_instance.test_transaction_rollback_on_exception)
        print("✅ Transaction rollback on exception test passed")
        
        run_async_test(test_instance.test_automatic_checkpoints)
        print("✅ Automatic checkpoints test passed")
        
        run_async_test(test_instance.test_manual_checkpoint_creation)
        print("✅ Manual checkpoint creation test passed")
        
        run_async_test(test_instance.test_rollback_to_checkpoint)
        print("✅ Rollback to checkpoint test passed")
        
        run_async_test(test_instance.test_concurrent_transactions)
        print("✅ Concurrent transactions test passed")
        
        print("\nRunning rollback handler tests...")
        
        handler_test = TestDatabaseRollbackHandlers()
        handler_test.setUp()
        
        run_async_test(handler_test.test_rollback_insert_user)
        print("✅ Rollback insert user test passed")
        
        run_async_test(handler_test.test_rollback_update_user)
        print("✅ Rollback update user test passed")
        
        print("\n🎉 All transaction manager tests passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        raise
    finally:
        test_instance.tearDown()