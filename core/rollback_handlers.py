# migration/core/rollback_handlers.py

import asyncio
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from core.logging_setup import get_logger
from core.transaction_manager import TransactionOperation

logger = get_logger()


@dataclass
class DatabaseConnection:
    """Mock database connection for rollback operations."""
    connection_string: str
    is_connected: bool = False
    
    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> bool:
        """Mock database execution."""
        logger.debug(f"Executing query: {query}")
        if params:
            logger.debug(f"Query parameters: {params}")
        # Simulate database operation
        await asyncio.sleep(0.001)
        return True
    
    async def begin_transaction(self):
        """Begin database transaction."""
        logger.debug("Beginning database transaction")
        await asyncio.sleep(0.001)
    
    async def commit(self):
        """Commit database transaction."""
        logger.debug("Committing database transaction")
        await asyncio.sleep(0.001)
    
    async def rollback(self):
        """Rollback database transaction."""
        logger.debug("Rolling back database transaction")
        await asyncio.sleep(0.001)


class DatabaseRollbackHandlers:
    """Collection of rollback handlers for database operations."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize rollback handlers.
        
        Args:
            db_connection: Database connection instance
        """
        self.db_connection = db_connection
    
    async def rollback_insert_user(self, operation: TransactionOperation) -> bool:
        """
        Rollback user insertion operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            user_data = operation.operation_data
            user_id = user_data.get('id') or user_data.get('username')
            
            if not user_id:
                logger.error("Cannot rollback user insert: missing user identifier")
                return False
            
            # Delete the inserted user
            query = "DELETE FROM users WHERE id = :user_id OR username = :username"
            params = {
                'user_id': user_data.get('id'),
                'username': user_data.get('username')
            }
            
            success = await self.db_connection.execute(query, params)
            
            if success:
                logger.info(f"Successfully rolled back user insert for user: {user_id}")
            else:
                logger.error(f"Failed to rollback user insert for user: {user_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during user insert rollback: {str(e)}")
            return False
    
    async def rollback_update_user(self, operation: TransactionOperation) -> bool:
        """
        Rollback user update operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            if not operation.rollback_data:
                logger.error("Cannot rollback user update: missing rollback data")
                return False
            
            user_data = operation.operation_data
            original_data = operation.rollback_data
            user_id = user_data.get('id') or user_data.get('username')
            
            if not user_id:
                logger.error("Cannot rollback user update: missing user identifier")
                return False
            
            # Restore original user data
            set_clauses = []
            params = {}
            
            for field, value in original_data.items():
                if field not in ['id', 'username']:  # Don't update primary keys
                    set_clauses.append(f"{field} = :{field}")
                    params[field] = value
            
            if not set_clauses:
                logger.warning("No fields to restore in user update rollback")
                return True
            
            params['user_id'] = user_data.get('id')
            params['username'] = user_data.get('username')
            
            query = f"UPDATE users SET {', '.join(set_clauses)} WHERE id = :user_id OR username = :username"
            
            success = await self.db_connection.execute(query, params)
            
            if success:
                logger.info(f"Successfully rolled back user update for user: {user_id}")
            else:
                logger.error(f"Failed to rollback user update for user: {user_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during user update rollback: {str(e)}")
            return False
    
    async def rollback_delete_user(self, operation: TransactionOperation) -> bool:
        """
        Rollback user deletion operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            if not operation.rollback_data:
                logger.error("Cannot rollback user delete: missing rollback data")
                return False
            
            original_data = operation.rollback_data
            
            # Re-insert the deleted user
            fields = list(original_data.keys())
            placeholders = [f":{field}" for field in fields]
            
            query = f"INSERT INTO users ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            
            success = await self.db_connection.execute(query, original_data)
            
            if success:
                user_id = original_data.get('id') or original_data.get('username')
                logger.info(f"Successfully rolled back user delete for user: {user_id}")
            else:
                logger.error("Failed to rollback user delete")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during user delete rollback: {str(e)}")
            return False
    
    async def rollback_insert_proxy(self, operation: TransactionOperation) -> bool:
        """
        Rollback proxy insertion operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            proxy_data = operation.operation_data
            proxy_id = proxy_data.get('id')
            
            if not proxy_id:
                logger.error("Cannot rollback proxy insert: missing proxy ID")
                return False
            
            # Delete the inserted proxy
            query = "DELETE FROM proxies WHERE id = :proxy_id"
            params = {'proxy_id': proxy_id}
            
            success = await self.db_connection.execute(query, params)
            
            if success:
                logger.info(f"Successfully rolled back proxy insert for proxy: {proxy_id}")
            else:
                logger.error(f"Failed to rollback proxy insert for proxy: {proxy_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during proxy insert rollback: {str(e)}")
            return False
    
    async def rollback_update_proxy(self, operation: TransactionOperation) -> bool:
        """
        Rollback proxy update operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            if not operation.rollback_data:
                logger.error("Cannot rollback proxy update: missing rollback data")
                return False
            
            proxy_data = operation.operation_data
            original_data = operation.rollback_data
            proxy_id = proxy_data.get('id')
            
            if not proxy_id:
                logger.error("Cannot rollback proxy update: missing proxy ID")
                return False
            
            # Restore original proxy data
            set_clauses = []
            params = {}
            
            for field, value in original_data.items():
                if field != 'id':  # Don't update primary key
                    set_clauses.append(f"{field} = :{field}")
                    params[field] = value
            
            if not set_clauses:
                logger.warning("No fields to restore in proxy update rollback")
                return True
            
            params['proxy_id'] = proxy_id
            
            query = f"UPDATE proxies SET {', '.join(set_clauses)} WHERE id = :proxy_id"
            
            success = await self.db_connection.execute(query, params)
            
            if success:
                logger.info(f"Successfully rolled back proxy update for proxy: {proxy_id}")
            else:
                logger.error(f"Failed to rollback proxy update for proxy: {proxy_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during proxy update rollback: {str(e)}")
            return False
    
    async def rollback_delete_proxy(self, operation: TransactionOperation) -> bool:
        """
        Rollback proxy deletion operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            if not operation.rollback_data:
                logger.error("Cannot rollback proxy delete: missing rollback data")
                return False
            
            original_data = operation.rollback_data
            
            # Re-insert the deleted proxy
            fields = list(original_data.keys())
            placeholders = [f":{field}" for field in fields]
            
            query = f"INSERT INTO proxies ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            
            success = await self.db_connection.execute(query, original_data)
            
            if success:
                proxy_id = original_data.get('id')
                logger.info(f"Successfully rolled back proxy delete for proxy: {proxy_id}")
            else:
                logger.error("Failed to rollback proxy delete")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during proxy delete rollback: {str(e)}")
            return False
    
    async def rollback_insert_admin(self, operation: TransactionOperation) -> bool:
        """
        Rollback admin insertion operation.
        
        Args:
            operation: The operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            admin_data = operation.operation_data
            admin_id = admin_data.get('id') or admin_data.get('username')
            
            if not admin_id:
                logger.error("Cannot rollback admin insert: missing admin identifier")
                return False
            
            # Delete the inserted admin
            query = "DELETE FROM admins WHERE id = :admin_id OR username = :username"
            params = {
                'admin_id': admin_data.get('id'),
                'username': admin_data.get('username')
            }
            
            success = await self.db_connection.execute(query, params)
            
            if success:
                logger.info(f"Successfully rolled back admin insert for admin: {admin_id}")
            else:
                logger.error(f"Failed to rollback admin insert for admin: {admin_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during admin insert rollback: {str(e)}")
            return False
    
    async def rollback_batch_operation(self, operation: TransactionOperation) -> bool:
        """
        Rollback batch operation by reversing individual operations.
        
        Args:
            operation: The batch operation to rollback
            
        Returns:
            True if rollback was successful
        """
        try:
            if not operation.rollback_data or 'operations' not in operation.rollback_data:
                logger.error("Cannot rollback batch operation: missing operation list")
                return False
            
            operations_to_rollback = operation.rollback_data['operations']
            success_count = 0
            
            # Rollback operations in reverse order
            for op_data in reversed(operations_to_rollback):
                try:
                    # Create a temporary operation object for rollback
                    temp_operation = TransactionOperation(
                        operation_id=op_data.get('operation_id', 'temp'),
                        operation_type=op_data['operation_type'],
                        target_table=op_data['target_table'],
                        operation_data=op_data['operation_data'],
                        rollback_data=op_data.get('rollback_data')
                    )
                    
                    # Get the appropriate rollback handler
                    handler_method = getattr(self, f"rollback_{op_data['operation_type']}", None)
                    if handler_method:
                        if await handler_method(temp_operation):
                            success_count += 1
                        else:
                            logger.error(f"Failed to rollback operation in batch: {op_data['operation_type']}")
                    else:
                        logger.warning(f"No rollback handler for operation type: {op_data['operation_type']}")
                
                except Exception as e:
                    logger.error(f"Error rolling back operation in batch: {str(e)}")
            
            total_operations = len(operations_to_rollback)
            success = success_count == total_operations
            
            if success:
                logger.info(f"Successfully rolled back batch operation with {total_operations} operations")
            else:
                logger.error(f"Batch rollback partially failed: {success_count}/{total_operations} operations rolled back")
            
            return success
            
        except Exception as e:
            logger.error(f"Error during batch operation rollback: {str(e)}")
            return False
    
    def get_rollback_handlers(self) -> Dict[str, callable]:
        """
        Get dictionary of all rollback handlers.
        
        Returns:
            Dictionary mapping operation types to handler functions
        """
        return {
            'insert_user': self.rollback_insert_user,
            'update_user': self.rollback_update_user,
            'delete_user': self.rollback_delete_user,
            'insert_proxy': self.rollback_insert_proxy,
            'update_proxy': self.rollback_update_proxy,
            'delete_proxy': self.rollback_delete_proxy,
            'insert_admin': self.rollback_insert_admin,
            'batch_operation': self.rollback_batch_operation,
        }


class FileSystemRollbackHandlers:
    """Rollback handlers for file system operations."""
    
    def __init__(self, backup_dir: str = "./rollback_backups"):
        """
        Initialize file system rollback handlers.
        
        Args:
            backup_dir: Directory to store file backups
        """
        self.backup_dir = backup_dir
        import os
        os.makedirs(backup_dir, exist_ok=True)
    
    async def rollback_file_write(self, operation: TransactionOperation) -> bool:
        """Rollback file write operation."""
        try:
            file_path = operation.operation_data.get('file_path')
            if not file_path:
                return False
            
            if operation.rollback_data and 'backup_path' in operation.rollback_data:
                # Restore from backup
                import shutil
                backup_path = operation.rollback_data['backup_path']
                shutil.copy2(backup_path, file_path)
                logger.info(f"Restored file from backup: {file_path}")
            else:
                # Delete the file if it was newly created
                import os
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.info(f"Removed newly created file: {file_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during file write rollback: {str(e)}")
            return False
    
    async def rollback_file_delete(self, operation: TransactionOperation) -> bool:
        """Rollback file deletion operation."""
        try:
            if not operation.rollback_data or 'backup_path' not in operation.rollback_data:
                return False
            
            file_path = operation.operation_data.get('file_path')
            backup_path = operation.rollback_data['backup_path']
            
            # Restore the deleted file
            import shutil
            shutil.copy2(backup_path, file_path)
            logger.info(f"Restored deleted file: {file_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error during file delete rollback: {str(e)}")
            return False