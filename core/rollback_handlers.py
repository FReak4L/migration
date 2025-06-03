# migration/core/rollback_handlers.py

import asyncio
from typing import Any, Dict, Callable, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

from core.logging_setup import get_logger

logger = get_logger()

@dataclass
class DatabaseConnection:
    """Mock database connection for demonstration."""
    connection_string: str
    
    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Mock execute method."""
        logger.debug(f"Mock DB Execute: {query} with params: {params}")
        return {"affected_rows": 1}
    
    async def rollback(self):
        """Mock rollback method."""
        logger.debug("Mock DB Rollback executed")

class RollbackHandler(ABC):
    """Abstract base class for rollback handlers."""
    
    @abstractmethod
    async def handle_rollback(self, operation):
        """Handle rollback for a specific operation."""
        pass

class DatabaseRollbackHandlers:
    """
    Comprehensive rollback handlers for database operations.
    
    Provides rollback functionality for:
    - User operations (insert, update, delete)
    - Admin operations (insert, update, delete)
    - Proxy operations (insert, update, delete)
    """
    
    def __init__(self, db_connection: DatabaseConnection):
        """
        Initialize rollback handlers with database connection.
        
        Args:
            db_connection: Database connection instance
        """
        self.db_connection = db_connection
        
        # Register all rollback handlers
        self._handlers = {
            "insert_user": self._rollback_insert_user,
            "update_user": self._rollback_update_user,
            "delete_user": self._rollback_delete_user,
            "insert_admin": self._rollback_insert_admin,
            "update_admin": self._rollback_update_admin,
            "delete_admin": self._rollback_delete_admin,
            "insert_proxy": self._rollback_insert_proxy,
            "update_proxy": self._rollback_update_proxy,
            "delete_proxy": self._rollback_delete_proxy,
        }
        
        logger.info(f"DatabaseRollbackHandlers initialized with {len(self._handlers)} handlers")
    
    def get_rollback_handlers(self) -> Dict[str, Callable]:
        """Get all registered rollback handlers."""
        return self._handlers.copy()
    
    async def _rollback_insert_user(self, operation):
        """Rollback user insertion by deleting the user."""
        try:
            user_data = operation.operation_data
            username = user_data.get("username")
            
            if not username:
                logger.error(f"Cannot rollback user insert: missing username in operation {operation.operation_id}")
                return
            
            # Delete the inserted user
            query = "DELETE FROM users WHERE username = :username"
            params = {"username": username}
            
            result = await self.db_connection.execute(query, params)
            logger.info(f"Rolled back user insert for username: {username}")
            
        except Exception as e:
            logger.error(f"Failed to rollback user insert operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_update_user(self, operation):
        """Rollback user update by restoring original data."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback user update: missing rollback data in operation {operation.operation_id}")
                return
            
            username = rollback_data.get("username")
            if not username:
                logger.error(f"Cannot rollback user update: missing username in rollback data")
                return
            
            # Restore original user data
            set_clauses = []
            params = {"username": username}
            
            for field, value in rollback_data.items():
                if field != "username":
                    set_clauses.append(f"{field} = :{field}")
                    params[field] = value
            
            if set_clauses:
                query = f"UPDATE users SET {', '.join(set_clauses)} WHERE username = :username"
                result = await self.db_connection.execute(query, params)
                logger.info(f"Rolled back user update for username: {username}")
            
        except Exception as e:
            logger.error(f"Failed to rollback user update operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_delete_user(self, operation):
        """Rollback user deletion by re-inserting the user."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback user delete: missing rollback data in operation {operation.operation_id}")
                return
            
            # Re-insert the deleted user
            fields = list(rollback_data.keys())
            placeholders = [f":{field}" for field in fields]
            
            query = f"INSERT INTO users ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            
            result = await self.db_connection.execute(query, rollback_data)
            logger.info(f"Rolled back user delete for username: {rollback_data.get('username')}")
            
        except Exception as e:
            logger.error(f"Failed to rollback user delete operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_insert_admin(self, operation):
        """Rollback admin insertion by deleting the admin."""
        try:
            admin_data = operation.operation_data
            username = admin_data.get("username")
            
            if not username:
                logger.error(f"Cannot rollback admin insert: missing username in operation {operation.operation_id}")
                return
            
            # Delete the inserted admin
            query = "DELETE FROM admins WHERE username = :username"
            params = {"username": username}
            
            result = await self.db_connection.execute(query, params)
            logger.info(f"Rolled back admin insert for username: {username}")
            
        except Exception as e:
            logger.error(f"Failed to rollback admin insert operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_update_admin(self, operation):
        """Rollback admin update by restoring original data."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback admin update: missing rollback data in operation {operation.operation_id}")
                return
            
            username = rollback_data.get("username")
            if not username:
                logger.error(f"Cannot rollback admin update: missing username in rollback data")
                return
            
            # Restore original admin data
            set_clauses = []
            params = {"username": username}
            
            for field, value in rollback_data.items():
                if field != "username":
                    set_clauses.append(f"{field} = :{field}")
                    params[field] = value
            
            if set_clauses:
                query = f"UPDATE admins SET {', '.join(set_clauses)} WHERE username = :username"
                result = await self.db_connection.execute(query, params)
                logger.info(f"Rolled back admin update for username: {username}")
            
        except Exception as e:
            logger.error(f"Failed to rollback admin update operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_delete_admin(self, operation):
        """Rollback admin deletion by re-inserting the admin."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback admin delete: missing rollback data in operation {operation.operation_id}")
                return
            
            # Re-insert the deleted admin
            fields = list(rollback_data.keys())
            placeholders = [f":{field}" for field in fields]
            
            query = f"INSERT INTO admins ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            
            result = await self.db_connection.execute(query, rollback_data)
            logger.info(f"Rolled back admin delete for username: {rollback_data.get('username')}")
            
        except Exception as e:
            logger.error(f"Failed to rollback admin delete operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_insert_proxy(self, operation):
        """Rollback proxy insertion by deleting the proxy."""
        try:
            proxy_data = operation.operation_data
            proxy_tag = proxy_data.get("tag")
            
            if not proxy_tag:
                logger.error(f"Cannot rollback proxy insert: missing tag in operation {operation.operation_id}")
                return
            
            # Delete the inserted proxy
            query = "DELETE FROM proxies WHERE tag = :tag"
            params = {"tag": proxy_tag}
            
            result = await self.db_connection.execute(query, params)
            logger.info(f"Rolled back proxy insert for tag: {proxy_tag}")
            
        except Exception as e:
            logger.error(f"Failed to rollback proxy insert operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_update_proxy(self, operation):
        """Rollback proxy update by restoring original data."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback proxy update: missing rollback data in operation {operation.operation_id}")
                return
            
            proxy_tag = rollback_data.get("tag")
            if not proxy_tag:
                logger.error(f"Cannot rollback proxy update: missing tag in rollback data")
                return
            
            # Restore original proxy data
            set_clauses = []
            params = {"tag": proxy_tag}
            
            for field, value in rollback_data.items():
                if field != "tag":
                    set_clauses.append(f"{field} = :{field}")
                    params[field] = value
            
            if set_clauses:
                query = f"UPDATE proxies SET {', '.join(set_clauses)} WHERE tag = :tag"
                result = await self.db_connection.execute(query, params)
                logger.info(f"Rolled back proxy update for tag: {proxy_tag}")
            
        except Exception as e:
            logger.error(f"Failed to rollback proxy update operation {operation.operation_id}: {str(e)}")
    
    async def _rollback_delete_proxy(self, operation):
        """Rollback proxy deletion by re-inserting the proxy."""
        try:
            rollback_data = operation.rollback_data
            if not rollback_data:
                logger.error(f"Cannot rollback proxy delete: missing rollback data in operation {operation.operation_id}")
                return
            
            # Re-insert the deleted proxy
            fields = list(rollback_data.keys())
            placeholders = [f":{field}" for field in fields]
            
            query = f"INSERT INTO proxies ({', '.join(fields)}) VALUES ({', '.join(placeholders)})"
            
            result = await self.db_connection.execute(query, rollback_data)
            logger.info(f"Rolled back proxy delete for tag: {rollback_data.get('tag')}")
            
        except Exception as e:
            logger.error(f"Failed to rollback proxy delete operation {operation.operation_id}: {str(e)}")

class FileSystemRollbackHandlers:
    """
    Rollback handlers for file system operations.
    
    Provides rollback functionality for:
    - File creation/deletion
    - Directory operations
    - Backup/restore operations
    """
    
    def __init__(self, backup_directory: str = "./rollback_backups"):
        """
        Initialize file system rollback handlers.
        
        Args:
            backup_directory: Directory to store backup files for rollback
        """
        self.backup_directory = backup_directory
        
        # Register file system rollback handlers
        self._handlers = {
            "create_file": self._rollback_create_file,
            "delete_file": self._rollback_delete_file,
            "update_file": self._rollback_update_file,
            "create_directory": self._rollback_create_directory,
            "delete_directory": self._rollback_delete_directory,
        }
        
        logger.info(f"FileSystemRollbackHandlers initialized with backup directory: {backup_directory}")
    
    def get_rollback_handlers(self) -> Dict[str, Callable]:
        """Get all registered file system rollback handlers."""
        return self._handlers.copy()
    
    async def _rollback_create_file(self, operation):
        """Rollback file creation by deleting the file."""
        try:
            file_path = operation.operation_data.get("file_path")
            if file_path and Path(file_path).exists():
                Path(file_path).unlink()
                logger.info(f"Rolled back file creation: {file_path}")
        except Exception as e:
            logger.error(f"Failed to rollback file creation: {str(e)}")
    
    async def _rollback_delete_file(self, operation):
        """Rollback file deletion by restoring from backup."""
        try:
            rollback_data = operation.rollback_data
            if rollback_data and "backup_path" in rollback_data:
                original_path = rollback_data["original_path"]
                backup_path = rollback_data["backup_path"]
                
                if Path(backup_path).exists():
                    import shutil
                    shutil.copy2(backup_path, original_path)
                    logger.info(f"Rolled back file deletion: {original_path}")
        except Exception as e:
            logger.error(f"Failed to rollback file deletion: {str(e)}")
    
    async def _rollback_update_file(self, operation):
        """Rollback file update by restoring original content."""
        try:
            rollback_data = operation.rollback_data
            if rollback_data and "original_content" in rollback_data:
                file_path = rollback_data["file_path"]
                original_content = rollback_data["original_content"]
                
                with open(file_path, 'w') as f:
                    f.write(original_content)
                logger.info(f"Rolled back file update: {file_path}")
        except Exception as e:
            logger.error(f"Failed to rollback file update: {str(e)}")
    
    async def _rollback_create_directory(self, operation):
        """Rollback directory creation by removing the directory."""
        try:
            dir_path = operation.operation_data.get("directory_path")
            if dir_path and Path(dir_path).exists():
                import shutil
                shutil.rmtree(dir_path)
                logger.info(f"Rolled back directory creation: {dir_path}")
        except Exception as e:
            logger.error(f"Failed to rollback directory creation: {str(e)}")
    
    async def _rollback_delete_directory(self, operation):
        """Rollback directory deletion by restoring from backup."""
        try:
            rollback_data = operation.rollback_data
            if rollback_data and "backup_path" in rollback_data:
                original_path = rollback_data["original_path"]
                backup_path = rollback_data["backup_path"]
                
                if Path(backup_path).exists():
                    import shutil
                    shutil.copytree(backup_path, original_path)
                    logger.info(f"Rolled back directory deletion: {original_path}")
        except Exception as e:
            logger.error(f"Failed to rollback directory deletion: {str(e)}")