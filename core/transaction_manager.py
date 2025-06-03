# migration/core/transaction_manager.py

import asyncio
import json
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, AsyncGenerator
from pathlib import Path

from core.logging_setup import get_logger

logger = get_logger()


class TransactionState(Enum):
    """Transaction state enumeration."""
    PENDING = "pending"
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class CheckpointType(Enum):
    """Checkpoint type enumeration."""
    AUTOMATIC = "automatic"
    MANUAL = "manual"
    ERROR_RECOVERY = "error_recovery"


@dataclass
class TransactionCheckpoint:
    """Represents a transaction checkpoint for rollback purposes."""
    checkpoint_id: str
    transaction_id: str
    timestamp: datetime
    checkpoint_type: CheckpointType
    data_snapshot: Dict[str, Any]
    operation_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransactionOperation:
    """Represents a single operation within a transaction."""
    operation_id: str
    operation_type: str
    target_table: str
    operation_data: Dict[str, Any]
    rollback_data: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    is_reversible: bool = True


@dataclass
class TransactionLog:
    """Complete transaction log with all operations and checkpoints."""
    transaction_id: str
    start_time: datetime
    end_time: Optional[datetime]
    state: TransactionState
    operations: List[TransactionOperation] = field(default_factory=list)
    checkpoints: List[TransactionCheckpoint] = field(default_factory=list)
    error_message: Optional[str] = None
    rollback_reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class TransactionManager:
    """
    Advanced transaction manager with atomic operations, checkpoints, and rollback capabilities.
    """
    
    def __init__(self, 
                 checkpoint_interval: int = 100,
                 max_operations_per_transaction: int = 10000,
                 enable_persistence: bool = True,
                 transaction_log_dir: Optional[Path] = None):
        """
        Initialize transaction manager.
        
        Args:
            checkpoint_interval: Number of operations between automatic checkpoints
            max_operations_per_transaction: Maximum operations allowed per transaction
            enable_persistence: Whether to persist transaction logs to disk
            transaction_log_dir: Directory for transaction log files
        """
        self.checkpoint_interval = checkpoint_interval
        self.max_operations_per_transaction = max_operations_per_transaction
        self.enable_persistence = enable_persistence
        self.transaction_log_dir = transaction_log_dir or Path("./transaction_logs")
        
        # Active transactions
        self._active_transactions: Dict[str, TransactionLog] = {}
        self._transaction_locks: Dict[str, asyncio.Lock] = {}
        
        # Global lock for transaction management
        self._global_lock = asyncio.Lock()
        
        # Rollback handlers
        self._rollback_handlers: Dict[str, Callable] = {}
        
        # Ensure log directory exists
        if self.enable_persistence:
            self.transaction_log_dir.mkdir(parents=True, exist_ok=True)
    
    def register_rollback_handler(self, operation_type: str, handler: Callable):
        """
        Register a rollback handler for a specific operation type.
        
        Args:
            operation_type: Type of operation (e.g., 'insert_user', 'update_proxy')
            handler: Async function to handle rollback for this operation type
        """
        self._rollback_handlers[operation_type] = handler
        logger.debug(f"Registered rollback handler for operation type: {operation_type}")
    
    @asynccontextmanager
    async def transaction(self, 
                         transaction_id: Optional[str] = None,
                         metadata: Optional[Dict[str, Any]] = None) -> AsyncGenerator[str, None]:
        """
        Async context manager for atomic transactions.
        
        Args:
            transaction_id: Optional custom transaction ID
            metadata: Optional metadata for the transaction
            
        Yields:
            Transaction ID for use in operations
        """
        if transaction_id is None:
            transaction_id = f"txn_{uuid.uuid4().hex[:12]}"
        
        async with self._global_lock:
            if transaction_id in self._active_transactions:
                raise ValueError(f"Transaction {transaction_id} is already active")
            
            # Initialize transaction
            transaction_log = TransactionLog(
                transaction_id=transaction_id,
                start_time=datetime.now(timezone.utc),
                end_time=None,
                state=TransactionState.PENDING,
                metadata=metadata or {}
            )
            
            self._active_transactions[transaction_id] = transaction_log
            self._transaction_locks[transaction_id] = asyncio.Lock()
        
        logger.info(f"Starting transaction: {transaction_id}")
        
        try:
            # Mark transaction as active
            await self._update_transaction_state(transaction_id, TransactionState.ACTIVE)
            
            yield transaction_id
            
            # Commit transaction
            await self._commit_transaction(transaction_id)
            
        except Exception as e:
            logger.error(f"Transaction {transaction_id} failed: {str(e)}")
            await self._rollback_transaction(transaction_id, str(e))
            raise
        
        finally:
            # Cleanup
            async with self._global_lock:
                if transaction_id in self._active_transactions:
                    # Persist transaction log if enabled
                    if self.enable_persistence:
                        await self._persist_transaction_log(transaction_id)
                    
                    del self._active_transactions[transaction_id]
                    del self._transaction_locks[transaction_id]
    
    async def add_operation(self,
                           transaction_id: str,
                           operation_type: str,
                           target_table: str,
                           operation_data: Dict[str, Any],
                           rollback_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Add an operation to a transaction.
        
        Args:
            transaction_id: ID of the transaction
            operation_type: Type of operation (insert, update, delete)
            target_table: Target table/collection name
            operation_data: Data for the operation
            rollback_data: Data needed for rollback (optional)
            
        Returns:
            Operation ID
        """
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found or not active")
        
        async with self._transaction_locks[transaction_id]:
            transaction_log = self._active_transactions[transaction_id]
            
            # Check operation limit
            if len(transaction_log.operations) >= self.max_operations_per_transaction:
                raise ValueError(f"Transaction {transaction_id} has reached maximum operations limit")
            
            # Create operation
            operation = TransactionOperation(
                operation_id=f"op_{uuid.uuid4().hex[:8]}",
                operation_type=operation_type,
                target_table=target_table,
                operation_data=operation_data,
                rollback_data=rollback_data
            )
            
            transaction_log.operations.append(operation)
            
            # Create automatic checkpoint if needed
            if len(transaction_log.operations) % self.checkpoint_interval == 0:
                await self._create_checkpoint(transaction_id, CheckpointType.AUTOMATIC)
            
            logger.debug(f"Added operation {operation.operation_id} to transaction {transaction_id}")
            return operation.operation_id
    
    async def create_manual_checkpoint(self, 
                                     transaction_id: str,
                                     metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Create a manual checkpoint in a transaction.
        
        Args:
            transaction_id: ID of the transaction
            metadata: Optional metadata for the checkpoint
            
        Returns:
            Checkpoint ID
        """
        return await self._create_checkpoint(transaction_id, CheckpointType.MANUAL, metadata)
    
    async def rollback_to_checkpoint(self, 
                                   transaction_id: str,
                                   checkpoint_id: str) -> bool:
        """
        Rollback transaction to a specific checkpoint.
        
        Args:
            transaction_id: ID of the transaction
            checkpoint_id: ID of the checkpoint to rollback to
            
        Returns:
            True if rollback was successful
        """
        if transaction_id not in self._active_transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        async with self._transaction_locks[transaction_id]:
            transaction_log = self._active_transactions[transaction_id]
            
            # Find the checkpoint
            checkpoint = None
            checkpoint_index = -1
            for i, cp in enumerate(transaction_log.checkpoints):
                if cp.checkpoint_id == checkpoint_id:
                    checkpoint = cp
                    checkpoint_index = i
                    break
            
            if checkpoint is None:
                logger.error(f"Checkpoint {checkpoint_id} not found in transaction {transaction_id}")
                return False
            
            # Rollback operations after the checkpoint
            operations_to_rollback = transaction_log.operations[checkpoint.operation_count:]
            
            success = True
            for operation in reversed(operations_to_rollback):
                if not await self._rollback_operation(operation):
                    success = False
                    break
            
            if success:
                # Remove operations and checkpoints after the target checkpoint
                transaction_log.operations = transaction_log.operations[:checkpoint.operation_count]
                transaction_log.checkpoints = transaction_log.checkpoints[:checkpoint_index + 1]
                
                logger.info(f"Successfully rolled back transaction {transaction_id} to checkpoint {checkpoint_id}")
            else:
                logger.error(f"Failed to rollback transaction {transaction_id} to checkpoint {checkpoint_id}")
            
            return success
    
    async def _create_checkpoint(self,
                               transaction_id: str,
                               checkpoint_type: CheckpointType,
                               metadata: Optional[Dict[str, Any]] = None) -> str:
        """Create a checkpoint for the transaction."""
        transaction_log = self._active_transactions[transaction_id]
        
        checkpoint = TransactionCheckpoint(
            checkpoint_id=f"cp_{uuid.uuid4().hex[:8]}",
            transaction_id=transaction_id,
            timestamp=datetime.now(timezone.utc),
            checkpoint_type=checkpoint_type,
            data_snapshot=self._create_data_snapshot(transaction_log),
            operation_count=len(transaction_log.operations),
            metadata=metadata or {}
        )
        
        transaction_log.checkpoints.append(checkpoint)
        
        logger.debug(f"Created {checkpoint_type.value} checkpoint {checkpoint.checkpoint_id} "
                    f"for transaction {transaction_id} at operation {checkpoint.operation_count}")
        
        return checkpoint.checkpoint_id
    
    def _create_data_snapshot(self, transaction_log: TransactionLog) -> Dict[str, Any]:
        """Create a snapshot of current transaction data."""
        return {
            "operation_count": len(transaction_log.operations),
            "last_operation_id": transaction_log.operations[-1].operation_id if transaction_log.operations else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "state": transaction_log.state.value
        }
    
    async def _update_transaction_state(self, transaction_id: str, new_state: TransactionState):
        """Update the state of a transaction."""
        if transaction_id in self._active_transactions:
            self._active_transactions[transaction_id].state = new_state
            logger.debug(f"Transaction {transaction_id} state updated to {new_state.value}")
    
    async def _commit_transaction(self, transaction_id: str):
        """Commit a transaction."""
        await self._update_transaction_state(transaction_id, TransactionState.COMMITTED)
        self._active_transactions[transaction_id].end_time = datetime.now(timezone.utc)
        
        logger.info(f"Transaction {transaction_id} committed successfully with "
                   f"{len(self._active_transactions[transaction_id].operations)} operations")
    
    async def _rollback_transaction(self, transaction_id: str, error_message: str):
        """Rollback an entire transaction."""
        if transaction_id not in self._active_transactions:
            return
        
        async with self._transaction_locks[transaction_id]:
            transaction_log = self._active_transactions[transaction_id]
            transaction_log.error_message = error_message
            transaction_log.rollback_reason = "Transaction failure"
            
            # Create error recovery checkpoint
            await self._create_checkpoint(transaction_id, CheckpointType.ERROR_RECOVERY)
            
            # Rollback all operations in reverse order
            rollback_success = True
            for operation in reversed(transaction_log.operations):
                if not await self._rollback_operation(operation):
                    rollback_success = False
            
            # Update transaction state
            if rollback_success:
                await self._update_transaction_state(transaction_id, TransactionState.ROLLED_BACK)
                logger.info(f"Transaction {transaction_id} rolled back successfully")
            else:
                await self._update_transaction_state(transaction_id, TransactionState.FAILED)
                logger.error(f"Transaction {transaction_id} rollback failed")
            
            transaction_log.end_time = datetime.now(timezone.utc)
    
    async def _rollback_operation(self, operation: TransactionOperation) -> bool:
        """Rollback a single operation."""
        if not operation.is_reversible:
            logger.warning(f"Operation {operation.operation_id} is not reversible")
            return True  # Consider non-reversible operations as successful rollback
        
        handler = self._rollback_handlers.get(operation.operation_type)
        if handler is None:
            logger.warning(f"No rollback handler found for operation type: {operation.operation_type}")
            return False
        
        try:
            await handler(operation)
            logger.debug(f"Successfully rolled back operation {operation.operation_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to rollback operation {operation.operation_id}: {str(e)}")
            return False
    
    async def _persist_transaction_log(self, transaction_id: str):
        """Persist transaction log to disk."""
        if not self.enable_persistence:
            return
        
        transaction_log = self._active_transactions[transaction_id]
        log_file = self.transaction_log_dir / f"{transaction_id}.json"
        
        try:
            log_data = {
                "transaction_id": transaction_log.transaction_id,
                "start_time": transaction_log.start_time.isoformat(),
                "end_time": transaction_log.end_time.isoformat() if transaction_log.end_time else None,
                "state": transaction_log.state.value,
                "operations": [
                    {
                        "operation_id": op.operation_id,
                        "operation_type": op.operation_type,
                        "target_table": op.target_table,
                        "timestamp": op.timestamp.isoformat(),
                        "is_reversible": op.is_reversible,
                        "operation_data": op.operation_data,
                        "rollback_data": op.rollback_data
                    }
                    for op in transaction_log.operations
                ],
                "checkpoints": [
                    {
                        "checkpoint_id": cp.checkpoint_id,
                        "timestamp": cp.timestamp.isoformat(),
                        "checkpoint_type": cp.checkpoint_type.value,
                        "operation_count": cp.operation_count,
                        "data_snapshot": cp.data_snapshot,
                        "metadata": cp.metadata
                    }
                    for cp in transaction_log.checkpoints
                ],
                "error_message": transaction_log.error_message,
                "rollback_reason": transaction_log.rollback_reason,
                "metadata": transaction_log.metadata
            }
            
            with open(log_file, 'w') as f:
                json.dump(log_data, f, indent=2)
            
            logger.debug(f"Persisted transaction log for {transaction_id}")
            
        except Exception as e:
            logger.error(f"Failed to persist transaction log for {transaction_id}: {str(e)}")
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Get the current status of a transaction."""
        if transaction_id not in self._active_transactions:
            return None
        
        transaction_log = self._active_transactions[transaction_id]
        return {
            "transaction_id": transaction_id,
            "state": transaction_log.state.value,
            "start_time": transaction_log.start_time.isoformat(),
            "operation_count": len(transaction_log.operations),
            "checkpoint_count": len(transaction_log.checkpoints),
            "last_checkpoint": transaction_log.checkpoints[-1].checkpoint_id if transaction_log.checkpoints else None,
            "metadata": transaction_log.metadata
        }
    
    def get_active_transactions(self) -> List[str]:
        """Get list of active transaction IDs."""
        return list(self._active_transactions.keys())
    
    async def force_rollback_transaction(self, transaction_id: str, reason: str = "Manual rollback") -> bool:
        """Force rollback of an active transaction."""
        if transaction_id not in self._active_transactions:
            logger.warning(f"Transaction {transaction_id} not found for forced rollback")
            return False
        
        logger.warning(f"Forcing rollback of transaction {transaction_id}: {reason}")
        await self._rollback_transaction(transaction_id, f"Forced rollback: {reason}")
        return True
    
    async def cleanup_old_logs(self, days_old: int = 30):
        """Clean up old transaction log files."""
        if not self.enable_persistence:
            return
        
        cutoff_time = time.time() - (days_old * 24 * 60 * 60)
        cleaned_count = 0
        
        for log_file in self.transaction_log_dir.glob("*.json"):
            if log_file.stat().st_mtime < cutoff_time:
                try:
                    log_file.unlink()
                    cleaned_count += 1
                except Exception as e:
                    logger.error(f"Failed to delete old log file {log_file}: {str(e)}")
        
        logger.info(f"Cleaned up {cleaned_count} old transaction log files")
        return cleaned_count