# migration/core/transaction_manager.py

import asyncio
import json
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Set
from contextlib import asynccontextmanager

from core.logging_setup import get_logger

logger = get_logger()

class TransactionStatus(Enum):
    """Transaction status enumeration."""
    ACTIVE = "active"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"

@dataclass
class TransactionOperation:
    """Represents a single operation within a transaction."""
    operation_id: str
    operation_type: str  # insert_user, insert_admin, insert_proxy, etc.
    target_table: str
    operation_data: Dict[str, Any]
    rollback_data: Optional[Dict[str, Any]]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class TransactionCheckpoint:
    """Represents a checkpoint within a transaction."""
    checkpoint_id: str
    transaction_id: str
    operation_count: int
    checkpoint_data: Dict[str, Any]
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class Transaction:
    """Represents a complete transaction with operations and checkpoints."""
    transaction_id: str
    status: TransactionStatus
    operations: List[TransactionOperation] = field(default_factory=list)
    checkpoints: List[TransactionCheckpoint] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    rollback_handlers: Dict[str, Callable] = field(default_factory=dict)

class TransactionManager:
    """
    Advanced transaction manager with checkpoint support and rollback capabilities.
    
    Features:
    - Atomic transactions with automatic rollback
    - Manual and automatic checkpoints
    - Operation tracking and replay
    - Concurrent transaction support
    - Persistent transaction logs
    """
    
    def __init__(self, 
                 checkpoint_interval: int = 100,
                 max_operations_per_transaction: int = 10000,
                 enable_persistence: bool = True,
                 persistence_path: Optional[Path] = None):
        """
        Initialize the transaction manager.
        
        Args:
            checkpoint_interval: Number of operations between automatic checkpoints
            max_operations_per_transaction: Maximum operations allowed per transaction
            enable_persistence: Whether to persist transactions to disk
            persistence_path: Path for transaction persistence files
        """
        self.checkpoint_interval = checkpoint_interval
        self.max_operations_per_transaction = max_operations_per_transaction
        self.enable_persistence = enable_persistence
        self.persistence_path = persistence_path or Path("./transaction_logs")
        
        # Active transactions
        self._active_transactions: Dict[str, Transaction] = {}
        self._transaction_lock = asyncio.Lock()
        
        # Global rollback handlers
        self._rollback_handlers: Dict[str, Callable] = {}
        
        # Ensure persistence directory exists
        if self.enable_persistence:
            self.persistence_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"TransactionManager initialized with checkpoint_interval={checkpoint_interval}")
    
    def register_rollback_handler(self, operation_type: str, handler: Callable):
        """Register a rollback handler for a specific operation type."""
        self._rollback_handlers[operation_type] = handler
        logger.debug(f"Registered rollback handler for operation type: {operation_type}")
    
    @asynccontextmanager
    async def transaction(self, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for atomic transactions.
        
        Args:
            metadata: Optional metadata for the transaction
            
        Yields:
            transaction_id: Unique identifier for the transaction
        """
        transaction_id = str(uuid.uuid4())
        
        async with self._transaction_lock:
            transaction = Transaction(
                transaction_id=transaction_id,
                status=TransactionStatus.ACTIVE,
                metadata=metadata or {},
                rollback_handlers=self._rollback_handlers.copy()
            )
            self._active_transactions[transaction_id] = transaction
        
        logger.info(f"Started transaction {transaction_id}")
        
        try:
            yield transaction_id
            
            # Commit transaction
            await self._commit_transaction(transaction_id)
            logger.info(f"Transaction {transaction_id} committed successfully")
            
        except Exception as e:
            # Rollback transaction
            logger.error(f"Transaction {transaction_id} failed: {str(e)}")
            await self._rollback_transaction(transaction_id, str(e))
            raise
        
        finally:
            # Clean up
            async with self._transaction_lock:
                if transaction_id in self._active_transactions:
                    transaction = self._active_transactions[transaction_id]
                    transaction.end_time = datetime.now(timezone.utc)
                    
                    # Persist transaction if enabled
                    if self.enable_persistence:
                        await self._persist_transaction(transaction)
                    
                    # Remove from active transactions
                    del self._active_transactions[transaction_id]
    
    async def add_operation(self,
                          transaction_id: str,
                          operation_type: str,
                          target_table: str,
                          operation_data: Dict[str, Any],
                          rollback_data: Optional[Dict[str, Any]] = None):
        """
        Add an operation to a transaction.
        
        Args:
            transaction_id: ID of the transaction
            operation_type: Type of operation (insert_user, update_admin, etc.)
            target_table: Target table/collection name
            operation_data: Data for the operation
            rollback_data: Data needed for rollback (optional)
        """
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found or not active")
            
            transaction = self._active_transactions[transaction_id]
            
            # Check operation limit
            if len(transaction.operations) >= self.max_operations_per_transaction:
                raise ValueError(f"Transaction {transaction_id} exceeded maximum operations limit")
            
            # Create operation
            operation = TransactionOperation(
                operation_id=str(uuid.uuid4()),
                operation_type=operation_type,
                target_table=target_table,
                operation_data=operation_data,
                rollback_data=rollback_data
            )
            
            transaction.operations.append(operation)
            
            # Create automatic checkpoint if needed
            if len(transaction.operations) % self.checkpoint_interval == 0:
                await self._create_automatic_checkpoint(transaction_id)
            
            logger.debug(f"Added operation {operation.operation_id} to transaction {transaction_id}")
    
    async def create_manual_checkpoint(self,
                                     transaction_id: str,
                                     checkpoint_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Create a manual checkpoint in a transaction.
        
        Args:
            transaction_id: ID of the transaction
            checkpoint_data: Optional data to store with the checkpoint
            
        Returns:
            checkpoint_id: ID of the created checkpoint
        """
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found or not active")
            
            transaction = self._active_transactions[transaction_id]
            
            checkpoint = TransactionCheckpoint(
                checkpoint_id=str(uuid.uuid4()),
                transaction_id=transaction_id,
                operation_count=len(transaction.operations),
                checkpoint_data=checkpoint_data or {}
            )
            
            transaction.checkpoints.append(checkpoint)
            
            logger.info(f"Created manual checkpoint {checkpoint.checkpoint_id} for transaction {transaction_id}")
            return checkpoint.checkpoint_id
    
    async def _create_automatic_checkpoint(self, transaction_id: str) -> str:
        """Create an automatic checkpoint."""
        checkpoint_data = {
            "type": "automatic",
            "operation_count": len(self._active_transactions[transaction_id].operations)
        }
        
        checkpoint_id = await self.create_manual_checkpoint(transaction_id, checkpoint_data)
        logger.debug(f"Created automatic checkpoint {checkpoint_id} for transaction {transaction_id}")
        return checkpoint_id
    
    async def rollback_to_checkpoint(self, transaction_id: str, checkpoint_id: str) -> bool:
        """
        Rollback a transaction to a specific checkpoint.
        
        Args:
            transaction_id: ID of the transaction
            checkpoint_id: ID of the checkpoint to rollback to
            
        Returns:
            True if rollback was successful
        """
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                logger.error(f"Transaction {transaction_id} not found or not active")
                return False
            
            transaction = self._active_transactions[transaction_id]
            
            # Find the checkpoint
            checkpoint = None
            for cp in transaction.checkpoints:
                if cp.checkpoint_id == checkpoint_id:
                    checkpoint = cp
                    break
            
            if not checkpoint:
                logger.error(f"Checkpoint {checkpoint_id} not found in transaction {transaction_id}")
                return False
            
            # Rollback operations after the checkpoint
            operations_to_rollback = transaction.operations[checkpoint.operation_count:]
            
            for operation in reversed(operations_to_rollback):
                await self._rollback_operation(operation)
            
            # Remove operations after checkpoint
            transaction.operations = transaction.operations[:checkpoint.operation_count]
            
            # Remove checkpoints after this one
            transaction.checkpoints = [cp for cp in transaction.checkpoints 
                                     if cp.timestamp <= checkpoint.timestamp]
            
            logger.info(f"Rolled back transaction {transaction_id} to checkpoint {checkpoint_id}")
            return True
    
    async def _rollback_operation(self, operation: TransactionOperation):
        """Rollback a single operation."""
        handler = self._rollback_handlers.get(operation.operation_type)
        if handler:
            try:
                await handler(operation)
                logger.debug(f"Rolled back operation {operation.operation_id}")
            except Exception as e:
                logger.error(f"Failed to rollback operation {operation.operation_id}: {str(e)}")
        else:
            logger.warning(f"No rollback handler found for operation type: {operation.operation_type}")
    
    async def _commit_transaction(self, transaction_id: str):
        """Commit a transaction."""
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                raise ValueError(f"Transaction {transaction_id} not found")
            
            transaction = self._active_transactions[transaction_id]
            transaction.status = TransactionStatus.COMMITTED
            
            logger.info(f"Committed transaction {transaction_id} with {len(transaction.operations)} operations")
    
    async def _rollback_transaction(self, transaction_id: str, reason: str):
        """Rollback an entire transaction."""
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                logger.error(f"Transaction {transaction_id} not found for rollback")
                return
            
            transaction = self._active_transactions[transaction_id]
            transaction.status = TransactionStatus.ROLLED_BACK
            transaction.metadata["rollback_reason"] = reason
            
            # Rollback all operations in reverse order
            for operation in reversed(transaction.operations):
                await self._rollback_operation(operation)
            
            logger.info(f"Rolled back transaction {transaction_id}: {reason}")
    
    async def force_rollback_transaction(self, transaction_id: str, reason: str = "Manual rollback") -> bool:
        """Force rollback of an active transaction."""
        async with self._transaction_lock:
            if transaction_id not in self._active_transactions:
                logger.error(f"Transaction {transaction_id} not found or not active")
                return False
            
            await self._rollback_transaction(transaction_id, reason)
            return True
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a transaction."""
        if transaction_id in self._active_transactions:
            transaction = self._active_transactions[transaction_id]
            return {
                "transaction_id": transaction.transaction_id,
                "status": transaction.status.value,
                "operation_count": len(transaction.operations),
                "checkpoint_count": len(transaction.checkpoints),
                "start_time": transaction.start_time.isoformat(),
                "metadata": transaction.metadata
            }
        return None
    
    def get_active_transactions(self) -> List[str]:
        """Get list of active transaction IDs."""
        return list(self._active_transactions.keys())
    
    async def _persist_transaction(self, transaction: Transaction):
        """Persist transaction to disk."""
        if not self.enable_persistence:
            return
        
        try:
            transaction_file = self.persistence_path / f"transaction_{transaction.transaction_id}.json"
            
            # Convert transaction to serializable format
            transaction_data = {
                "transaction_id": transaction.transaction_id,
                "status": transaction.status.value,
                "start_time": transaction.start_time.isoformat(),
                "end_time": transaction.end_time.isoformat() if transaction.end_time else None,
                "metadata": transaction.metadata,
                "operations": [
                    {
                        "operation_id": op.operation_id,
                        "operation_type": op.operation_type,
                        "target_table": op.target_table,
                        "operation_data": op.operation_data,
                        "rollback_data": op.rollback_data,
                        "timestamp": op.timestamp.isoformat()
                    }
                    for op in transaction.operations
                ],
                "checkpoints": [
                    {
                        "checkpoint_id": cp.checkpoint_id,
                        "transaction_id": cp.transaction_id,
                        "operation_count": cp.operation_count,
                        "checkpoint_data": cp.checkpoint_data,
                        "timestamp": cp.timestamp.isoformat()
                    }
                    for cp in transaction.checkpoints
                ]
            }
            
            with open(transaction_file, 'w') as f:
                json.dump(transaction_data, f, indent=2, default=str)
            
            logger.debug(f"Persisted transaction {transaction.transaction_id} to {transaction_file}")
            
        except Exception as e:
            logger.error(f"Failed to persist transaction {transaction.transaction_id}: {str(e)}")