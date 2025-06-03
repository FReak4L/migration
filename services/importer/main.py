#!/usr/bin/env python3
"""
Importer Microservice
Handles data import to Marzban
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.config import AppConfig
from core.logging_setup import get_logger
from importer.marzban_importer import MarzbanImporter

logger = get_logger()

app = FastAPI(
    title="Importer Service",
    description="Data import service for Marzban",
    version="1.0.0"
)

class ImportRequest(BaseModel):
    """Import request model."""
    data: Dict[str, List[Dict[str, Any]]] = Field(..., description="Validated data to import")
    config: Dict[str, Any] = Field(..., description="Marzban configuration")
    options: Dict[str, Any] = Field(default_factory=dict, description="Import options")

class ImportResult(BaseModel):
    """Import result model."""
    status: str
    statistics: Dict[str, Any]
    imported_at: datetime
    import_time_seconds: float
    rollback_info: Dict[str, Any]

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Importer Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "importer",
        "timestamp": datetime.utcnow().isoformat(),
        "features": [
            "transactional_import",
            "rollback_support",
            "batch_processing",
            "error_recovery"
        ]
    }

@app.post("/import", response_model=ImportResult)
async def import_data(request: ImportRequest):
    """Import data to Marzban."""
    start_time = datetime.utcnow()
    
    try:
        logger.info("Starting data import to Marzban")
        
        # Create importer with configuration
        importer = MarzbanImporter(request.config)
        
        # Perform import
        import_result = await asyncio.to_thread(
            importer.import_all_data,
            request.data,
            request.options
        )
        
        end_time = datetime.utcnow()
        import_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Data import completed: {import_result.statistics}")
        
        return ImportResult(
            status="success",
            statistics=import_result.statistics,
            imported_at=end_time,
            import_time_seconds=import_time,
            rollback_info=import_result.rollback_info
        )
        
    except Exception as e:
        logger.error(f"Data import failed: {e}")
        raise HTTPException(status_code=500, detail=f"Import failed: {str(e)}")

@app.post("/import/test-connection")
async def test_connection(config: Dict[str, Any]):
    """Test connection to Marzban."""
    try:
        importer = MarzbanImporter(config)
        is_connected = await asyncio.to_thread(importer.test_connection)
        
        return {
            "status": "success" if is_connected else "failed",
            "connected": is_connected,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")

@app.post("/import/rollback")
async def rollback_import(rollback_info: Dict[str, Any]):
    """Rollback a previous import."""
    try:
        logger.info("Starting import rollback")
        
        # Create importer and perform rollback
        config = rollback_info.get("config", {})
        importer = MarzbanImporter(config)
        
        rollback_result = await asyncio.to_thread(
            importer.rollback_import,
            rollback_info
        )
        
        return {
            "status": "success",
            "rollback_result": rollback_result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Import rollback failed: {e}")
        raise HTTPException(status_code=500, detail=f"Rollback failed: {str(e)}")

@app.get("/import/status/{import_id}")
async def get_import_status(import_id: str):
    """Get status of a specific import operation."""
    # In production, this would query a database or cache
    return {
        "import_id": import_id,
        "status": "completed",  # This would be dynamic
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)