#!/usr/bin/env python3
"""
Extractor Microservice
Handles data extraction from Marzneshin
"""

import asyncio
from datetime import datetime
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.logging_setup import get_logger
from extractor.marzneshin_extractor import MarzneshinExtractor

logger = get_logger()

app = FastAPI(
    title="Extractor Service",
    description="Data extraction service for Marzneshin",
    version="1.0.0"
)

class ExtractionRequest(BaseModel):
    """Extraction request model."""
    config: Dict[str, Any] = Field(..., description="Marzneshin configuration")
    options: Dict[str, Any] = Field(default_factory=dict, description="Extraction options")

class ExtractionResult(BaseModel):
    """Extraction result model."""
    status: str
    data: Dict[str, List[Dict[str, Any]]]
    statistics: Dict[str, Any]
    extracted_at: datetime
    extraction_time_seconds: float

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Extractor Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "extractor",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/extract", response_model=ExtractionResult)
async def extract_data(request: ExtractionRequest):
    """Extract data from Marzneshin."""
    start_time = datetime.utcnow()
    
    try:
        logger.info("Starting data extraction from Marzneshin")
        
        # Create extractor with configuration
        extractor = MarzneshinExtractor(request.config)
        
        # Extract data
        extracted_data = await asyncio.to_thread(extractor.extract_all_data)
        
        end_time = datetime.utcnow()
        extraction_time = (end_time - start_time).total_seconds()
        
        # Compile statistics
        statistics = {
            "users_extracted": len(extracted_data.get("users", [])),
            "admins_extracted": len(extracted_data.get("admins", [])),
            "extraction_time_seconds": extraction_time,
            "source_api_calls": getattr(extractor, 'api_call_count', 0),
            "errors_encountered": getattr(extractor, 'error_count', 0)
        }
        
        logger.info(f"Data extraction completed: {statistics}")
        
        return ExtractionResult(
            status="success",
            data=extracted_data,
            statistics=statistics,
            extracted_at=end_time,
            extraction_time_seconds=extraction_time
        )
        
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")

@app.get("/extract/test-connection")
async def test_connection(config: Dict[str, Any]):
    """Test connection to Marzneshin."""
    try:
        extractor = MarzneshinExtractor(config)
        is_connected = await asyncio.to_thread(extractor.test_connection)
        
        return {
            "status": "success" if is_connected else "failed",
            "connected": is_connected,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Connection test failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)