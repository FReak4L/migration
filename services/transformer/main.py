#!/usr/bin/env python3
"""
Transformer Microservice
Handles data transformation using the enhanced schema compatibility engine
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
from transformer.data_transformer import DataTransformer

logger = get_logger()

app = FastAPI(
    title="Transformer Service",
    description="Data transformation service with schema compatibility",
    version="2.0.0"
)

class TransformationRequest(BaseModel):
    """Transformation request model."""
    data: Dict[str, List[Dict[str, Any]]] = Field(..., description="Raw data from extractor")
    options: Dict[str, Any] = Field(default_factory=dict, description="Transformation options")

class TransformationResult(BaseModel):
    """Transformation result model."""
    status: str
    data: Dict[str, List[Dict[str, Any]]]
    statistics: Dict[str, Any]
    transformed_at: datetime
    transformation_time_seconds: float
    schema_compatibility_report: Dict[str, Any]

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Transformer Service",
        "version": "2.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "transformer",
        "timestamp": datetime.utcnow().isoformat(),
        "features": [
            "schema_compatibility_engine",
            "enhanced_uuid_conversion",
            "datetime_transformation",
            "field_mapping_system"
        ]
    }

@app.post("/transform", response_model=TransformationResult)
async def transform_data(request: TransformationRequest):
    """Transform data for Marzban compatibility."""
    start_time = datetime.utcnow()
    
    try:
        logger.info("Starting data transformation with enhanced schema compatibility")
        
        # Create transformer with default config
        config = AppConfig()
        transformer = DataTransformer(config)
        
        # Perform enhanced transformation
        transformed_data = await asyncio.to_thread(
            transformer.orchestrate_enhanced_transformation,
            request.data
        )
        
        end_time = datetime.utcnow()
        transformation_time = (end_time - start_time).total_seconds()
        
        # Get detailed statistics
        detailed_stats = transformer.get_transformation_statistics()
        
        logger.info(f"Data transformation completed: {detailed_stats['transformation_stats']}")
        
        return TransformationResult(
            status="success",
            data=transformed_data,
            statistics=detailed_stats['transformation_stats'],
            transformed_at=end_time,
            transformation_time_seconds=transformation_time,
            schema_compatibility_report=detailed_stats['schema_conversion_report']
        )
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Transformation failed: {str(e)}")

@app.post("/transform/analyze-schema")
async def analyze_schema_compatibility(data: Dict[str, List[Dict[str, Any]]]):
    """Analyze schema compatibility without performing transformation."""
    try:
        config = AppConfig()
        transformer = DataTransformer(config)
        
        # Analyze schema compatibility
        await asyncio.to_thread(transformer.analyze_schema_compatibility, data)
        
        return {
            "status": "success",
            "message": "Schema compatibility analysis completed",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Schema analysis failed: {e}")
        raise HTTPException(status_code=500, detail=f"Schema analysis failed: {str(e)}")

@app.get("/transform/field-mappings")
async def get_field_mappings():
    """Get available field mappings for Marzneshin to Marzban transformation."""
    try:
        config = AppConfig()
        transformer = DataTransformer(config)
        
        # Get field mappings from schema engine
        user_mappings = transformer.schema_engine.get_marzneshin_to_marzban_user_mappings()
        
        return {
            "status": "success",
            "user_field_mappings": [
                {
                    "source_field": mapping.source_field,
                    "target_field": mapping.target_field,
                    "source_type": mapping.source_type.value,
                    "target_type": mapping.target_type.value
                }
                for mapping in user_mappings
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get field mappings: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get field mappings: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)