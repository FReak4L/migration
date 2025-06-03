#!/usr/bin/env python3
"""
Validator Microservice
Handles data validation for transformed data
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
from validator.data_validator import DataValidator

logger = get_logger()

app = FastAPI(
    title="Validator Service",
    description="Data validation service for migration",
    version="1.0.0"
)

class ValidationRequest(BaseModel):
    """Validation request model."""
    data: Dict[str, List[Dict[str, Any]]] = Field(..., description="Transformed data to validate")
    options: Dict[str, Any] = Field(default_factory=dict, description="Validation options")

class ValidationResult(BaseModel):
    """Validation result model."""
    status: str
    is_valid: bool
    validation_report: Dict[str, Any]
    errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    validated_at: datetime
    validation_time_seconds: float

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Validator Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "validator",
        "timestamp": datetime.utcnow().isoformat(),
        "validation_rules": [
            "user_data_integrity",
            "admin_data_integrity",
            "proxy_configuration_validity",
            "field_type_validation",
            "required_field_validation"
        ]
    }

@app.post("/validate", response_model=ValidationResult)
async def validate_data(request: ValidationRequest):
    """Validate transformed data."""
    start_time = datetime.utcnow()
    
    try:
        logger.info("Starting data validation")
        
        # Create validator with configuration
        config = AppConfig()
        validator = DataValidator(config)
        
        # Perform validation
        validation_result = await asyncio.to_thread(
            validator.validate_all_data,
            request.data
        )
        
        end_time = datetime.utcnow()
        validation_time = (end_time - start_time).total_seconds()
        
        logger.info(f"Data validation completed: {validation_result.is_valid}")
        
        return ValidationResult(
            status="success",
            is_valid=validation_result.is_valid,
            validation_report=validation_result.report,
            errors=validation_result.errors,
            warnings=validation_result.warnings,
            validated_at=end_time,
            validation_time_seconds=validation_time
        )
        
    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

@app.post("/validate/users")
async def validate_users_only(users: List[Dict[str, Any]]):
    """Validate only user data."""
    try:
        config = AppConfig()
        validator = DataValidator(config)
        
        validation_result = await asyncio.to_thread(
            validator.validate_users,
            users
        )
        
        return {
            "status": "success",
            "is_valid": validation_result.is_valid,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"User validation failed: {e}")
        raise HTTPException(status_code=500, detail=f"User validation failed: {str(e)}")

@app.post("/validate/admins")
async def validate_admins_only(admins: List[Dict[str, Any]]):
    """Validate only admin data."""
    try:
        config = AppConfig()
        validator = DataValidator(config)
        
        validation_result = await asyncio.to_thread(
            validator.validate_admins,
            admins
        )
        
        return {
            "status": "success",
            "is_valid": validation_result.is_valid,
            "errors": validation_result.errors,
            "warnings": validation_result.warnings,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Admin validation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Admin validation failed: {str(e)}")

@app.get("/validate/rules")
async def get_validation_rules():
    """Get available validation rules."""
    return {
        "status": "success",
        "validation_rules": {
            "user_validation": [
                "username_required",
                "username_unique",
                "expire_date_format",
                "data_limit_positive",
                "status_valid_enum"
            ],
            "admin_validation": [
                "username_required",
                "username_unique",
                "password_hash_required",
                "is_sudo_boolean"
            ],
            "proxy_validation": [
                "type_valid_enum",
                "settings_valid_json",
                "uuid_format_valid"
            ]
        },
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)