#!/usr/bin/env python3
"""
Configuration Microservice
Centralized configuration management for all services
"""

import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.logging_setup import get_logger

logger = get_logger()

app = FastAPI(
    title="Configuration Service",
    description="Centralized configuration management",
    version="1.0.0"
)

class ConfigurationRequest(BaseModel):
    """Configuration request model."""
    service_name: str = Field(..., description="Name of the requesting service")
    environment: str = Field(default="production", description="Environment (dev, staging, production)")

class ConfigurationResponse(BaseModel):
    """Configuration response model."""
    service_name: str
    environment: str
    configuration: Dict[str, Any]
    retrieved_at: datetime
    version: str

# In-memory configuration store (in production, use a proper config management system)
CONFIGURATIONS = {
    "gateway": {
        "production": {
            "service_urls": {
                "extractor": "http://extractor-service:8001",
                "transformer": "http://transformer-service:8002",
                "importer": "http://importer-service:8003",
                "validator": "http://validator-service:8004",
                "config": "http://config-service:8005"
            },
            "timeouts": {
                "service_call_timeout": 30,
                "health_check_timeout": 5
            },
            "cors": {
                "allow_origins": ["*"],
                "allow_credentials": True,
                "allow_methods": ["*"],
                "allow_headers": ["*"]
            }
        },
        "development": {
            "service_urls": {
                "extractor": "http://localhost:8001",
                "transformer": "http://localhost:8002",
                "importer": "http://localhost:8003",
                "validator": "http://localhost:8004",
                "config": "http://localhost:8005"
            },
            "timeouts": {
                "service_call_timeout": 60,
                "health_check_timeout": 10
            },
            "cors": {
                "allow_origins": ["http://localhost:3000", "http://localhost:8080"],
                "allow_credentials": True,
                "allow_methods": ["*"],
                "allow_headers": ["*"]
            }
        }
    },
    "extractor": {
        "production": {
            "connection_pool_size": 10,
            "retry_attempts": 3,
            "retry_delay": 1,
            "timeout": 30,
            "rate_limiting": {
                "requests_per_minute": 60,
                "burst_size": 10
            }
        },
        "development": {
            "connection_pool_size": 5,
            "retry_attempts": 2,
            "retry_delay": 2,
            "timeout": 60,
            "rate_limiting": {
                "requests_per_minute": 30,
                "burst_size": 5
            }
        }
    },
    "transformer": {
        "production": {
            "conversion_strategy": "LENIENT",
            "batch_size": 100,
            "parallel_processing": True,
            "max_workers": 4,
            "schema_validation": True
        },
        "development": {
            "conversion_strategy": "STRICT",
            "batch_size": 50,
            "parallel_processing": False,
            "max_workers": 2,
            "schema_validation": True
        }
    },
    "validator": {
        "production": {
            "validation_level": "strict",
            "fail_fast": False,
            "max_errors": 100,
            "warning_threshold": 10
        },
        "development": {
            "validation_level": "lenient",
            "fail_fast": True,
            "max_errors": 10,
            "warning_threshold": 5
        }
    },
    "importer": {
        "production": {
            "batch_size": 50,
            "transaction_timeout": 300,
            "rollback_enabled": True,
            "backup_before_import": True,
            "parallel_import": False
        },
        "development": {
            "batch_size": 10,
            "transaction_timeout": 600,
            "rollback_enabled": True,
            "backup_before_import": True,
            "parallel_import": False
        }
    }
}

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Configuration Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "config",
        "timestamp": datetime.utcnow().isoformat(),
        "available_services": list(CONFIGURATIONS.keys()),
        "available_environments": ["production", "development"]
    }

@app.get("/config/{service_name}", response_model=ConfigurationResponse)
async def get_configuration(service_name: str, environment: str = "production"):
    """Get configuration for a specific service."""
    if service_name not in CONFIGURATIONS:
        raise HTTPException(status_code=404, detail=f"Configuration for service '{service_name}' not found")
    
    if environment not in CONFIGURATIONS[service_name]:
        raise HTTPException(status_code=404, detail=f"Configuration for environment '{environment}' not found")
    
    config = CONFIGURATIONS[service_name][environment]
    
    # Add environment variables if they exist
    env_config = {}
    for key, value in config.items():
        env_key = f"{service_name.upper()}_{key.upper()}"
        if env_key in os.environ:
            env_config[key] = os.environ[env_key]
        else:
            env_config[key] = value
    
    return ConfigurationResponse(
        service_name=service_name,
        environment=environment,
        configuration=env_config,
        retrieved_at=datetime.utcnow(),
        version="1.0.0"
    )

@app.post("/config/{service_name}")
async def update_configuration(service_name: str, config: Dict[str, Any], environment: str = "production"):
    """Update configuration for a specific service."""
    if service_name not in CONFIGURATIONS:
        CONFIGURATIONS[service_name] = {}
    
    if environment not in CONFIGURATIONS[service_name]:
        CONFIGURATIONS[service_name][environment] = {}
    
    CONFIGURATIONS[service_name][environment].update(config)
    
    return {
        "status": "success",
        "message": f"Configuration updated for {service_name} in {environment}",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/config")
async def list_all_configurations():
    """List all available configurations."""
    return {
        "status": "success",
        "configurations": CONFIGURATIONS,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/config/{service_name}/environments")
async def list_environments(service_name: str):
    """List available environments for a service."""
    if service_name not in CONFIGURATIONS:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found")
    
    return {
        "service_name": service_name,
        "environments": list(CONFIGURATIONS[service_name].keys()),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)