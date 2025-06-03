#!/usr/bin/env python3
"""
API Gateway for Migration Microservices
Handles routing, authentication, and service orchestration
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from core.config import AppConfig
from core.logging_setup import get_logger

logger = get_logger()

# Service URLs - should be configurable via environment
SERVICE_URLS = {
    "extractor": "http://extractor-service:8001",
    "transformer": "http://transformer-service:8002", 
    "importer": "http://importer-service:8003",
    "validator": "http://validator-service:8004",
    "config": "http://config-service:8005"
}

class MigrationRequest(BaseModel):
    """Migration request model."""
    source_config: Dict[str, Any] = Field(..., description="Marzneshin configuration")
    target_config: Dict[str, Any] = Field(..., description="Marzban configuration")
    migration_options: Dict[str, Any] = Field(default_factory=dict, description="Migration options")
    dry_run: bool = Field(default=False, description="Perform dry run without actual import")

class MigrationStatus(BaseModel):
    """Migration status model."""
    migration_id: str
    status: str  # pending, running, completed, failed
    progress: float = Field(ge=0, le=100)
    current_step: str
    steps_completed: int
    total_steps: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    statistics: Dict[str, Any] = Field(default_factory=dict)

class ServiceHealth(BaseModel):
    """Service health model."""
    service: str
    status: str  # healthy, unhealthy, unknown
    response_time_ms: float
    last_check: datetime
    details: Dict[str, Any] = Field(default_factory=dict)

# In-memory storage for migration status (in production, use Redis or database)
migration_statuses: Dict[str, MigrationStatus] = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting API Gateway...")
    # Startup
    await check_service_health()
    yield
    # Shutdown
    logger.info("Shutting down API Gateway...")

app = FastAPI(
    title="Migration API Gateway",
    description="Microservice API Gateway for Marzneshin to Marzban Migration",
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_http_client() -> httpx.AsyncClient:
    """Get HTTP client for service communication."""
    return httpx.AsyncClient(timeout=30.0)

async def call_service(service_name: str, endpoint: str, method: str = "GET", data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Call a microservice endpoint."""
    if service_name not in SERVICE_URLS:
        raise HTTPException(status_code=404, detail=f"Service {service_name} not found")
    
    url = f"{SERVICE_URLS[service_name]}{endpoint}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            if method.upper() == "GET":
                response = await client.get(url)
            elif method.upper() == "POST":
                response = await client.post(url, json=data)
            elif method.upper() == "PUT":
                response = await client.put(url, json=data)
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported method: {method}")
            
            response.raise_for_status()
            return response.json()
        
        except httpx.RequestError as e:
            logger.error(f"Request error calling {service_name}: {e}")
            raise HTTPException(status_code=503, detail=f"Service {service_name} unavailable")
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error calling {service_name}: {e}")
            raise HTTPException(status_code=e.response.status_code, detail=f"Service error: {e.response.text}")

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Migration API Gateway",
        "version": "2.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/health/services", response_model=Dict[str, ServiceHealth])
async def check_service_health():
    """Check health of all microservices."""
    health_status = {}
    
    async with httpx.AsyncClient(timeout=5.0) as client:
        for service_name, service_url in SERVICE_URLS.items():
            start_time = datetime.utcnow()
            try:
                response = await client.get(f"{service_url}/health")
                end_time = datetime.utcnow()
                response_time = (end_time - start_time).total_seconds() * 1000
                
                if response.status_code == 200:
                    health_status[service_name] = ServiceHealth(
                        service=service_name,
                        status="healthy",
                        response_time_ms=response_time,
                        last_check=end_time,
                        details=response.json() if response.content else {}
                    )
                else:
                    health_status[service_name] = ServiceHealth(
                        service=service_name,
                        status="unhealthy",
                        response_time_ms=response_time,
                        last_check=end_time,
                        details={"error": f"HTTP {response.status_code}"}
                    )
            except Exception as e:
                end_time = datetime.utcnow()
                health_status[service_name] = ServiceHealth(
                    service=service_name,
                    status="unhealthy",
                    response_time_ms=0,
                    last_check=end_time,
                    details={"error": str(e)}
                )
    
    return health_status

@app.post("/migration/start", response_model=Dict[str, str])
async def start_migration(request: MigrationRequest, background_tasks: BackgroundTasks):
    """Start a new migration process."""
    import uuid
    migration_id = str(uuid.uuid4())
    
    # Initialize migration status
    migration_statuses[migration_id] = MigrationStatus(
        migration_id=migration_id,
        status="pending",
        progress=0.0,
        current_step="Initializing",
        steps_completed=0,
        total_steps=5,  # extract, transform, validate, import, finalize
        started_at=datetime.utcnow()
    )
    
    # Start migration in background
    background_tasks.add_task(execute_migration, migration_id, request)
    
    return {"migration_id": migration_id, "status": "started"}

@app.get("/migration/{migration_id}/status", response_model=MigrationStatus)
async def get_migration_status(migration_id: str):
    """Get migration status."""
    if migration_id not in migration_statuses:
        raise HTTPException(status_code=404, detail="Migration not found")
    
    return migration_statuses[migration_id]

@app.get("/migration/{migration_id}/logs")
async def get_migration_logs(migration_id: str):
    """Get migration logs."""
    # In production, this would fetch logs from a centralized logging system
    return {"migration_id": migration_id, "logs": "Logs would be retrieved from logging service"}

async def execute_migration(migration_id: str, request: MigrationRequest):
    """Execute the migration process orchestrating all microservices."""
    try:
        status = migration_statuses[migration_id]
        
        # Step 1: Extract data
        status.current_step = "Extracting data from Marzneshin"
        status.progress = 10.0
        logger.info(f"Migration {migration_id}: Starting data extraction")
        
        extraction_result = await call_service(
            "extractor", 
            "/extract", 
            "POST", 
            {"config": request.source_config}
        )
        
        status.steps_completed = 1
        status.progress = 25.0
        
        # Step 2: Transform data
        status.current_step = "Transforming data for Marzban compatibility"
        logger.info(f"Migration {migration_id}: Starting data transformation")
        
        transformation_result = await call_service(
            "transformer",
            "/transform",
            "POST",
            {"data": extraction_result["data"], "options": request.migration_options}
        )
        
        status.steps_completed = 2
        status.progress = 50.0
        
        # Step 3: Validate data
        status.current_step = "Validating transformed data"
        logger.info(f"Migration {migration_id}: Starting data validation")
        
        validation_result = await call_service(
            "validator",
            "/validate",
            "POST",
            {"data": transformation_result["data"]}
        )
        
        status.steps_completed = 3
        status.progress = 75.0
        
        # Step 4: Import data (skip if dry run)
        if not request.dry_run:
            status.current_step = "Importing data to Marzban"
            logger.info(f"Migration {migration_id}: Starting data import")
            
            import_result = await call_service(
                "importer",
                "/import",
                "POST",
                {
                    "data": transformation_result["data"],
                    "config": request.target_config
                }
            )
        else:
            import_result = {"status": "skipped", "message": "Dry run - import skipped"}
        
        status.steps_completed = 4
        status.progress = 90.0
        
        # Step 5: Finalize
        status.current_step = "Finalizing migration"
        status.steps_completed = 5
        status.progress = 100.0
        status.status = "completed"
        status.completed_at = datetime.utcnow()
        
        # Compile statistics
        status.statistics = {
            "extraction": extraction_result.get("statistics", {}),
            "transformation": transformation_result.get("statistics", {}),
            "validation": validation_result.get("statistics", {}),
            "import": import_result.get("statistics", {}) if not request.dry_run else {"dry_run": True}
        }
        
        logger.info(f"Migration {migration_id}: Completed successfully")
        
    except Exception as e:
        logger.error(f"Migration {migration_id} failed: {e}")
        status.status = "failed"
        status.error_message = str(e)
        status.completed_at = datetime.utcnow()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)