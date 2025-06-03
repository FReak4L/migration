#!/usr/bin/env python3
"""
Tests for microservice architecture
"""

import pytest
import asyncio
from unittest.mock import Mock, patch
from datetime import datetime

# Test imports for microservices
try:
    from services.gateway.main import app as gateway_app
    from services.extractor.main import app as extractor_app
    from services.transformer.main import app as transformer_app
    from services.validator.main import app as validator_app
    from services.importer.main import app as importer_app
    from services.config.main import app as config_app
except ImportError:
    pytest.skip("Microservice modules not available", allow_module_level=True)

class TestMicroserviceArchitecture:
    """Test microservice architecture components."""
    
    def test_service_apps_creation(self):
        """Test that all service apps can be created."""
        # Test that all FastAPI apps are properly initialized
        assert gateway_app is not None
        assert extractor_app is not None
        assert transformer_app is not None
        assert validator_app is not None
        assert importer_app is not None
        assert config_app is not None
    
    def test_service_metadata(self):
        """Test service metadata and configuration."""
        # Test gateway app
        assert gateway_app.title == "Migration API Gateway"
        assert gateway_app.version == "2.0.0"
        
        # Test extractor app
        assert extractor_app.title == "Extractor Service"
        assert extractor_app.version == "1.0.0"
        
        # Test transformer app
        assert transformer_app.title == "Transformer Service"
        assert transformer_app.version == "2.0.0"
        
        # Test validator app
        assert validator_app.title == "Validator Service"
        assert validator_app.version == "1.0.0"
        
        # Test importer app
        assert importer_app.title == "Importer Service"
        assert importer_app.version == "1.0.0"
        
        # Test config app
        assert config_app.title == "Configuration Service"
        assert config_app.version == "1.0.0"
    
    def test_service_routes(self):
        """Test that services have required routes."""
        # Get routes for each service
        gateway_routes = [route.path for route in gateway_app.routes]
        extractor_routes = [route.path for route in extractor_app.routes]
        transformer_routes = [route.path for route in transformer_app.routes]
        validator_routes = [route.path for route in validator_app.routes]
        importer_routes = [route.path for route in importer_app.routes]
        config_routes = [route.path for route in config_app.routes]
        
        # Test gateway routes
        assert "/" in gateway_routes
        assert "/health" in gateway_routes
        assert "/health/services" in gateway_routes
        assert "/migration/start" in gateway_routes
        assert "/migration/{migration_id}/status" in gateway_routes
        
        # Test extractor routes
        assert "/" in extractor_routes
        assert "/health" in extractor_routes
        assert "/extract" in extractor_routes
        assert "/extract/test-connection" in extractor_routes
        
        # Test transformer routes
        assert "/" in transformer_routes
        assert "/health" in transformer_routes
        assert "/transform" in transformer_routes
        assert "/transform/analyze-schema" in transformer_routes
        assert "/transform/field-mappings" in transformer_routes
        
        # Test validator routes
        assert "/" in validator_routes
        assert "/health" in validator_routes
        assert "/validate" in validator_routes
        assert "/validate/users" in validator_routes
        assert "/validate/admins" in validator_routes
        assert "/validate/rules" in validator_routes
        
        # Test importer routes
        assert "/" in importer_routes
        assert "/health" in importer_routes
        assert "/import" in importer_routes
        assert "/import/test-connection" in importer_routes
        assert "/import/rollback" in importer_routes
        
        # Test config routes
        assert "/" in config_routes
        assert "/health" in config_routes
        assert "/config/{service_name}" in config_routes
        assert "/config" in config_routes

class TestServiceCommunication:
    """Test service-to-service communication patterns."""
    
    @pytest.mark.asyncio
    async def test_service_health_check_format(self):
        """Test that health check responses follow expected format."""
        from services.gateway.main import health_check as gateway_health
        from services.extractor.main import health_check as extractor_health
        from services.transformer.main import health_check as transformer_health
        
        # Test gateway health check
        gateway_response = await gateway_health()
        assert "status" in gateway_response
        assert "timestamp" in gateway_response
        assert gateway_response["status"] == "healthy"
        
        # Test extractor health check
        extractor_response = await extractor_health()
        assert "status" in extractor_response
        assert "service" in extractor_response
        assert "timestamp" in extractor_response
        assert extractor_response["status"] == "healthy"
        assert extractor_response["service"] == "extractor"
        
        # Test transformer health check
        transformer_response = await transformer_health()
        assert "status" in transformer_response
        assert "service" in transformer_response
        assert "timestamp" in transformer_response
        assert "features" in transformer_response
        assert transformer_response["status"] == "healthy"
        assert transformer_response["service"] == "transformer"

class TestDockerConfiguration:
    """Test Docker configuration files."""
    
    def test_docker_compose_structure(self):
        """Test docker-compose.yml structure."""
        import yaml
        import os
        
        compose_file = "/workspace/migration/docker-compose.yml"
        assert os.path.exists(compose_file), "docker-compose.yml should exist"
        
        with open(compose_file, 'r') as f:
            compose_config = yaml.safe_load(f)
        
        # Test basic structure
        assert "version" in compose_config
        assert "services" in compose_config
        assert "networks" in compose_config
        assert "volumes" in compose_config
        
        # Test required services
        services = compose_config["services"]
        required_services = [
            "config-service",
            "extractor-service", 
            "transformer-service",
            "validator-service",
            "importer-service",
            "api-gateway"
        ]
        
        for service in required_services:
            assert service in services, f"Service {service} should be defined"
            
            # Test service configuration
            service_config = services[service]
            assert "build" in service_config or "image" in service_config
            assert "ports" in service_config
            assert "environment" in service_config
            assert "healthcheck" in service_config
    
    def test_dockerfile_existence(self):
        """Test that Dockerfile exists for each service."""
        import os
        
        services = [
            "gateway", "extractor", "transformer", 
            "validator", "importer", "config"
        ]
        
        for service in services:
            dockerfile_path = f"/workspace/migration/services/{service}/Dockerfile"
            assert os.path.exists(dockerfile_path), f"Dockerfile for {service} should exist"
    
    def test_nginx_configuration(self):
        """Test nginx configuration file."""
        import os
        
        nginx_file = "/workspace/migration/nginx.conf"
        assert os.path.exists(nginx_file), "nginx.conf should exist"
        
        with open(nginx_file, 'r') as f:
            nginx_config = f.read()
        
        # Test basic nginx structure
        assert "upstream api_gateway" in nginx_config
        assert "upstream extractor_service" in nginx_config
        assert "upstream transformer_service" in nginx_config
        assert "upstream validator_service" in nginx_config
        assert "upstream importer_service" in nginx_config
        assert "upstream config_service" in nginx_config
        
        # Test rate limiting
        assert "limit_req_zone" in nginx_config
        assert "limit_req" in nginx_config

class TestServiceModels:
    """Test Pydantic models used in services."""
    
    def test_migration_request_model(self):
        """Test MigrationRequest model."""
        from services.gateway.main import MigrationRequest
        
        # Test valid request
        request_data = {
            "source_config": {"url": "http://marzneshin.example.com"},
            "target_config": {"url": "http://marzban.example.com"},
            "migration_options": {"batch_size": 100},
            "dry_run": True
        }
        
        request = MigrationRequest(**request_data)
        assert request.source_config == {"url": "http://marzneshin.example.com"}
        assert request.target_config == {"url": "http://marzban.example.com"}
        assert request.migration_options == {"batch_size": 100}
        assert request.dry_run is True
    
    def test_migration_status_model(self):
        """Test MigrationStatus model."""
        from services.gateway.main import MigrationStatus
        
        # Test valid status
        status_data = {
            "migration_id": "test-123",
            "status": "running",
            "progress": 50.0,
            "current_step": "Transforming data",
            "steps_completed": 2,
            "total_steps": 5,
            "started_at": datetime.utcnow()
        }
        
        status = MigrationStatus(**status_data)
        assert status.migration_id == "test-123"
        assert status.status == "running"
        assert status.progress == 50.0
        assert status.current_step == "Transforming data"
        assert status.steps_completed == 2
        assert status.total_steps == 5
    
    def test_service_health_model(self):
        """Test ServiceHealth model."""
        from services.gateway.main import ServiceHealth
        
        # Test valid health status
        health_data = {
            "service": "extractor",
            "status": "healthy",
            "response_time_ms": 150.5,
            "last_check": datetime.utcnow(),
            "details": {"version": "1.0.0"}
        }
        
        health = ServiceHealth(**health_data)
        assert health.service == "extractor"
        assert health.status == "healthy"
        assert health.response_time_ms == 150.5
        assert health.details == {"version": "1.0.0"}

if __name__ == "__main__":
    pytest.main([__file__, "-v"])