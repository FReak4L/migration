# Migration Microservices Architecture

## Overview

This document describes the microservice architecture implementation for the Marzneshin to Marzban migration system. The architecture follows modern cloud-native principles with service decomposition, containerization, and orchestration.

## Architecture Components

### 1. API Gateway (`services/gateway/`)
- **Port**: 8000
- **Purpose**: Central entry point for all client requests
- **Features**:
  - Request routing to appropriate microservices
  - Authentication and authorization
  - Rate limiting and throttling
  - Request/response transformation
  - Circuit breaker pattern
  - Migration orchestration

### 2. Extractor Service (`services/extractor/`)
- **Port**: 8001
- **Purpose**: Data extraction from Marzneshin
- **Features**:
  - Marzneshin API integration
  - Connection pooling
  - Retry mechanisms
  - Rate limiting compliance
  - Data extraction statistics

### 3. Transformer Service (`services/transformer/`)
- **Port**: 8002
- **Purpose**: Data transformation with schema compatibility
- **Features**:
  - Enhanced schema compatibility engine
  - UUID format conversion
  - Datetime transformation
  - Field mapping system
  - Batch processing
  - Transformation statistics

### 4. Validator Service (`services/validator/`)
- **Port**: 8004
- **Purpose**: Data validation for transformed data
- **Features**:
  - Comprehensive validation rules
  - Schema validation
  - Data integrity checks
  - Validation reporting
  - Error categorization

### 5. Importer Service (`services/importer/`)
- **Port**: 8003
- **Purpose**: Data import to Marzban
- **Features**:
  - Transactional imports
  - Rollback capabilities
  - Batch processing
  - Error recovery
  - Import statistics

### 6. Configuration Service (`services/config/`)
- **Port**: 8005
- **Purpose**: Centralized configuration management
- **Features**:
  - Environment-specific configurations
  - Dynamic configuration updates
  - Service discovery
  - Configuration versioning

## Service Communication

### Synchronous Communication
- **Protocol**: HTTP/REST
- **Format**: JSON
- **Authentication**: Service-to-service tokens
- **Timeout**: Configurable per service

### Asynchronous Communication (Future)
- **Message Broker**: Apache Kafka
- **Event Sourcing**: For audit trails
- **Event Types**: Migration events, status updates

## Deployment

### Docker Compose (Development)
```bash
# Start all services
docker-compose up -d

# Start specific service
docker-compose up -d api-gateway

# View logs
docker-compose logs -f transformer-service

# Scale services
docker-compose up -d --scale transformer-service=3
```

### Kubernetes (Production)
```bash
# Apply configurations
kubectl apply -f k8s/

# Check status
kubectl get pods -n migration

# Scale deployment
kubectl scale deployment transformer-service --replicas=3
```

## Service Endpoints

### API Gateway
- `GET /` - Service information
- `GET /health` - Health check
- `GET /health/services` - All services health
- `POST /migration/start` - Start migration
- `GET /migration/{id}/status` - Migration status
- `GET /migration/{id}/logs` - Migration logs

### Extractor Service
- `GET /health` - Health check
- `POST /extract` - Extract data
- `GET /extract/test-connection` - Test connection

### Transformer Service
- `GET /health` - Health check
- `POST /transform` - Transform data
- `POST /transform/analyze-schema` - Analyze schema
- `GET /transform/field-mappings` - Get field mappings

### Validator Service
- `GET /health` - Health check
- `POST /validate` - Validate data
- `POST /validate/users` - Validate users only
- `POST /validate/admins` - Validate admins only
- `GET /validate/rules` - Get validation rules

### Importer Service
- `GET /health` - Health check
- `POST /import` - Import data
- `POST /import/test-connection` - Test connection
- `POST /import/rollback` - Rollback import
- `GET /import/status/{id}` - Import status

### Configuration Service
- `GET /health` - Health check
- `GET /config/{service}` - Get service config
- `POST /config/{service}` - Update service config
- `GET /config` - List all configurations
- `GET /config/{service}/environments` - List environments

## Configuration Management

### Environment Variables
```bash
# Service URLs
EXTRACTOR_SERVICE_URL=http://extractor-service:8001
TRANSFORMER_SERVICE_URL=http://transformer-service:8002
VALIDATOR_SERVICE_URL=http://validator-service:8004
IMPORTER_SERVICE_URL=http://importer-service:8003
CONFIG_SERVICE_URL=http://config-service:8005

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Database
DATABASE_URL=postgresql://user:pass@db:5432/migration

# Redis
REDIS_URL=redis://redis:6379/0
```

### Configuration Files
- `docker-compose.yml` - Development orchestration
- `nginx.conf` - Load balancer configuration
- `k8s/` - Kubernetes manifests
- `configs/` - Service-specific configurations

## Monitoring and Observability

### Health Checks
- Each service exposes `/health` endpoint
- Docker health checks configured
- Kubernetes liveness/readiness probes

### Logging
- Structured logging with JSON format
- Centralized log aggregation
- Log levels: DEBUG, INFO, WARN, ERROR

### Metrics
- Prometheus metrics exposure
- Service-specific metrics
- Business metrics (migration success rate, etc.)

### Tracing
- Distributed tracing with OpenTelemetry
- Request correlation IDs
- Performance monitoring

## Security

### Service-to-Service Authentication
- JWT tokens for service communication
- Mutual TLS (mTLS) for production
- API key authentication

### Network Security
- Private Docker networks
- Kubernetes network policies
- Firewall rules

### Data Security
- Encryption at rest
- Encryption in transit
- Sensitive data masking in logs

## Scaling and Performance

### Horizontal Scaling
- Stateless service design
- Load balancing with Nginx
- Auto-scaling based on metrics

### Performance Optimization
- Connection pooling
- Caching with Redis
- Async processing
- Batch operations

### Resource Management
- CPU and memory limits
- Resource quotas
- Quality of Service (QoS)

## Error Handling and Resilience

### Circuit Breaker Pattern
- Fail-fast behavior
- Automatic recovery
- Fallback mechanisms

### Retry Mechanisms
- Exponential backoff
- Jitter for avoiding thundering herd
- Maximum retry limits

### Graceful Degradation
- Partial functionality during failures
- Fallback to cached data
- User-friendly error messages

## Development Workflow

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run single service
python services/gateway/main.py

# Run tests
pytest tests/

# Code formatting
black .
isort .
```

### Testing
- Unit tests for each service
- Integration tests for service communication
- End-to-end tests for complete workflows
- Load testing for performance validation

### CI/CD Pipeline
- Automated testing on pull requests
- Docker image building
- Security scanning
- Deployment automation

## Migration from Monolith

### Phase 1: Service Extraction
- Extract services from monolithic codebase
- Maintain backward compatibility
- Gradual migration of functionality

### Phase 2: Service Communication
- Implement service-to-service communication
- Add circuit breakers and retry logic
- Monitor service interactions

### Phase 3: Data Consistency
- Implement distributed transactions
- Add event sourcing
- Ensure data consistency across services

### Phase 4: Full Microservices
- Complete service decomposition
- Independent deployments
- Service mesh implementation

## Troubleshooting

### Common Issues
1. **Service Discovery**: Check service URLs and network connectivity
2. **Authentication**: Verify service tokens and certificates
3. **Performance**: Monitor resource usage and scaling
4. **Data Consistency**: Check transaction logs and event streams

### Debugging Tools
- Service logs: `docker-compose logs -f <service>`
- Health checks: `curl http://localhost:8000/health/services`
- Metrics: Prometheus dashboard
- Tracing: Jaeger UI

## Future Enhancements

### Event Sourcing
- Implement event store
- Add event replay capabilities
- Audit trail for all operations

### Service Mesh
- Istio or Linkerd implementation
- Advanced traffic management
- Enhanced security policies

### Advanced Monitoring
- Application Performance Monitoring (APM)
- Business metrics dashboards
- Alerting and notification systems

### Auto-scaling
- Kubernetes Horizontal Pod Autoscaler
- Vertical Pod Autoscaler
- Custom metrics-based scaling