# Contributing to OpenHands Migration Agent

Thank you for your interest in contributing to the OpenHands Migration Agent! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Git
- Basic understanding of async/await patterns
- Familiarity with database migrations

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/YOUR_USERNAME/migration.git
   cd migration
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

4. **Install Pre-commit Hooks**
   ```bash
   pre-commit install
   ```

5. **Run Tests**
   ```bash
   pytest
   ```

## 🔄 Development Workflow

### Branch Naming Convention

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation updates
- `refactor/description` - Code refactoring
- `test/description` - Test improvements

### Commit Message Format

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(uuid): implement batch UUID transformation optimization

- Add UUIDOptimizer class with 1.8M+ ops/sec performance
- Implement memory-efficient batch processing
- Add comprehensive benchmarking suite

Closes #123
```

## 🧪 Testing Guidelines

### Test Structure

```
tests/
├── test_schema_analyzer.py      # Schema analysis tests
├── test_uuid_optimizer.py       # UUID optimization tests
├── test_transaction_manager.py  # Transaction management tests
├── test_fault_tolerance.py      # Fault tolerance tests
├── test_event_sourcing.py       # Event sourcing tests
└── test_reactive_streams.py     # Reactive streams tests
```

### Writing Tests

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test component interactions
3. **Performance Tests**: Benchmark critical operations
4. **End-to-End Tests**: Test complete migration workflows

**Example Test:**
```python
import pytest
from core.uuid_optimizer import UUIDOptimizer

class TestUUIDOptimizer:
    @pytest.fixture
    def optimizer(self):
        return UUIDOptimizer()
    
    def test_batch_transform_performance(self, optimizer):
        uuids = ["550e8400-e29b-41d4-a716-446655440000"] * 10000
        
        start_time = time.time()
        result = optimizer.batch_transform_uuids(uuids)
        end_time = time.time()
        
        ops_per_sec = len(uuids) / (end_time - start_time)
        assert ops_per_sec > 1_000_000  # Minimum 1M ops/sec
        assert len(result) == len(uuids)
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_uuid_optimizer.py

# Run with coverage
pytest --cov=core --cov-report=html

# Run performance benchmarks
pytest tests/test_uuid_performance.py -v --benchmark-only
```

## 📝 Code Style Guidelines

### Python Style

We follow [PEP 8](https://pep8.org/) with some modifications:

- Line length: 88 characters (Black default)
- Use type hints for all function signatures
- Use docstrings for all public methods
- Prefer async/await over callbacks

### Code Formatting

We use automated formatting tools:

```bash
# Format code
black .

# Sort imports
isort .

# Lint code
flake8

# Type checking
mypy .
```

### Documentation Style

```python
async def transform_data(
    self, 
    data: Dict[str, Any], 
    batch_size: int = 1000
) -> Dict[str, Any]:
    """Transform migration data with optimizations.
    
    Args:
        data: Source data to transform
        batch_size: Number of records to process in each batch
        
    Returns:
        Transformed data ready for import
        
    Raises:
        TransformationError: If transformation fails
        
    Example:
        >>> transformer = DataTransformer()
        >>> result = await transformer.transform_data(source_data)
    """
```

## 🏗️ Architecture Guidelines

### Component Design

1. **Single Responsibility**: Each component should have one clear purpose
2. **Dependency Injection**: Use dependency injection for testability
3. **Error Handling**: Implement comprehensive error handling
4. **Logging**: Add structured logging for observability

### Performance Considerations

1. **Async/Await**: Use async patterns for I/O operations
2. **Batch Processing**: Process data in batches for efficiency
3. **Memory Management**: Avoid loading large datasets into memory
4. **Caching**: Implement caching for frequently accessed data

### Example Component Structure

```python
from abc import ABC, abstractmethod
from typing import Any, Dict, List
import logging

class BaseTransformer(ABC):
    """Base class for data transformers."""
    
    def __init__(self, config: AppConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    async def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data according to implementation."""
        pass
    
    async def validate_input(self, data: Dict[str, Any]) -> bool:
        """Validate input data format."""
        # Implementation here
        pass
```

## 🔍 Code Review Process

### Pull Request Guidelines

1. **Small, Focused PRs**: Keep PRs small and focused on a single feature/fix
2. **Clear Description**: Provide clear description of changes
3. **Tests Required**: All new code must include tests
4. **Documentation**: Update documentation for new features

### PR Template

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Performance tests added/updated

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests pass locally
```

### Review Criteria

Reviewers will check for:

1. **Functionality**: Does the code work as intended?
2. **Performance**: Are there any performance implications?
3. **Security**: Are there any security concerns?
4. **Maintainability**: Is the code easy to understand and maintain?
5. **Testing**: Are there adequate tests?

## 🐛 Bug Reports

### Bug Report Template

```markdown
**Describe the Bug**
A clear description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Go to '...'
2. Click on '....'
3. See error

**Expected Behavior**
What you expected to happen.

**Environment**
- OS: [e.g. Ubuntu 20.04]
- Python Version: [e.g. 3.9.0]
- Migration Agent Version: [e.g. 2.0.0]

**Additional Context**
Add any other context about the problem here.
```

## 💡 Feature Requests

### Feature Request Template

```markdown
**Is your feature request related to a problem?**
A clear description of what the problem is.

**Describe the solution you'd like**
A clear description of what you want to happen.

**Describe alternatives you've considered**
Alternative solutions or features you've considered.

**Additional context**
Add any other context or screenshots about the feature request.
```

## 🏆 Recognition

Contributors will be recognized in:

- README.md acknowledgments
- Release notes
- GitHub contributors page

## 📞 Getting Help

- **GitHub Issues**: For bugs and feature requests
- **GitHub Discussions**: For questions and general discussion
- **Email**: For security-related issues

## 📄 License

By contributing, you agree that your contributions will be licensed under the MIT License.

---

Thank you for contributing to OpenHands Migration Agent! 🚀