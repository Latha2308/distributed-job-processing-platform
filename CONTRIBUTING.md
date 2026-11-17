# Contributing Guide

Thank you for considering contributing to the Distributed Job Processing Platform!

## Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Latha2308/distributed-job-processing-platform.git
   cd distributed-job-processing-platform
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   make install
   ```

4. **Set up environment**
   ```bash
   make dev-setup
   # Edit .env file with your configuration
   ```

5. **Start infrastructure**
   ```bash
   make docker-up
   ```

6. **Run database migrations**
   ```bash
   make migrate
   ```

## Code Style

We use the following tools to maintain code quality:

- **Black**: Code formatting (line length: 100)
- **isort**: Import sorting
- **Flake8**: Linting
- **MyPy**: Type checking

Format your code before committing:
```bash
make format
```

Check code quality:
```bash
make lint
```

## Testing

Write tests for all new features and bug fixes:

```bash
make test
```

Test files should:
- Be placed in the `tests/` directory
- Follow the naming convention `test_*.py`
- Use pytest fixtures from `conftest.py`
- Aim for >80% code coverage

## Commit Guidelines

We follow conventional commits:

```
feat: add job cancellation endpoint
fix: resolve race condition in worker
docs: update API documentation
test: add tests for retry mechanism
refactor: simplify queue service
chore: update dependencies
```

## Pull Request Process

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Write code
   - Add tests
   - Update documentation

3. **Ensure quality**
   ```bash
   make format
   make lint
   make test
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "feat: your feature description"
   ```

5. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Create Pull Request**
   - Describe your changes
   - Reference any related issues
   - Wait for review

## Project Structure

```
app/
├── api/              # API endpoints
│   └── v1/
│       ├── endpoints/
│       └── router.py
├── core/             # Core configuration
│   ├── config.py
│   ├── database.py
│   ├── redis_client.py
│   └── metrics.py
├── db/               # Database models
│   └── models.py
├── models/           # Pydantic models
│   ├── job.py
│   └── worker.py
├── services/         # Business logic
│   ├── job_service.py
│   ├── queue_service.py
│   └── worker_service.py
├── worker/           # Worker implementation
│   ├── worker.py
│   └── job_executor.py
└── main.py           # Application entry
```

## Adding New Job Types

To add a new job type:

1. **Add handler in `app/worker/job_executor.py`**
   ```python
   async def _execute_your_job_type(self, payload: Dict[str, Any]) -> Dict[str, Any]:
       # Your implementation
       return {"status": "success"}
   ```

2. **Register handler**
   ```python
   self.handlers = {
       "your_job_type": self._execute_your_job_type,
   }
   ```

3. **Add tests**
   ```python
   @pytest.mark.asyncio
   async def test_your_job_type(client):
       # Test your job type
   ```

## Database Migrations

Create a new migration:
```bash
make migration message="your migration description"
```

Apply migrations:
```bash
make migrate
```

## Documentation

Update documentation when:
- Adding new features
- Changing APIs
- Modifying configuration
- Adding dependencies

Documentation files:
- `README.md`: Overview and quick start
- `ARCHITECTURE.md`: System design
- `CONTRIBUTING.md`: This file
- API docs: Auto-generated at `/docs`

## Questions?

Open an issue for:
- Bug reports
- Feature requests
- Documentation improvements
- Questions about the codebase

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
