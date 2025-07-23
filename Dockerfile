# Multi-stage build for smaller final image
FROM python:3.11-slim AS builder

# Install uv for fast dependency management
RUN pip install uv

WORKDIR /app

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install dependencies
RUN uv sync --frozen --no-dev

FROM python:3.11-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy from builder stage
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY pyproject.toml ./

# Activate virtual environment
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"

# Create data directories
RUN mkdir -p data/{input,output} .cache

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
ENTRYPOINT ["python", "-m", "company_enricher.cli"]
CMD ["--help"]
