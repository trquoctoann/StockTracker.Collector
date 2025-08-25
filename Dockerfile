# Multi-stage build cho Stock Tracker Collector
FROM python:3.12-slim as builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml .
COPY pytest.ini .

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel
RUN pip install .

# Production stage
FROM python:3.12-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Create non-root user
RUN groupadd -r stocktracker && useradd -r -g stocktracker stocktracker

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create app directory and required subdirectories
WORKDIR /app
RUN mkdir -p logs output data && chown -R stocktracker:stocktracker /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy application code
COPY . .
RUN chown -R stocktracker:stocktracker /app

# Switch to non-root user
USER stocktracker

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python main.py health || exit 1

# Default command
CMD ["python", "main.py"]

# Labels
LABEL maintainer="Toan Tran Quoc <trquoctoan.work@gmail.com>"
LABEL description="Stock Tracker Collector - Async data collection system"
LABEL version="0.0.1"
