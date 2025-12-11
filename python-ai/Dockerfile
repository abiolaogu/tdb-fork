FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g. for building packages)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project definition
COPY pyproject.toml README.md ./

# Copy source code
COPY lumaai ./lumaai

# Install dependencies and the package
RUN pip install --no-cache-dir .

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "lumaai.main:app", "--host", "0.0.0.0", "--port", "8000"]
