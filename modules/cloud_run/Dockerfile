FROM python:3.12-slim

ENV APP_HOME=/app
WORKDIR $APP_HOME

# Create a non-root user
RUN groupadd -r storageflow && useradd -r -g storageflow -d $APP_HOME storageflow

# Install Poetry
RUN pip install poetry && poetry self add poetry-plugin-export

# Copy project definition files
COPY ./tmp/pyproject.toml ./pyproject.toml
COPY ./tmp/poetry.lock ./poetry.lock

# Export requirements and install with pip
RUN poetry export --without-hashes --without dev -f requirements.txt > requirements.txt \
  && pip install --no-cache-dir -r requirements.txt

# Copy actual application code
COPY ./tmp/storageflow ./storageflow


COPY ./tmp/config/user ./config/user
COPY ./tmp/config/root ./config/root


# Change ownership of the app directory
RUN chown -R storageflow:storageflow $APP_HOME

# Switch to the non-root user
USER storageflow

# Entrypoint
CMD ["uvicorn", "storageflow.main:app", "--host", "0.0.0.0", "--port", "8080", "--log-level", "debug"]

