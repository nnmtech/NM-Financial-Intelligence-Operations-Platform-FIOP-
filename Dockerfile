FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create app user and directory
ARG APP_USER=foip
ARG APP_UID=1000
RUN groupadd -g ${APP_UID} ${APP_USER} || true \
    && useradd -m -u ${APP_UID} -g ${APP_UID} -s /bin/sh ${APP_USER} || true

WORKDIR /app

# Copy requirements and install (if present)
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
    && if [ -s /app/requirements.txt ]; then pip install -r /app/requirements.txt; fi

# Copy application code
COPY . /app

# Ensure entrypoint is executable
RUN chmod +x /app/scripts/entrypoint.sh || true

# Set ownership to non-root user
RUN chown -R ${APP_USER}:${APP_USER} /app

USER ${APP_USER}

ENV PATH="/home/${APP_USER}/.local/bin:${PATH}"

EXPOSE 8000

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["python", "-m", "src.main"]
