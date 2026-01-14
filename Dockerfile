FROM python:3.11-slim AS builder

ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libpq-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy requirements and build wheels to avoid shipping build deps into final image
COPY requirements.txt /build/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel \
    && if [ -s /build/requirements.txt ]; then pip wheel --wheel-dir /wheels -r /build/requirements.txt; fi

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

# Create non-root user early so ownership operations can use it
ARG APP_USER=foip
ARG APP_UID=1000
RUN groupadd -g ${APP_UID} ${APP_USER} || true \
    && useradd -m -u ${APP_UID} -g ${APP_UID} -s /bin/sh ${APP_USER} || true

WORKDIR /app

# Copy built wheels from builder and install them (no build deps required)
COPY --from=builder /wheels /wheels
COPY requirements.txt /app/requirements.txt
RUN if [ -d /wheels ]; then pip install --no-index --find-links=/wheels -r /app/requirements.txt; else python -m pip install --upgrade pip setuptools wheel && pip install -r /app/requirements.txt; fi

# Copy application code
COPY . /app

# Ensure entrypoint is executable
RUN chmod +x /app/scripts/entrypoint.sh || true

# Set ownership to non-root user
RUN chown -R ${APP_USER}:${APP_USER} /app /wheels || true

USER ${APP_USER}

ENV PATH="/home/${APP_USER}/.local/bin:${PATH}"

EXPOSE 8000

ENTRYPOINT ["/app/scripts/entrypoint.sh"]
CMD ["python", "-m", "src.main"]
