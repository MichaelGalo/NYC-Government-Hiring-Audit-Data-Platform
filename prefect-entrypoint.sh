#!/bin/bash
set -e

# Wait for Prefect server DB to be ready (optional: add a sleep or health check)
sleep 5

# Create work pool if it doesn't exist
prefect work-pool create data_ingestion --type process || true

# Start Prefect server
prefect server start --host 0.0.0.0