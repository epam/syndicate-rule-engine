#!/bin/sh

set -e

echo "Creating the necessary buckets in Minio"
python main.py create_buckets

echo "Creating indexes in MongoDB"
python main.py create_indexes

echo "Creating the necessary engine and token in Vault"
python main.py init_vault

echo "Initializing"
python main.py init

echo "Starting server"
python main.py run --gunicorn