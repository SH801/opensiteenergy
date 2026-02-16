#!/bin/bash

if [ -f ".env" ]; then
    . ./.env
fi

. venv/bin/activate
uvicorn opensiteenergy:app --host 0.0.0.0 --port 8000 --log-level info