#!/bin/bash

# Get the directory where this script is located
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Start frontend
echo "⚛️  Starting frontend server..."
cd "$PROJECT_ROOT/frontend"
NODE_ENV=development npm run electron:dev &
FRONTEND_PID=$!

# Wait a moment for backend to start
#sleep 10

# Start backend in background
echo "🐍 Starting backend server..."
cd "$PROJECT_ROOT/backend"
APP_ENV=development uv run uvicorn src.api.server:app --port 8000 --reload
BACKEND_PID=$!

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID