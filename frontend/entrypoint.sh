#!/usr/bin/env sh
set -e

echo "🔄 Installing JS dependencies and (re)building assets..."
cd assets
npm ci
npm run deploy

echo "🚀 Starting Phoenix server..."
cd ..
exec mix phx.server