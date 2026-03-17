#!/bin/bash

# --- CONFIGURATION ---
PROJECT_DIR="/var/lib/fragcomms/backend_scripts"
BRANCH="main" # Change this if you use 'master' or a 'dev' branch
SERVICE_NAME="fragcomms-backend" # Assuming you use systemd to run your server

echo "[$(date)] Checking for updates..."

# 1. Navigate to the project directory
cd "$PROJECT_DIR" || { echo "Failed to enter directory"; exit 1; }

# 2. Fetch the latest metadata from the remote repository
git fetch origin "$BRANCH"

# 3. Compare local commit hash to remote commit hash
LOCAL_HASH=$(git rev-parse HEAD)
REMOTE_HASH=$(git rev-parse origin/"$BRANCH")

if [ "$LOCAL_HASH" = "$REMOTE_HASH" ]; then
    echo "Server is already up to date. Exiting."
    exit 0
fi

echo "New commit detected! ($REMOTE_HASH)"
echo "Pulling latest code..."

# 4. Pull the new code
git pull origin "$BRANCH"

# 5. Update Python Dependencies (Optional but highly recommended)
# This ensures if you add a new package to requirements.txt, the server gets it
echo "Updating Python dependencies..."
source src/.venv/bin/activate
pip install -r src/requirements.txt

# 6. Restart the backend service
echo "Restarting the backend service..."
sudo rc-service $SERVICE_NAME restart

echo "[$(date)] Update complete!"
