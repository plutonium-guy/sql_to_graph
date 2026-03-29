#!/usr/bin/env bash
set -euo pipefail

# Publish sql-to-graph to PyPI
# Usage: ./scripts/publish.sh [run-id]
#   run-id: GitHub Actions run ID (defaults to latest tag run)

REPO="plutonium-guy/sql_to_graph"
DIST_DIR="/tmp/sql_to_graph_publish"

# Require token
if [ -z "${UV_PUBLISH_TOKEN:-}" ]; then
    echo "Error: UV_PUBLISH_TOKEN not set"
    echo "Export it first: export UV_PUBLISH_TOKEN=pypi-..."
    exit 1
fi

# Get run ID
if [ -n "${1:-}" ]; then
    RUN_ID="$1"
else
    echo "Finding latest tag run..."
    RUN_ID=$(gh run list --repo "$REPO" --limit 20 --json databaseId,event,headBranch,status,conclusion \
        --jq '[.[] | select(.headBranch | startswith("v"))][0].databaseId')
    if [ -z "$RUN_ID" ]; then
        echo "Error: No tag run found"
        exit 1
    fi
fi

echo "Using run: $RUN_ID"

# Check run status
STATUS=$(gh run view "$RUN_ID" --repo "$REPO" --json status,conclusion --jq '.status')
if [ "$STATUS" != "completed" ]; then
    echo "Run $RUN_ID is still $STATUS, waiting..."
    gh run watch "$RUN_ID" --repo "$REPO" --exit-status || true
fi

# Clean and download
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

echo "Downloading artifacts..."
gh run download "$RUN_ID" --repo "$REPO" --dir "$DIST_DIR"

# Flatten: move all .whl and .tar.gz into one directory
mkdir -p "$DIST_DIR/dist"
find "$DIST_DIR" -name "*.whl" -exec mv {} "$DIST_DIR/dist/" \;
find "$DIST_DIR" -name "*.tar.gz" -exec mv {} "$DIST_DIR/dist/" \;

echo ""
echo "Files to publish:"
ls -lh "$DIST_DIR/dist/"
echo ""

TOTAL=$(ls "$DIST_DIR/dist/" | wc -l | tr -d ' ')
echo "Total: $TOTAL files"
echo ""

read -p "Publish to PyPI? [y/N] " confirm
if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
    echo "Aborted."
    exit 0
fi

echo "Publishing..."
uv publish "$DIST_DIR/dist/"*

echo ""
echo "Done! Published $TOTAL files to https://pypi.org/project/sql-to-graph/"

# Cleanup
rm -rf "$DIST_DIR"
