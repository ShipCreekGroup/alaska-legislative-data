#!/bin/bash

# This script zips a directory and creates a GitHub release with the zip file.
# Usage: ./release.sh <directory>
directory=${1}
if [ -z "$directory" ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi
# Make sure the directory exists
if [ ! -d "$directory" ]; then
    echo "Error: Directory '$directory' does not exist."
    exit 1
fi

# Create a temporary zip file
zip_file="${directory}.zip"
echo "Creating zip file from directory: $directory"
zip -r "$zip_file" "$directory"
# Display the size of the zip file
echo "Zip file size: $(du -h "$zip_file" | cut -f1)"

# Create GitHub release with the zip file
gh release create "$(date -u '+%Y%m%d-%H%M%S')" \
    "$zip_file" \
    --title "$(date -u -Iseconds)" \
    --notes "Released at $(TZ='America/Anchorage' date) AK time"

# Clean up
echo "Cleaning up temporary zip file"
rm "$zip_file"