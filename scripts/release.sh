#!/bin/bash

# Zips a directory, publishes it as a new GitHub release, and deletes every
# older release (R2 keeps the last 30 days; the raw store can rebuild any day).
# Usage: scripts/release.sh <directory>
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
tag="$(date -u '+%Y%m%d-%H%M%S')"
gh release create "$tag" \
    "$zip_file" \
    --title "$(date -u -Iseconds)" \
    --notes "Released at $(TZ='America/Anchorage' date) AK time"

# Clean up
echo "Cleaning up temporary zip file"
rm "$zip_file"

# Delete older releases and their tags
gh release list --limit 1000 --json tagName --jq '.[].tagName' | while read -r old; do
    if [ "$old" != "$tag" ]; then
        echo "Deleting release $old"
        gh release delete "$old" --yes --cleanup-tag
    fi
done
