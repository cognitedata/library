#!/bin/zsh
# Get current date in YYYY-MM-DD format
current_date=$(date +%Y-%m-%d)

# Launch tests and save results with date in filename
echo 'Launching automated tests... This may take a while.'
mkdir -p test-results
pytest --self-contained-html --html=test-results/test-results-${current_date}.html

