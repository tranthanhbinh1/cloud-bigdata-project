#!/usr/bin/env bash
# report.md -> report.pdf (pandoc for HTML with embedded figures, headless Chrome for the PDF).
#   report/build.sh
set -euo pipefail
cd "$(dirname "$0")"
pandoc report.md -f gfm -t html5 --standalone --embed-resources --resource-path=. \
  --metadata title="Experimental Evaluation of Data Compression in Spark over HDFS" \
  --css=report.css -o report.html
google-chrome --headless=new --disable-gpu --no-pdf-header-footer --print-to-pdf=report.pdf \
  "file://$PWD/report.html" 2>/dev/null
rm -f report.html
echo "report/report.pdf"
