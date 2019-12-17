#!/bin/sh
# Shell script to send generated dag files to remote location
dag-generator -start="$1" -end="$2" -out="$HOME"/.ethash
rsync -avz --ignore-existing "$HOME"/.ethash/* "$3"
rm -rf "$HOME"/.ethash/
