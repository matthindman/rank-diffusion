#!/bin/zsh
# A10 audit: both long panels, one after the other.
PY=/Library/Frameworks/Python.framework/Versions/3.11/bin/python3
for p in reddit_submissions_long reddit_comments_ext; do
  $PY -u llm_fitting/phase1_a10.py audit $p || exit 1
done
