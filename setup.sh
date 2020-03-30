#!/bin/bash
if [ ! -d "venv" ]; then
  python3 -m venv ./venv
  . ./venv/bin/activate
  pip3 install quandl PyPDF2
else
  . ./venv/bin/activate
fi
