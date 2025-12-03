#!/usr/bin/env bash

uv sync

sed -inE 's/^\(\s.*\)print \(.*\)$/\1print(\2)/g' .venv/lib/python3.13/site-packages/geni/portal.py
sed -inE '93s/f.write(buf)/f.write(str(buf))/g' .venv/lib/python3.13/site-packages/geni/rspec/pg.py
