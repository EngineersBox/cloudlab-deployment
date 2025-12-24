#!/usr/bin/env bash

python3 profile.py \
    --dc_count=1 \
    --racks_per_dc=3 \
    --nodes_per_rack=1 \
    --application=hbase \
    --application_version=2.6 \
    --collector_version=2.6 \
    --github_username=engineersbox \
    --github_token="$(pass github/engineersbox/readonly)"
