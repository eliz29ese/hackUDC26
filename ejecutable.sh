#!/usr/bin/env bash
set -euo pipefail
set -x

export METEOSIX_API_KEY="A5HGOhjDyj9XpW5T5Uu49QwbyunOvp727PU9vONJK1IOKKz6U9vO0P9fs98c999d"
export INFLUX_URL="https://us-east-1-1.aws.cloud2.influxdata.com"
export INFLUX_TOKEN="28vx12MetAtmNZp0ikvhvCj6W4kiml2qiAyYlhS35RuP-CNwyQ6_hvXdec7pu-savPFY5sue5M8PVWkafWv8Wg=="
export INFLUX_ORG="ad81ae82a9b1e1fc"
export INFLUX_BUCKET="meteo_galicia"

python -u meteosix_to_influx.py