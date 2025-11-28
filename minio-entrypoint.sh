#!/bin/sh
set -e
minio server /data --console-address ":9001" &
sleep 5
mc alias set myminio http://localhost:9000 user password
mc mb myminio/warehouse || true # data io
mc mb myminio/sepex-storage || true # sepex storage
wait