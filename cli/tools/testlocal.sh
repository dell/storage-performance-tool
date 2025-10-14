#!/bin/bash

# run spt tests locally

echo "=== Run S3 Write test ==="
echo

./spt --debug run write \
  --endpoint http://10.246.190.191:9000 \
  --access-key G54D8NZVL5VRYHE3SF8I \
  --secret-key AoyXzJHxJKcIuS+OWRx6GadW+Y94orgxJzy+3DfA \
  --bucket testwrites \
  --threads 4 \
  --object-size 1MB \
  --object-count 1000 \
  --cleanup \
  --keep-scenario

echo "=== Test Complete ==="
echo
