#~/bin/bash

METRIC_VERSION=(metrics_3.6 metrics_4.0 metrics_4.2 metrics_4.4 metrics_5.0)

for m in "${METRIC_VERSION[@]}"   ;
do
  echo $m
  go run mongolo  "$m.ftdc" 1 > "$m.txt"
  grep -v "^#" "$m.txt"  > "$m.csv"
done

