# Iceberg Physical Sort vs Binpack

Source snapshot: `7389069542196458125`

Six isolated layouts; same snapshot, category partitioning, no result reuse, one excluded warmup per query.
Sort is implemented with PyArrow, not Spark rewrite_data_files. Small row groups use 4096 rows, page index disabled.
Binpack uses Athena OPTIMIZE. rewrite_large controls for the writer/encoding change.

| Layout | Files | Row groups | Active bytes | Build seconds |
| --- | ---: | ---: | ---: | ---: |
| binpack | 19 | 19 | 12569775 | 13.179 |
| rewrite_large | 19 | 19 | 8388653 | 13.231 |
| date_large | 19 | 19 | 7576334 | 13.465 |
| rewrite_small | 19 | 88 | 18328468 | 23.713 |
| date_small | 19 | 88 | 20014882 | 25.001 |
| id_small | 19 | 88 | 7247566 | 23.264 |

| Workload | Layout | Mean scanned bytes | Mean total ms | Median total ms | Mean engine ms |
| --- | --- | ---: | ---: | ---: | ---: |
| full | binpack | 641719 | 962.33 | 965.00 | 786.33 |
| full | rewrite_large | 645567 | 1010.33 | 866.00 | 854.00 |
| full | date_large | 584523 | 912.67 | 898.00 | 747.00 |
| full | rewrite_small | 1114673 | 1135.00 | 1029.00 | 970.67 |
| full | date_small | 1239411 | 1038.67 | 1017.00 | 871.00 |
| full | id_small | 499116 | 921.00 | 911.00 | 800.33 |
| latest_day | binpack | 840984 | 886.00 | 890.00 | 736.33 |
| latest_day | rewrite_large | 835444 | 919.33 | 878.00 | 787.00 |
| latest_day | date_large | 148189 | 854.00 | 852.00 | 706.00 |
| latest_day | rewrite_small | 337670 | 972.67 | 999.00 | 828.67 |
| latest_day | date_small | 109852 | 1742.33 | 1906.00 | 1591.67 |
| latest_day | id_small | 693561 | 939.33 | 947.00 | 780.33 |
| latest_week | binpack | 840984 | 876.67 | 801.00 | 737.33 |
| latest_week | rewrite_large | 835444 | 1180.33 | 913.00 | 1030.00 |
| latest_week | date_large | 327663 | 781.00 | 753.00 | 647.67 |
| latest_week | rewrite_small | 491425 | 1602.67 | 1136.00 | 1464.00 |
| latest_week | date_small | 194042 | 1463.33 | 1043.00 | 1228.33 |
| latest_week | id_small | 693561 | 1814.33 | 1297.00 | 1592.67 |
| product_id | binpack | 132934 | 854.33 | 791.00 | 702.67 |
| product_id | rewrite_large | 129934 | 1173.00 | 1146.00 | 1008.33 |
| product_id | date_large | 125623 | 856.67 | 850.00 | 722.67 |
| product_id | rewrite_small | 607315 | 945.00 | 908.00 | 802.00 |
| product_id | date_small | 684398 | 963.67 | 929.00 | 816.00 |
| product_id | id_small | 37204 | 880.00 | 928.00 | 734.00 |
| category_day | binpack | 25980 | 825.33 | 794.00 | 676.33 |
| category_day | rewrite_large | 25837 | 1350.67 | 811.00 | 1217.67 |
| category_day | date_large | 1809 | 846.67 | 807.00 | 702.67 |
| category_day | rewrite_small | 13445 | 786.33 | 776.00 | 652.67 |
| category_day | date_small | 2173 | 752.00 | 752.00 | 614.33 |
| category_day | id_small | 22152 | 826.67 | 808.00 | 679.33 |
| category | binpack | 17439 | 1001.33 | 710.00 | 862.33 |
| category | rewrite_large | 17550 | 730.33 | 717.00 | 592.33 |
| category | date_large | 15948 | 1005.67 | 825.00 | 881.67 |
| category | rewrite_small | 23934 | 775.33 | 791.00 | 619.33 |
| category | date_small | 22439 | 837.67 | 849.00 | 696.00 |
| category | id_small | 14700 | 714.00 | 669.00 | 586.00 |

All output row-multiset fingerprints and per-workload query result hashes matched.
GET counts are not measured; API counts in JSON are not S3 request counts.
Build timings cover preparation/validation in the completing attempt; resumed runs exclude earlier work. Use the separate OPTIMIZE statistics for Athena compaction runtime.
Experiment tables and data remain isolated under the recorded benchmark prefix. Do not expire snapshots on production as cleanup.
No statistical significance claim from three runs. Workloads are representative experiments, not measured business traffic.
