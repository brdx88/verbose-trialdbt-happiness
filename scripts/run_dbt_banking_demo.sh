#!/bin/bash
set -e

dbt deps

dbt build --select banking_demo

dbt test --select assert_sri_today_exists
dbt test --select assert_acl_today_exists
dbt test --select assert_dm_customer_today_exists
dbt test --select assert_dm_customer_existsrow_count_anomaly
dbt test --select assert_dm_customer_total_balance_anomaly