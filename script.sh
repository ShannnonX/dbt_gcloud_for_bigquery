#!/bin/sh
dbt deps
dbt run --profiles-dir .