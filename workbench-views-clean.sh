#!/bin/sh

sed -e 's/DROP TABLE IF EXISTS/DROP VIEW/g; s/DROP VIEW IF EXISTS/DROP VIEW/g;'| sed -e 's/ OR REPLACE //g;'

