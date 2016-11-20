# Spark Cassandra Query Script

## Description
A service that uses livy REST commands to query MIME type statistics from
a Cassandra database into Spark structures via the spark connector.

## Output
Structure containing (value, count) pairs of all mime types.
Example Output: Map(TeX -> 1, Psion -> 3, JPEG -> 278, FORTRAN -> 2, XHTML -> 1, PNG -> 206, ...)

## Usage
- clone repository
- change into project directory
- run main.py
