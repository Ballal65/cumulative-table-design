# What is a Cumulative Table?

A cumulative table preserves historical data by combining snapshots of data over time. Each iteration involves:

1. FULL OUTER JOIN: Combining today's and yesterday's datasets.

2. COALESCE: Retaining all values while merging unchanging dimensions.

3. Cumulation Metrics: Computing-derived metrics such as days since an event.

4. Combining Arrays and Changing Values: Managing dynamic attributes like nested lists or status changes.

### Strengths

- Facilitates historical analysis without requiring shuffle operations.

- Enables state transition analysis.

- Everything is always sorted. When we use spark, it will help us avoid shuffling. 

### Drawbacks

Sequential backfilling is mandatory.

Handling sensitive data (e.g., deleted records) requires careful design.

# Key concepts to understand

## Temporal Cardinality Explosion

Adding temporal aspects significantly increases the size and complexity of the data. For instance, tracking player statistics daily would multiply the dataset size by the number of days. Proper use of nested structures like arrays helps manage this explosion efficiently.

## Run-Length Encoding Compression

This technique compresses repeated data, making formats like Parquet optimal for cumulative tables. However, shuffles in distributed systems (e.g., Spark) can disrupt compression, so JOINs and GROUP BYs must be carefully handled.
