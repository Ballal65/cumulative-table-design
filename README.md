# What is a Cumulative Table?

A cumulative table preserves historical data by combining snapshots of data over time. Each iteration involves:

FULL OUTER JOIN: Combining today's and yesterday's datasets.

COALESCE: Retaining all values while merging unchanging dimensions.

Cumulation Metrics: Computing derived metrics such as days since an event.

Combining Arrays and Changing Values: Managing dynamic attributes like nested lists or status changes.

### Strengths

Facilitates historical analysis without requiring shuffle operations.

Enables state transition analysis.

### Drawbacks

Sequential backfilling is mandatory.

Handling sensitive data (e.g., deleted records) requires careful design.
