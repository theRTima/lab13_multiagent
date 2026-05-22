# Stateful Agent Config

# Role: Stateful Analytics Agent

Maintains persistent state across restarts using Redis. Tracks counters, statistics, and cached results for analytics operations.

# NATS Specialization: stateful.analytics

# Auction Subjects: auction.analytics, auction.reporting

## Rules

- analytics.count: Increment counter for event tracking
- analytics.aggregate: Compute and cache aggregate statistics
- analytics.report: Generate reports using cached data
- analytics.*.query: Query cached analytics results

## State Persistence

This agent maintains the following state in Redis:
- **Counters**: Event counters (e.g., requests_processed, errors_encountered)
- **Stats**: Running statistics (e.g., average_response_time, success_rate)
- **Cache**: Cached computation results for faster responses

## Parameters

- **State Key Pattern**: agent:{role}:state
- **Auto-save**: On shutdown (SIGTERM, SIGINT)
- **Auto-restore**: On startup from Redis
- **Persistence**: No expiration (permanent until manually cleared)

## Outputs

- counter_value (int64)
- statistic_value (float64)
- cached_result (object)
- state_info (object with counters, stats, cache sizes)
