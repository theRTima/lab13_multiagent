# Credit Scoring Orchestrator (Python)

Asyncio-based orchestrator that manages the credit scoring pipeline by coordinating with Go agents via NATS.

## Pipeline

```
Data Collection → Income Analysis → Risk Evaluation
```

## How It Works

1. **Auction Phase**: Broadcasts auction to all agents capable of a task
2. **Bid Collection**: Waits 500ms for bids from agents (includes cost, load, capacity)
3. **Agent Selection**: Selects the lowest-cost bidder
4. **Task Execution**: Sends task to selected agent, waits for result
5. **Retry Logic**: Retries up to 3 times on timeout or failure
6. **Pipeline Flow**: Passes results through stages sequentially

## Quick Start

### Setup

```bash
cd python
pip install -r requirements.txt
```

### Run Orchestrator

```bash
# Make sure NATS server is running
python orchestrator.py
```

### Expected Output

```
[2026-05-22 18:50:00] INFO: Connected to NATS: nats://localhost:4222
############################################################
Processing application: APP001
############################################################

[Data Collection] Starting auction on auction.data_collection
[Data Collection] Received bid from Data Collection Agent: cost=1.00, load=0/100
[Data Collection] Selected agent: Data Collection Agent (cost: 1.00)
[Data Collection] Sending task to Data Collection Agent on data.collection
[Data Collection] Task completed: done
...
```

## Architecture

### Orchestrator Flow

```python
CreditScoringOrchestrator
├── process_application()      # Entry point for applicant
│   ├── execute_stage()        # Execute each pipeline stage
│   │   ├── run_auction()      # Broadcast + collect bids
│   │   ├── select bidder      # Pick lowest cost
│   │   └── send_task()        # Execute on selected agent
│   └── retry on timeout       # Up to 3 attempts
```

### Stages

Each stage has:
- **Auction Subject**: Where agents bid (e.g., `auction.data_collection`)
- **Task Subject**: Where tasks are sent (e.g., `data.collection`)
- **Timeout**: 500ms for auction, 5s for task execution
- **Max Retries**: 3 attempts

## Message Flow

### Auction (Broadcast)
```json
→ Subject: auction.data_collection
→ Message: {
    "task_type": "data_collection",
    "trace_id": "trace-abc123"
  }
← Agents reply with Bid on reply-to inbox
```

### Task (Request/Reply)
```json
→ Subject: data.collection
→ Message: {
    "type": "data_collection.process",
    "data": {...applicant_data...},
    "trace_id": "trace-xyz789"
  }
← Agent replies with Result on reply-to subject
```

## Customization

### Add New Pipeline Stage

Edit `CreditScoringOrchestrator.pipeline_stages`:

```python
PipelineStage(
    name="Your Stage Name",
    auction_subject="auction.your_stage",
    task_subject="your.stage.queue",
    auction_timeout_ms=500,
    max_retries=3,
)
```

### Adjust Timeouts

```python
# In PipelineStage:
auction_timeout_ms=1000  # Wait 1 second for bids
# Or in execute_stage:
timeout=10  # Task execution timeout
```

## Monitoring

Logs include:
- Auction broadcasts
- Bids received (agent, cost, load)
- Agent selection
- Task execution status
- Retries and failures
- Final application results

## Integration with Go Agents

The orchestrator works with any Go agent that:
1. Listens on the designated `task_subject` queue
2. Bids on the `auction_subject` when broadcasted
3. Returns results in JSON format with `trace_id`

See `income-analyzer-config.md`, `data-collection-config.md`, and `risk-evaluation-config.md` for agent configuration examples.
