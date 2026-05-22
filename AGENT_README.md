# Universal Credit Scoring Agent

Lightweight Go agent framework that reads Markdown configs and processes credit scoring workflows via NATS.

## Quick Start

```bash
# Build
go build -o agent .

# Run with default config (agent connects to nats://localhost:4222)
./agent -config income-analyzer-config.md

# Run with custom NATS server
./agent -config income-analyzer-config.md -nats nats://nats-server:4222
```

## Markdown Config Format

```markdown
# Role: [Agent Name]

[Description]

# NATS Specialization: [queue_name]

## Rules

- pattern: action description
- regex.pattern*: action for matching patterns
```

**Key sections:**
- `# Role:` — Agent's identity and purpose
- `# NATS Specialization:` — Queue name to listen on
- `## Rules` — Bulletpoints defining message patterns and actions

## How It Works

1. **Load Config** — Parses Markdown for Role, Specialization, and Rules
2. **Connect** — Establishes NATS connection
3. **Subscribe** — Joins queue named after Specialization
4. **Process** — Matches incoming messages against Rules
5. **Reply** — Sends response back to requester

## Message Format

**Request:**
```json
{
  "type": "income.validate",
  "data": {
    "applicant_id": "APP001",
    "annual_income": 75000,
    "documents": ["w2", "paystub"]
  },
  "trace_id": "trace-123"
}
```

**Response:**
```json
{
  "result": {
    "action": "Verify income documentation and amounts",
    "status": "processed",
    "input": {...}
  },
  "trace_id": "trace-123"
}
```

## Creating Custom Agents

Copy `income-analyzer-config.md` and modify:

1. Role name
2. Description  
3. NATS Specialization queue
4. Add/modify Rules to match your domain

Then run:
```bash
./agent -config your-agent-config.md
```

## Architecture Notes

- **Lightweight** — ~200 lines, minimal dependencies
- **Stateless** — No persistent state between messages
- **Observable** — Log output includes Role, message type, trace IDs
- **Extensible** — Rule matching uses regex patterns
- **Request/Reply** — Built-in NATS reply handling for synchronous workflows
