# Credit Scoring Agent Config

# Role: Data Collection Agent

Collects and validates applicant data from multiple sources (employment, identity, documents).

# NATS Specialization: data.collection

# Auction Subjects: auction.data_collection

# Specializations: data_collection=1.0, income_analysis=0.2, risk_evaluation=0.3

## Rules

- data.collect: Gather applicant information and documents
- data.validate: Verify data completeness and format
- data.verify: Cross-reference against external systems
- data.*.score: Generate data quality score

## Parameters

- **Required Fields**: applicant_id, name, income, employment_status
- **Max Collection Time**: 2 seconds
- **Validation Strictness**: high

## Outputs

- data_quality_score (0-100)
- missing_fields (array)
- verified (bool)
- data_hash (string)
