# Credit Scoring Agent Config

# Role: Income Analyzer

Analyzes applicant income from employment, investments, and other sources to determine creditworthiness contribution.

# NATS Specialization: income.analysis

## Rules

- income.validate: Verify income documentation and amounts
- income.assess: Calculate total income and stability metrics
- income.verify: Cross-reference with employment records
- income.*.score: Generate income-based credit scoring component

## Parameters

- **Min Annual Income**: $0
- **Max Analysis Age**: 90 days
- **Required Documents**: W2, Pay Stubs, Tax Returns

## Outputs

- total_income (float)
- stability_score (0-100)
- verified (bool)
- risk_level (string: low, medium, high)
