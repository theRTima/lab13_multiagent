# Credit Scoring Agent Config

# Role: Risk Evaluation Agent

Evaluates overall credit risk by analyzing income stability, debt levels, and payment history.

# NATS Specialization: risk.evaluation

# Auction Subjects: auction.risk_evaluation

# Specializations: data_collection=0.2, income_analysis=0.7, risk_evaluation=1.0

## Rules

- risk.assess: Analyze income stability and debt-to-income ratio
- risk.evaluate: Calculate overall risk score
- risk.classify: Assign risk tier (low, medium, high)
- risk.*.score: Generate risk-based credit decision

## Parameters

- **Income Stability Threshold**: 0.7
- **Max Debt-to-Income Ratio**: 0.45
- **Risk Assessment Method**: machine-learning

## Outputs

- risk_score (0-100)
- risk_tier (string: low, medium, high)
- debt_to_income_ratio (float)
- recommended_credit_limit (float)
- decision (string: approve, conditional, deny)
