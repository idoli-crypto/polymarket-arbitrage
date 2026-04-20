1. Purpose

The purpose of this system is to build a research and operational engine for detecting and evaluating Neg Risk arbitrage opportunities on Polymarket, while calculating net profit under real execution conditions.

At this stage, the system is not intended to execute real trades, but to empirically measure:

Whether a real executable edge exists
Whether it can be executed in practice
What the net profit is after costs
What the lifetime of opportunities is

The system is based on the core assumption:

Real economic arbitrage ≠ mathematical price deviation
It is a function of execution + fees + liquidity + resolution rules

2. Scope
In Scope (V1)
Connect to Polymarket API
Ingest real time market data
Store order book snapshots
Detect Neg Risk opportunities only
Calculate executable edge
Calculate fees, slippage, and depth
Perform semantic validation based on rules
Simulate execution
Compute KPI metrics
Provide monitoring dashboard
Out of Scope (V1)
Real trade execution
Real position management
Wallet integration
Cross platform arbitrage
3. System Architecture
Repository Structure

Modular monorepo structure:

apps/api
apps/worker
apps/web
packages/core
packages/data
packages/integrations
infra
docs
Core Components
1. Data Ingestion
Connect to Polymarket CLOB API
Use WebSocket for real time updates
Store order book data
2. Opportunity Engine
Detect Neg Risk opportunities only
Compute probability sums
Calculate gross edge
3. Execution Evaluator
Compute best executable price
Check order book depth
Calculate fees
Estimate slippage
Calculate net edge
4. Semantic Validation Engine
Validate relationships between markets
Analyze resolution rules
Prevent false arbitrage
5. KPI Engine

Based directly on:

Net Realized Arbitrage Return
Execution Success Rate
Real Executable Edge
Fill Completion Ratio
Time to Execution
Capital Lock Time
Slippage Cost
False Positive Rate
6. Web Dashboard
Opportunity display
KPI monitoring
System status
Market details
4. Data Flow

Unified pipeline:

ingest → normalize → detect → validate → simulate → measure → display

5. Data Layer

Core tables:

markets
market_snapshots
detected_opportunities
semantic_links
detector_runs
execution_simulations
kpi_daily

Strict separation between:

expected vs actual

6. Technology Stack
Backend: Python + FastAPI
Worker: Python
Database: PostgreSQL
ORM: SQLAlchemy
Web: React or Next.js
Deployment: Render
Version Control: GitHub
7. QA Strategy

QA is part of the system, not a final step.

Test Types
Unit Tests
Edge calculation
Fee calculation
Neg Risk math
Integration Tests
Polymarket API integration
Ingestion flow
Database writes
Simulation Validation
Ensure simulated execution reflects real market constraints
Data Integrity Checks
No missing snapshots
No market mismatches
No broken timestamps
KPI Validation
KPI calculations are correct
No logical inconsistencies
8. IT and Infrastructure
Environment
Render deployment
Managed PostgreSQL
Environment variables
Requirements
Health endpoints
Basic monitoring
Centralized logs
Retry mechanisms for ingestion
Future Scaling
Worker separation
Queue system
Horizontal scaling
9. Security

Minimum required even in V1:

1. Secrets Management
No API keys in code
Use environment variables
2. API Protection
Basic rate limiting
Input validation
3. Data Protection
Prevent data corruption
Validate before insert
4. Future Readiness

System must support future execution:

Separation between read and write
Isolation of execution module
10. Governance

All development must follow:

Issue
Spec
Branch
PR
Review
Merge

GitHub is the single source of truth.

11. Acceptance Criteria (V1)

The system is considered complete when:

Ingestion works in real time
Snapshots are stored
Neg Risk is detected
Net edge is calculated
Semantic validation works
Execution simulation runs
KPI is calculated
Dashboard displays data
12. Future Phases

Next phase:

Execution service
Order management
Risk engine
Capital allocation
File Location

This document must be located at:

docs/v1-system-architecture.md

Final Note

Many systems fail at this point:

They build detection
but do not build validation and execution realism

This document enforces working in the real world
not in a theoretical model

This is the difference between a research project
and a system that can generate real profit
