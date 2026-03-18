-- Ex 6.1: Doc blocks
-- Define reusable documentation in markdown files
-- Reference with {{ doc("block_name") }} in YAML descriptions
-- The special __overview__ block renders on the docs homepage

{% docs __overview__ %}

# Jaffle Shop - dbt Training Project

Welcome to the Jaffle Shop dbt project! This is a training environment used to learn dbt fundamentals.

## Data Sources

| Source | Description |
|--------|-------------|
| **jaffle_shop** | App database with customers and orders |
| **Stripe** | Payment processor with payment transactions |

## Project Layers

- **Staging** (`models/staging/`): One-to-one mapping with source tables. Clean up column names, cast types.
- **Marts** (`models/marts/`): Business-level models consumed by analysts and BI tools.

## Key Models

| Model | Description |
|-------|-------------|
| `stg_customers` | Staged customer data |
| `stg_orders` | Staged order data |
| `stg_payments` | Staged payment data (amount converted from cents to dollars) |
| `orders` | One record per order with total payment amount |
| `customers` | One record per customer with lifetime value |

## Useful Commands

```bash
dbt run                          # Build all models
dbt test                         # Run all tests
dbt build                        # Run models + tests in DAG order
dbt run --select +customers      # Build customers and all upstream
dbt source freshness             # Check source data freshness
dbt docs generate && dbt docs serve  # Generate and view this site
```

{% enddocs %}

{% docs order_status %}

One of the following values:

| status         | definition                                                 |
|----------------|------------------------------------------------------------|
| placed         | Order placed but not yet shipped                           |
| shipped        | Order has been shipped but hasn't yet been delivered       |
| completed      | Order has been received by customers                       |
| return_pending | Customer has indicated they would like to return this item |
| returned       | Item has been returned                                     |

{% enddocs %}

{% docs jaffle_shop %}

The Jaffle Shop is a fictional restaurant chain used for dbt training.
It has two data sources:
- **jaffle_shop** app database (customers, orders)
- **Stripe** payment processor (payments)

{% enddocs %}
