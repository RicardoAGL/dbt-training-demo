-- Ex 6.1: Doc blocks
-- Define reusable documentation in markdown files
-- Reference with {{ doc("block_name") }} in YAML descriptions

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
