version: 2

models:
  - name: fact_sale
    description: >
      Transaction fact table. One row per line item ordered. FudgeFlix rows
      represent billing events from ff_account_billing. FudgeMart rows represent
      order line items joined from fm_orders, fm_order_details, and fm_products.
      Assumptions: FudgeFlix unit cost is 50% of billed amount; FudgeFlix
      quantity is always 1; division fully determines sales channel; payment
      info only exists for FudgeMart.
    columns:
      - name: order_id
        description: >
          Degenerate dimension. Source key used for drill-through.
          ab_id from FudgeFlix, order_id from FudgeMart.
        tests:
          - not_null

      - name: customer_key
        description: Foreign key to dim_customer.customer_key
        tests:
          - not_null

      - name: date_key
        description: Foreign key to dim_date.date_key in YYYYMMDD format. Derived from ab_date (FudgeFlix) or order_date (FudgeMart)
        tests:
          - not_null

      - name: product_key
        description: Foreign key to dim_product.product_key
        tests:
          - not_null

      - name: payment_method_key
        description: >
          Foreign key to dim_payment_method.payment_method_key.
          Resolves to 'Not Applicable' placeholder row for FudgeFlix.

      - name: channel_key
        description: Foreign key to dim_sales_channel.channel_key. 1 for FudgeMart, 2 for FudgeFlix
        tests:
          - not_null

      - name: order_quantity
        description: Quantity of product in the order. Always 1 for FudgeFlix
        tests:
          - not_null

      - name: unit_selling_price
        description: Selling price per unit. ab_billed_amount for FudgeFlix, product_retail_price for FudgeMart

      - name: unit_cost_price
        description: Cost price per unit. 50% of ab_billed_amount for FudgeFlix, product_wholesale_price for FudgeMart

      - name: sold_amount
        description: Additive fact. Total selling amount (quantity * unit_selling_price)

      - name: cost_amount
        description: Additive fact. Total cost amount (quantity * unit_cost_price)
        tests:
          - not_null

      - name: order_profit
        description: Additive fact. sold_amount minus cost_amount
        tests:
          - not_null

      - name: division
        description: Source system identifier. Either 'FudgeFlix' or 'FudgeMart'
        tests:
          - not_null