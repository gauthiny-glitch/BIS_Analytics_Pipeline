/*
  dim_product
  ===========
  Lookup table for the 4 BIS Banking Department deposit products.
  One row per product.

  Hardcoded because there are only 4 of them and they are fixed by
  BIS internal instrument conventions, so they will not change deal
  by deal.

  The "deal_type" codes follow the Findur naming convention
  INSTRUMENT_DEP, where DEP stands for Deposit.

  [CONFIRMED] Product types: FIXBIS, MTI, Sight, Notice -- BIS Annual Report
  [CONFIRMED] All four are currency deposits (borrowed funds) -- BIS Annual Report

  How it is stored: as a view in Fabric. A view stores no data of its own,
  it just runs the query each time something asks for it.
*/

/*
  dbt config block. Tells dbt how to store this model when it runs.

  - {{ }} are Jinja markers. Anything between them is an instruction
    for dbt, not SQL sent to Fabric.
  - config(...) is the dbt instruction.
  - materialized = 'view' means: store the output as a view in Fabric.
    A view holds no data of its own; the query re-runs on every call.

  Optional. If omitted, dbt uses the project default from dbt_project.yml.
  Valid values: 'view', 'table', 'ephemeral', 'incremental'.
  On the Fabric SQL Endpoint only 'view' and 'ephemeral' actually work.
*/
{{
    config(
        materialized = 'view'
    )
}}

/*
  Opens a CTE named 'products'.
  - with        starts the CTE block.
  - products    is the alias we will reference later in 'select * from products'.
  - as (        opens the body. Everything inside the parentheses defines
                what 'products' contains.
  CTEs only exist for the duration of this query. Nothing is stored in Fabric.
*/
with products as (

    select *     -- Grab every column from the wrapper sub-query below.
                 -- The '*' is shorthand for "all columns the source provides".
    
    /* - Where the SELECT above reads from. The open paren opens an inline
       - sub-query instead of naming a real table -- needed because T-SQL
       - 'values' cannot stand alone.*/
    from (

        /*
        The 4 deposit products. Each parenthesised tuple is one row;
        three positions per tuple = three columns. No column names
        are attached yet -- they get assigned by position on the
        'as t (deal_type, product_name, product_description)' line
        below.

        Position 1: Findur "deal_type" code (INSTRUMENT_DEP convention)
        Position 2: short "product_name" (Power BI label)
        Position 3: plain-English "product_description" (interview / docs use)

        The list is a contract. Every distinct "deal_type" value in
        the fact table must appear here exactly once, or Power BI
        relationships will develop orphan rows.
        */
        values
            ('FIXBIS_DEP', 'FIXBIS',  'Fixed-rate BIS deposit. Fixed term, fixed interest rate. Most common product.'),
            ('MTI_DEP',    'MTI',     'Medium-term instrument. Longer tenor fixed-rate deposit, typically 1-10 years.'),
            ('SIGHT_DEP',  'Sight',   'Sight account. Overnight / on-demand deposit. No fixed maturity.'),
            ('NOTICE_DEP', 'Notice',  'Notice account. Deposit redeemable with advance notice period.')

    /*
      The line that names the columns.
      - )                 closes the wrapper sub-query (matches 'from (' earlier).
      - as t              gives the sub-query a throwaway alias 't'. T-SQL
                          requires every sub-query to have an alias, but
                          we never reference 't' again.
      - (col, col, col)   the column list, assigning names by position.

      Naked 'values' rows are positional only -- the rows above have no
      column names until this line runs. Position 1 -> "deal_type",
      Position 2 -> "product_name", Position 3 -> "product_description".

      WARNING: swap two names here and every row's data silently lands in
      the wrong column. No error, no warning.
    */
    ) as t (deal_type, product_name, product_description)

/*Closes the 'products' CTE body, matching the 'with products as ('
  opener at the top.*/
)

/*
  The model's actual output query. Everything above is scaffolding that
  disappears when the query ends -- only this final SELECT survives.

  dbt wraps this line in:
      CREATE VIEW bis_analytics.dbo.dim_product AS
          select * from products

  The view name comes from the FILENAME ('dim_product.sql' ->
  'bis_analytics.dbo.dim_product'), not from the CTE name 'products'.

  'select *' is safe here because the columns were hand-typed in the
  'as t (deal_type, product_name, product_description)' line above. No
  upstream model can sneak a new column in.
*/
select * from products


/*
  ============================================================
  OUTPUT VIEW: 'bis_analytics.dbo.dim_product'
  ============================================================
  4 rows x 3 columns. One row per BIS Banking Department deposit product.

  Columns (types inferred by Fabric from the VALUES literal -- verify
  with INFORMATION_SCHEMA.COLUMNS)
  ------------------------------------------------------------
  "deal_type"            VARCHAR(10)*  PK. Findur INSTRUMENT_DEP code.
                                       Joins to 'fct_currency_deposits'."deal_type".
                                       Values: 'FIXBIS_DEP', 'MTI_DEP',
                                       'SIGHT_DEP', 'NOTICE_DEP'.

  "product_name"         VARCHAR(6)*   Short label for Power BI and interview
                                       talking points. Values: 'FIXBIS', 'MTI',
                                       'Sight', 'Notice'.

  "product_description"  VARCHAR(80)*  Plain-English one-liner. Used in tooltips,
                                       documentation, recruiter coffee chats.

  *Lengths sized to the longest literal in each column. Confirm with:
       SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
       FROM bis_analytics.INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_NAME = 'dim_product';

  Quick verification
  ------------------
       SELECT * FROM bis_analytics.dbo.dim_product;
       -- expected: 4 rows

       SELECT COUNT(DISTINCT deal_type) FROM bis_analytics.dbo.dim_product;
       -- expected: 4 (PK uniqueness check)

  Storage footprint
  -----------------
  Zero bytes on disk. 'dim_product' is a view; the 4 rows live inside the
  view definition itself. Every query that reads the view re-runs the
  underlying 'select * from products' against the inline 'values' literal.
  Sub-millisecond latency on 4 rows.
  ============================================================
*/

/*
  ACTUAL ROWS RETURNED BY: SELECT * FROM bis_analytics.dbo.dim_product;
  (4 rows)

  +------------+--------------+--------------------------------------------------------------------------------+
  | deal_type  | product_name | product_description                                                            |
  +------------+--------------+--------------------------------------------------------------------------------+
  | FIXBIS_DEP | FIXBIS       | Fixed-rate BIS deposit. Fixed term, fixed interest rate. Most common product.  |
  | MTI_DEP    | MTI          | Medium-term instrument. Longer tenor fixed-rate deposit, typically 1-10 years. |
  | SIGHT_DEP  | Sight        | Sight account. Overnight / on-demand deposit. No fixed maturity.               |
  | NOTICE_DEP | Notice       | Notice account. Deposit redeemable with advance notice period.                 |
  +------------+--------------+--------------------------------------------------------------------------------+
*/

