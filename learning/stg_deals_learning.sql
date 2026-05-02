/*
  stg_deals — Plain English Explanation
  ======================================

  stg_deals is the staging model for the raw table src_deals
  (which simulates a Findur deal extract).

  What it does, step by step:

    1. Reads from the raw source table using dbt's source() helper —
       that's how it knows where to look.

    2. Converts date columns (trade, value, maturity) from text strings
       into a real DATE type the database can do date math on.

    3. Converts timestamp columns (created, last modified) from text
       strings into DATETIME2 — date plus time of day with sub-second
       precision.

    4. Converts the deposit amount from text into a precise decimal
       number (DECIMAL(20, 2) — 18 digits before the dot, 2 after,
       fits any plausible BIS deposit including JPY mega-tickets).

    5. Converts the interest rate from text into a precise decimal
       kept in percentage form (e.g. 2.50 means 2.50%, not 0.025).

    6. Adds a new derived column called rate_decimal which is the same
       rate divided by 100, so it's in decimal form
       (e.g. 2.50% -> 0.025). Used by downstream models when interest
       is actually calculated.

    7. Does nothing else. No filtering of rows, no aggregations,
       no joins. Staging is *only* about making the raw data type-safe
       for everything downstream.

  Materialisation: SQL view. The output isn't a stored table — it's a
  saved query that recomputes every time something asks for it. No
  physical data, just an alias pointing at the source plus the casts.

  Grain: one row per deal. Every row in the output represents exactly
  one deal, identified uniquely by deal_id. Same number of rows in
  as out.
*/


/*
  CTE — Common Table Expression
  =============================
  A named temporary result set that exists only inside a single SQL query.

  You define it once at the top of the query (using the WITH keyword),
  give it a name, and from that point on you can refer to it by name as
  if it were a real table — but only within that same query. When the
  query finishes running, the CTE disappears. Nothing is stored.

  CTEs let you break a complex query into named, readable steps instead
  of writing one giant tangled SELECT.

  Analogy: a CTE is like a sticky note you write while solving a long
  math problem. You jot down an intermediate result, label it "subtotal,"
  then use "subtotal" in the next step. When the problem is done, you
  throw the sticky note away. The final answer stays; the sticky note doesn't.
*/


WITH 

-- 1st named CTE = "input"
input AS ( /*For this SQL query, create a temporary named result or CTE called input.
                  CTE Common Table Expression */

    select * from {{ source('bis_raw', 'src_deals') }}
    /*Inside that temporary block, select (* = every column) from the raw
    source table src_deals, which belongs to the dbt core source group bis_raw.
    aka
    Go to the raw source table named src_deals.
    that lives in the "bis_raw" MS Fabric Lakehouse (Cloud database).
    */
         ),

/*
Then this CTE pulls every column
"staged AS( )" is a 2nd CTE, or 2nd Common Table Expression
*/

-- 2nd CTE = "staged"
staged AS ( --Create another temporary query step or CTE called "staged".
/*
Now I am starting the cleaned version of the raw source data.
Aka "Run the SQL inside the parentheses and call the result "staged",
so I can refer to it by that name later in this query."

Aka Now define another step called "staged". Take everything from the step "input",
apply all the type casts and the "rate_decimal" derivation.
The result is the type-safe version of the data.
*/

    select -- give me back these Columns from the data

        -- identifiers column
        -- give me back the column "deal_id" from the data "as-is"
        deal_id,

        -- product / instrument column
        -- give me back the column "deal_type" from the data "as-is"
        deal_type,

        -- counterparty column
        /*
        give me back the column "cpty_code AND cpty_name AND cpty_country"
         from the data "as is"
         */
        cpty_code,

        -- counterparty column
        cpty_name,

        -- counterparty column
        cpty_country,
        /*
        Because SELECT is a complete list: anything you don't put in it gets dropped
        from the output.
        The staged CTE's job is to produce the full type-safe deal record
        — every column, not just the ones that needed casting.
        */

        -- date columns: cast from string YYYY-MM-DD to DATE
        /*
        give me back these 3 columns, such as "trade_date" AS "trade_date" (same name)
        but from "string" data-type to "date" data-type
        */
        try_cast(trade_date    as date) as trade_date,
        -- date column: cast from string YYYY-MM-DD to DATE
        try_cast(value_date    as date) as value_date,
        -- date column: cast from string YYYY-MM-DD to DATE
        try_cast(maturity_date as date) as maturity_date,
        /*
        Whenever we see as <name> at the end of an expression,
        that's just the alias, aka the label the result wears when coming out of the SELECT clause.
        Sometimes it's the same name as the input (like here),
        sometimes it's a new name (we will see this on derived columns like the "rate_decimal"
        column)
        */

        -- currency column
        currency, -- pass through aka keep the column "as-is"

        -- amounts column
        try_cast(nominal_amount as decimal(20, 2))    as nominal_amount, -- cast: string → decimal
        /*
        DECIMAL(20, 2) — for nominal_amount
        A number with 20 digits, 2 of them after the dot
        Why scale = 2
        Currency convention. Cents, centimes, pence, fen
        Why precision = 20
        - BIS holds ~SDR 314 billion average across ~700 active deposits → typical ticket ≈ SDR 450 million
        - Some single deposits will be much larger — say SDR 5 billion at the top end
        - Convert that to JPY (the largest-denomination major currency BIS holds):
          5 billion SDR ≈ 1 trillion JPY = 1,000,000,000,000.00 → 13 digits before the decimal
        */

        -- interest rate column: keep percentage form AND derive decimal form
        try_cast(rate_pct as decimal(10, 6))          as rate_pct,     -- cast: string → decimal (% form)
        /*
        Turn the rate from TEXT into a precise DECIMAL NUMBER (so we can do interest math on it).
        Use try_cast (not cast) so one bad value returns NULL instead of crashing the whole pipeline run.
        */

        -- interest rate column: keep percentage form AND derive decimal form
        try_cast(rate_pct as decimal(10, 6)) / 100.0  as rate_decimal, -- derived: divide by 100 for decimal form
        /*
        DECIMAL(10, 6) for rate_pct: an interest-rate column with sub-basis-point precision so accumulated
        rounding never quietly distorts the interest paid on large deposits.
        */
        /*
        Why scale = 6 (sub-basis-point precision)
        ==========================================
        BIS deposits are huge (billions of SDR). Tiny precision changes in
        the rate move real money:

        Rate stored as   |  Interest on SDR 1B  |  Difference vs 5.00%
        -----------------|----------------------|----------------------
        5.00%            |  SDR 50,000,000      |  ---
        5.01%            |  SDR 50,100,000      |  + SDR 100,000
        5.0001%          |  SDR 50,010,000      |  + SDR 10,000
        5.000001%        |  SDR 50,001,000      |  + SDR 1,000

        Reports show 2-4 decimals. Storage uses 6. The extra digits keep
        rounding errors invisible when interest is calculated across
        hundreds of deposits over multiple years.
        */

        -- deal lifecycle column
        deal_status, -- pass through aka keep the column "as-is"

        -- internal classification column
        portfolio, -- pass through aka keep the column "as-is"
        -- internal classification column
        book,      -- pass through aka keep the column "as-is"

        -- audit timestamps column: cast from string to DATETIME2 (date + time of day, sub-second precision)
        try_cast(created_dt       as datetime2) as created_dt,
          -- Convert created_dt from text to a DATETIME2 (date + time), same name, NULL on failure.

        -- audit timestamps column: cast from string to DATETIME2 (date + time of day, sub-second precision)
        try_cast(last_modified_dt as datetime2) as last_modified_dt
          -- Convert last_modified_dt from text to a DATETIME2 (date + time), same name, NULL on failure.

    from input -- tells the staged CTE where its rows come from (the input CTE defined earlier).

)-- closes the staged CTE definition. Everything inside the parentheses is what staged contains.

select * from staged --the actual query the model returns: "give me every column from staged."
-- This is what dbt materialises into the view dbo.stg_deals

/*
  How the file becomes a view in Fabric
  ======================================

  The last line  select * from staged  is the query dbt saves as the
  view in Fabric.

    - The view's name comes from the FILENAME, not from the CTE:
      stg_deals.sql  ->  dbo.stg_deals

    - The CTE name "staged" is just scratch paper inside the file.
      It disappears when the query finishes.

  Start:  physical table  bis_raw.dbo.src_deals
          (real data in the Lakehouse)

  End:    virtual view    bis_raw.dbo.stg_deals
          (saved query, no data; type-cast version of the input,
          recomputed on demand)

  CTE  = scratch paper.
  View = the actual deliverable.
*/

-- RECAP
/*
  Recap: input -> staged column mapping  (stg_deals.sql)
  ======================================================

  #   | Input column       | Input type  | Staged column      | Staged type           | Change
  ----|--------------------|-------------|--------------------|-----------------------|--------------
  1   | deal_id            | VARCHAR     | deal_id            | VARCHAR               | Pass-through
  2   | deal_type          | VARCHAR     | deal_type          | VARCHAR               | Pass-through
  3   | cpty_code          | VARCHAR     | cpty_code          | VARCHAR               | Pass-through
  4   | cpty_name          | VARCHAR     | cpty_name          | VARCHAR               | Pass-through
  5   | cpty_country       | VARCHAR     | cpty_country       | VARCHAR               | Pass-through
  6   | trade_date         | VARCHAR     | trade_date         | DATE                  | Cast (text -> date)
  7   | value_date         | VARCHAR     | value_date         | DATE                  | Cast (text -> date)
  8   | maturity_date      | VARCHAR     | maturity_date      | DATE                  | Cast (text -> date)
  9   | currency           | VARCHAR     | currency           | VARCHAR               | Pass-through
  10  | nominal_amount     | FLOAT       | nominal_amount     | DECIMAL(20, 2)        | Cast (precision upgrade) **
  11  | rate_pct           | FLOAT       | rate_pct           | DECIMAL(10, 6)        | Cast (precision upgrade) **
  12  | (derived)          | -           | rate_decimal       | DECIMAL ~ (16, 11) *  | Derived
  13  | deal_status        | VARCHAR     | deal_status        | VARCHAR               | Pass-through
  14  | portfolio          | VARCHAR     | portfolio          | VARCHAR               | Pass-through
  15  | book               | VARCHAR     | book               | VARCHAR               | Pass-through
  16  | created_dt         | VARCHAR     | created_dt         | DATETIME2             | Cast (text -> timestamp)
  17  | last_modified_dt   | VARCHAR     | last_modified_dt   | DATETIME2             | Cast (text -> timestamp)

  Counts:
    10 Pass-through
     6 Cast
     1 Derived
    17 staged columns from 16 source columns

  * rate_decimal = try_cast(rate_pct as decimal(10,6)) / 100.0
    The try_cast first converts rate_pct from FLOAT to DECIMAL(10,6).
    Then dividing by 100.0 triggers SQL Server's division promotion rules
    and the result lands at roughly DECIMAL(16, 11).
    To lock it to a specific shape, wrap the expression in another
    cast(... as decimal(X, Y)).

  ** Why nominal_amount and rate_pct are FLOAT in raw, not VARCHAR:
     The PySpark loader uses spark.read.csv(...).option("inferSchema", "true").
     Spark's CSV schema inference reliably detects numeric columns (int, double)
     and writes them to Delta as FLOAT in the Lakehouse SQL endpoint.
     But Spark does NOT infer dates or timestamps by default (no dateFormat or
     timestampFormat option is set), so date- and timestamp-shaped text stays
     as STRING -> VARCHAR.

     Consequence: the casts on rows 10 and 11 are NOT text -> number rescues.
     They are FLOAT -> DECIMAL precision upgrades. FLOAT is binary IEEE-754
     and accumulates rounding errors; DECIMAL is exact base-10. Money math
     belongs in DECIMAL, even when the raw layer already has a numeric type.

     The casts on rows 6, 7, 8, 16, 17 ARE text -> typed rescues — necessary
     type conversions, not precision upgrades.
*/

/*
  Three things, easy to confuse
  ==============================

  Thing | Is it an object? | Does it store data?      | Lifetime
  ------|------------------|--------------------------|-----------------------------
  Table | Yes              | Yes (data lives in it)   | Persistent until dropped
  View  | Yes              | No  (just a saved query) | Persistent until dropped
  CTE   | No               | No                       | Lives only inside one query
*/

/*
  Concepts taught by stg_deals.sql
  =================================

  CTEs and TRY_CAST are central (not CAST — the file uses TRY_CAST
  exclusively for the defensive null-on-failure behavior).

  Seven concepts in total, in order of importance:

    1. CTEs  (WITH ... AS (...))
       -> The "input" and "staged" blocks.

    2. TRY_CAST  (and why over CAST)
       -> Every type conversion in "staged".
       -> TRY_CAST returns NULL on failure; CAST aborts the whole model.

    3. Data type literacy
       (DATE, DATETIME2, DECIMAL precision/scale, VARCHAR)
       -> The type chosen for each column, and the reasoning
          behind precision/scale.

    4. dbt source() macro
       -> {{ source('bis_raw', 'src_deals') }}
       -> Declared in sources.yml, resolved at compile time.

    5. Column aliasing  (as col_name)
       -> After every cast, plus the derived rate_decimal.

    6. Derived columns
       -> rate_decimal = rate_pct / 100.0
       -> A new column that doesn't exist upstream.

    7. Staging-layer discipline
       -> No joins, no filtering, no aggregation.
       -> The PHILOSOPHY of what staging IS.
*/