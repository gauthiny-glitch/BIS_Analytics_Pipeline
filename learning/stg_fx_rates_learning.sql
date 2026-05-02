/*
  stg_fx_rates
  ============
  Cleans up the raw FX rates table so the rest of the project can trust it.

  What it does:
    1. Reads the raw table src_fx_rates
    2. Turns the rate_date column from text into a real date
    3. Leaves currency_code and sdr_per_unit alone (they are already clean)
    4. Does not drop, filter, or sum any rows -- only fixes types

  How it is stored:
    As a view in Fabric. A view stores no data of its own, it just runs the
    query each time something asks for it.

  One row per:
    (rate_date, currency_code) pair. The same currency repeats every month,
    and every month has many currencies, so it takes BOTH columns together
    to point at a single row.
*/

WITH 

-- 1st named CTE = "CTE1_input"
CTE1_input as ( /* with as () means create CTE aka a temporary table that will
contains the below selected columns */
    select *   
    from {{ source('bis_raw', 'src_fx_rates') }} 
    /*
    GRAB every Column AND every Row 
    FROM the raw FX rates table "src_fx_rates" 
    that lives in the "bis_raw" MS Fabric Lakehouse (Cloud database).
    */
          ),


-- 2nd named CTE = "CTE2_staged"
CTE2_staged as ( /*create the 2nd CTE (temporary table) that will that will
get its inputs from the 1st CTE named "CTE1_input"
*/
    select /* Give me back the below Columns named header 1, header 2, heard 3, ...etc
              FROM the returned results of the 1st CTE named CTE1_input  */

        -- date: cast from string YYYY-MM-DD to DATE
        try_cast(rate_date as date) as rate_date,
        /* take the CTE1 column "rate_date" (currently text like "2024-03-31"),
        convert it to a real DATE, and keep the same name.
        try_cast (vs cast) returns NULL on failure instead of crashing the model. */

        -- currency identifier
        currency_code, -- bring me the returned CTE1-column named "currency_code" as-is.

        -- SDR rate: 1 unit of currency_code = sdr_per_unit SDR
        sdr_per_unit -- bring me the returned CTE1-column named "sdr_per_unit".
        
        /*since it is the last column, not need to finish with a coma,
          when preceding the FROM clause
        */
    from CTE1_input /*from the CTE named CTE1_input that was the returned results from  
        ('bis_raw', 'src_fx_rates')  
        */
                )

select * from CTE2_staged -- give me every column FROM the CTE named "CTE2_staged"
--This is what dbt materializes into the VIEW named after the sql-file-name, "dbo.stg_fx_rates"


/*
  Data flow for stg_fx_rates
  ==========================

  Lakehouse Delta Table (real data on disk)
     |
     v
  1st CTE  "CT1_input"   <- exists only while the query runs, vanishes after
     |
     v
  2nd CTE  "CTE2_staged"  <- exists only while the query runs, vanishes after
     |
     v
  SQL View  "stg_fx_rates"  <- saved as an object in the SAME Lakehouse,
                              but stores ZERO data. It is just a saved
                              query that re-runs against the Delta Table
                              every time someone (Power BI, you, another
                              model) asks it for rows.

  Two things to nail down:

  1. The two CTEs are temporary. They are not objects, not stored, not
     visible outside this one query. They evaporate the second the query
     finishes. Sticky notes during the run, then thrown away.

  2. "Back into the Lakehouse" is correct in this setup specifically.
     profiles.yml has database: bis_raw, so dbt writes the views back
     into the same Lakehouse it reads from. A cleaner setup would have
     a separate bis_analytics Lakehouse for outputs. Works fine, just
     worth knowing.

  Round trip:
     Delta Table (data)
       -> ephemeral CTEs (no data)
       -> View (no data, just a saved query)
       -> next query bounces straight back to the Delta Table for fresh rows.
*/

/*
  Recap: input -> staged column mapping  (stg_fx_rates.sql)
  =========================================================

  #  | Input column   | Input type | Staged column  | Staged type | Change
  ---|----------------|------------|----------------|-------------|--------------
  1  | rate_date      | VARCHAR    | rate_date      | DATE        | Cast
  2  | currency_code  | VARCHAR    | currency_code  | VARCHAR     | Pass-through
  3  | sdr_per_unit   | FLOAT      | sdr_per_unit   | FLOAT       | Pass-through

  Counts:
    2 Pass-through
    1 Cast
    0 Derived
    3 staged columns from 3 source columns

  Source loader: PySpark, spark.read.csv(...) with inferSchema=true.
  Spark inferred sdr_per_unit as DOUBLE -> Fabric Delta wrote it as FLOAT.
  Spark left date-shaped text as STRING (no dateFormat option set) -> the
  rate_date cast in this staging file is doing the text -> DATE conversion.
*/
