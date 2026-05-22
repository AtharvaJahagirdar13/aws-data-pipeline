

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.window import Window


INPUT_PATH  = "s3://csv-raw-data-cc/uploads/"
OUTPUT_PATH = "s3://csv-processed-data-cc/processed/"
# ──────────────────────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job   = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ──────────────────────────────────────────────────────────────────────────────
# STEP 1 — Read raw CSV
#   multiLine + quote/escape handles Product Name cells with commas
#   encoding latin1 handles the BOM and accented characters
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("STEP 1 — Reading raw CSV")
print("=" * 60)

df = (
    spark.read
         .option("header",    "true")
         .option("inferSchema", "false")   # read everything as string first
         .option("quote",     '"')
         .option("escape",    '"')
         .option("multiLine", "true")
         .option("encoding",  "latin1")
         .csv(INPUT_PATH)
)

print(f"  Raw row count : {df.count()}")
print(f"  Raw columns   : {df.columns}")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 2 — Clean column names
#   Strips BOM (ï»¿ / \ufeff), whitespace, and replaces non-alphanumeric
#   characters with underscores
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 2 — Cleaning column names")

def clean_col(name: str) -> str:
    name = name.strip()
    name = name.replace("\ufeff", "").replace("ï»¿", "")
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+",            "_", name).strip("_")
    return name

clean_names = [clean_col(c) for c in df.columns]
df = df.toDF(*clean_names)
print(f"  Cleaned columns: {df.columns}")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 3 — Validate required columns
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 3 — Validating required columns")

REQUIRED = ["Sales", "Profit", "Quantity", "Discount", "Order_Date", "Ship_Date"]
missing  = [c for c in REQUIRED if c not in df.columns]
if missing:
    raise Exception(f"Missing required columns: {missing}  |  Found: {df.columns}")
print("  All required columns present")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 4 — Remove exact duplicate rows
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 4 — Removing duplicate rows")

before = df.count()
df = df.dropDuplicates()
print(f"  Removed {before - df.count()} duplicates -> {df.count()} rows remaining")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 5 — Replace sentinel string nulls with SQL NULL
#   Spark's inferSchema=false leaves blanks as empty strings, not nulls.
#   We normalise common null-like strings across ALL columns.
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 5 — Replacing sentinel strings with NULL")

NULL_SENTINELS = ["", "NULL", "null", "N/A", "n/a", "NA", "na", "NaN", "nan"]

for col_name in df.columns:
    df = df.withColumn(
        col_name,
        F.when(F.col(col_name).isin(NULL_SENTINELS), F.lit(None).cast(StringType()))
         .otherwise(F.col(col_name))
    )

# Drop rows where any core numeric column is null
NUMERIC_COLS = ["Sales", "Profit", "Quantity", "Discount"]
before = df.count()
df = df.dropna(subset=NUMERIC_COLS)
print(f"  Dropped {before - df.count()} rows with nulls in numeric cols -> {df.count()} remaining")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 6 — Type casting
#   Cast to Double first; rows that fail cast to null are dropped.
#   Quantity is cast to Double here and converted to Int after Winsorisation
#   (clip() in Spark returns Double — casting to Int too early breaks clip).
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 6 — Type casting")

df = (
    df
    .withColumn("Sales",    F.col("Sales")   .cast(DoubleType()))
    .withColumn("Profit",   F.col("Profit")  .cast(DoubleType()))
    .withColumn("Discount", F.col("Discount").cast(DoubleType()))
    .withColumn("Quantity", F.col("Quantity").cast(DoubleType()))   # stays Double until after clip
)

before = df.count()
df = df.dropna(subset=NUMERIC_COLS)
print(f"  After type-cast null removal: {df.count()} rows (dropped {before - df.count()})")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 7 — Parse dates  ← THE CRITICAL FIX
#
#   This dataset has TWO mixed formats:
#     MM-dd-yyyy   e.g. "01-09-2023"  (dashes  — US month-first)
#     M/d/yyyy     e.g. "1/13/2023"   (slashes — US month-first)
#
#   The original Glue job used coalesce() and tried dd-MM-yyyy FIRST,
#   which silently parsed "01-09-2023" as 1 September instead of 9 January.
#   That caused ~1 700 rows to look like Ship < Order.
#
#   Fix: detect the separator (dash vs slash) BEFORE choosing the format.
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 7 — Parsing dates (MM-dd-yyyy for dashes, M/d/yyyy for slashes)")

def parse_date_col(df, col_name: str):
    """
    Parse a date column that contains a mix of:
      MM-dd-yyyy   (dash separator)
      M/d/yyyy     (slash separator)
    Returns the column as DateType.
    """
    has_dash  = F.col(col_name).contains("-")
    has_slash = F.col(col_name).contains("/")

    return df.withColumn(
        col_name,
        F.when(has_dash,  F.to_date(F.col(col_name), "MM-dd-yyyy"))
         .when(has_slash, F.to_date(F.col(col_name), "M/d/yyyy"))
         # fallback — catches any remaining formats
         .otherwise(
             F.coalesce(
                 F.to_date(F.col(col_name), "yyyy-MM-dd"),
                 F.to_date(F.col(col_name), "MM/dd/yyyy"),
             )
         )
    )

df = parse_date_col(df, "Order_Date")
df = parse_date_col(df, "Ship_Date")

# Report parse failures
order_fail = df.filter(F.col("Order_Date").isNull()).count()
ship_fail  = df.filter(F.col("Ship_Date").isNull()).count()
if order_fail: print(f"  WARNING: {order_fail} unparseable Order_Date values")
if ship_fail:  print(f"  WARNING: {ship_fail} unparseable Ship_Date values")

before = df.count()
df = df.dropna(subset=["Order_Date", "Ship_Date"])
print(f"  After dropping NaT dates: {df.count()} rows (dropped {before - df.count()})")

# Sanity check: Ship must be >= Order
bad_dates = df.filter(F.col("Ship_Date") < F.col("Order_Date")).count()
if bad_dates:
    print(f"  NOTE: {bad_dates} rows where Ship_Date < Order_Date — removing data errors")
    df = df.filter(F.col("Ship_Date") >= F.col("Order_Date"))
else:
    print("  Ship_Date >= Order_Date for all rows")

print(f"  Date parsing complete — {df.count()} rows")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 8 — Outlier handling — IQR Winsorisation (clip, not drop)
#
#   The original script used filter() to REMOVE rows outside IQR bounds.
#   That silently shrinks the dataset.  Winsorisation clips outlier values
#   to the boundary so no rows are lost.
#
#   approxQuantile(col, [0.25, 0.75], relError) is efficient in Spark.
#   We use relError=0.01 (1% error tolerance) which is fast and accurate
#   enough for business reporting.
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 8 — Outlier handling (IQR Winsorisation — clips, does not drop)")

def winsorise_spark(df, col_name: str, factor: float = 1.5):
    """Clip values in col_name to [Q1 - factor*IQR, Q3 + factor*IQR]."""
    Q1, Q3 = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
    IQR    = Q3 - Q1
    lower  = Q1 - factor * IQR
    upper  = Q3 + factor * IQR

    clipped_df = df.withColumn(col_name, F.greatest(F.lit(lower), F.least(F.lit(upper), F.col(col_name))))
    clipped    = clipped_df.filter(F.col(col_name) != df[col_name]).count()
    print(f"  {col_name:<15s}  bounds [{lower:10.2f}, {upper:10.2f}]  -> clipped ~{clipped} values")
    return clipped_df

for col_name in ["Sales", "Profit", "Quantity"]:
    df = winsorise_spark(df, col_name)

# Now it is safe to cast Quantity to Integer (clip is done)
df = df.withColumn("Quantity", F.col("Quantity").cast(IntegerType()))
print(f"  Rows after Winsorisation: {df.count()}")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 9 — Feature Engineering
#   All 11 features from the pandas ETL, implemented with Spark functions.
# ──────────────────────────────────────────────────────────────────────────────
print("\nSTEP 9 — Feature engineering")

df = (
    df
    # ── Time features ──────────────────────────────────────────────────────
    .withColumn("Order_Year",      F.year(F.col("Order_Date")))
    .withColumn("Order_Month",     F.month(F.col("Order_Date")))
    .withColumn("Order_Quarter",   F.quarter(F.col("Order_Date")))
    .withColumn("Order_DayOfWeek", F.date_format(F.col("Order_Date"), "EEEE"))

    # ── Delivery performance ───────────────────────────────────────────────
    .withColumn("Delivery_Days",   F.datediff(F.col("Ship_Date"), F.col("Order_Date")))

    # ── Profitability ratio ────────────────────────────────────────────────
    # F.nullif does NOT exist in PySpark — use F.when to guard divide-by-zero
    .withColumn(
        "Profit_Margin_Pct",
        F.round(
            F.col("Profit") / F.when(F.col("Sales") != 0, F.col("Sales")) * 100,
            2
        )
    )

    # ── Revenue band segmentation ──────────────────────────────────────────
    .withColumn(
        "Revenue_Band",
        F.when(F.col("Sales") <  100, "Low")
         .when(F.col("Sales") <  500, "Medium")
         .otherwise("High")
    )

    # ── Loss flag ──────────────────────────────────────────────────────────
    .withColumn("Is_Loss", F.col("Profit") < 0)

    # ── Discount band ──────────────────────────────────────────────────────
    .withColumn(
        "Discount_Band",
        F.when(F.col("Discount").isNull(),    "None")
         .when(F.col("Discount") == 0,        "None")
         .when(F.col("Discount") <= 0.2,      "Low")
         .otherwise("High")
    )

    # ── Per-unit metrics ───────────────────────────────────────────────────
    # F.nullif does NOT exist in PySpark — use F.when to guard divide-by-zero
    .withColumn(
        "Revenue_Per_Unit",
        F.round(F.col("Sales") / F.when(F.col("Quantity") != 0, F.col("Quantity").cast(DoubleType())), 2)
    )
    .withColumn(
        "Profit_Per_Unit",
        F.round(F.col("Profit") / F.when(F.col("Quantity") != 0, F.col("Quantity").cast(DoubleType())), 2)
    )
)

print("  Engineered features added:")
for feat in [
    "Order_Year", "Order_Month", "Order_Quarter", "Order_DayOfWeek",
    "Delivery_Days", "Profit_Margin_Pct", "Revenue_Band",
    "Is_Loss", "Discount_Band", "Revenue_Per_Unit", "Profit_Per_Unit",
]:
    print(f"    • {feat}")


# ──────────────────────────────────────────────────────────────────────────────
# STEP 10 — Validation report
#   All logging happens BEFORE coalesce() so Spark still has full parallelism.
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("STEP 10 — Final Validation Report")
print("=" * 60)

total_rows = df.count()
print(f"\n  Total rows    : {total_rows}")
print(f"  Total columns : {len(df.columns)}")
print(f"  All columns   : {df.columns}")

print("\n  Schema:")
df.printSchema()

print("\n  Core numeric summary:")
df.select("Sales", "Profit", "Quantity", "Discount",
          "Delivery_Days", "Profit_Margin_Pct").describe().show()

print("  Revenue Band distribution:")
df.groupBy("Revenue_Band").count().orderBy("Revenue_Band").show()

print("  Discount Band distribution:")
df.groupBy("Discount_Band").count().orderBy("Discount_Band").show()

print("  Is_Loss distribution:")
df.groupBy("Is_Loss").count().show()

print("  Year distribution:")
df.groupBy("Order_Year").count().orderBy("Order_Year").show()

print("  Category distribution:")
df.groupBy("Category").count().orderBy("Category").show()

# Null check on engineered features
print("  Null counts on engineered features:")
null_exprs = [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in [
    "Delivery_Days", "Profit_Margin_Pct", "Revenue_Band",
    "Discount_Band", "Revenue_Per_Unit", "Profit_Per_Unit"
]]
df.select(null_exprs).show()


# ──────────────────────────────────────────────────────────────────────────────
# STEP 11 — Save to S3
#   coalesce(1) is called HERE — AFTER all transforms and validation.
#   In the original script coalesce was called before feature engineering,
#   which caused the engineered columns to not appear in the output.
# ──────────────────────────────────────────────────────────────────────────────
print(f"\n" + "=" * 60)
print(f"STEP 11 — Writing output to {OUTPUT_PATH}")
print("=" * 60)

(
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("quote",  '"')
      .option("escape", '"')
      .csv(OUTPUT_PATH)
)

print(f"  Written {total_rows} rows, {len(df.columns)} columns")
print("\nETL Job Completed Successfully")

job.commit()
