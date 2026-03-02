# Databricks notebook source
# Interview Case Study  PySpark / Databricks implementation
#
# This notebook implements the required pipeline:
#   1) Load CSVs into raw_* tables
#   2) Cast / clean data into store_* tables
#   3) Build publish_product
#   4) Build publish_orders
#   5) Answer the two analysis questions and persist the outputs
#

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


# ==============================================================================
# CONFIGURATION
# ==============================================================================
CATALOG = None                    
SCHEMA = "case_study"             
BASE_PATH = "/Volumes/workspace/default/test"

PATHS = {
    "products": f"{BASE_PATH}/products.csv",
    "sales_order_detail": f"{BASE_PATH}/sales_order_detail.csv",
    "sales_order_header": f"{BASE_PATH}/sales_order_header.csv",
}

TABLES = {
    "raw_products": "raw_products",
    "raw_sales_order_detail": "raw_sales_order_detail",
    "raw_sales_order_header": "raw_sales_order_header",
    "store_products": "store_products",
    "store_sales_order_detail": "store_sales_order_detail",
    "store_sales_order_header": "store_sales_order_header",
    "publish_product": "publish_product",
    "publish_orders": "publish_orders",
    "analysis_top_color_by_year": "analysis_top_color_by_year",
    "analysis_avg_leadtime_by_category": "analysis_avg_leadtime_by_category",
}

MONEY_TYPE = T.DecimalType(18, 4)
MEASURE_TYPE = T.DecimalType(18, 4)

SUBCATEGORY_CLOTHING = ["Gloves", "Shorts", "Socks", "Tights", "Vests"]
SUBCATEGORY_ACCESSORIES = ["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"]
SUBCATEGORY_COMPONENTS_EXACT = ["Wheels", "Saddles"]


# ==============================================================================
# UTILITIES
# ==============================================================================
def fq_table(table_name: str) -> str:
    """Return a fully qualified table name using the configured catalog/schema."""
    if CATALOG and SCHEMA:
        return f"{CATALOG}.{SCHEMA}.{table_name}"
    if SCHEMA:
        return f"{SCHEMA}.{table_name}"
    return table_name


def initialize_schema() -> None:
    """Create and select the target schema."""
    if CATALOG:
        spark.sql(f"USE CATALOG {CATALOG}")

    if SCHEMA:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
        spark.sql(f"USE {SCHEMA}")


def read_csv_as_raw(path: str) -> DataFrame:
    """
    Read a CSV as a raw landing dataset.
    All columns remain strings in raw_*; typing happens in store_*.
    """
    return (
        spark.read
             .option("header", True)
             .option("inferSchema", False)
             .csv(path)
    )


def write_delta_table(df: DataFrame, table_name: str) -> None:
    """Overwrite a Delta table with schema replacement."""
    (
        df.write
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .format("delta")
          .saveAsTable(fq_table(table_name))
    )


def require_columns(df: DataFrame, expected_columns: list[str], dataset_name: str) -> None:
    """Fail fast if a source file does not contain the expected columns."""
    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset_name} is missing expected columns: {missing}")


def normalize_blank_string(column_name: str):
    """Convert blank strings to NULL while preserving non-blank values."""
    return F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(F.col(column_name))


def validate_primary_key(df: DataFrame, key_columns: list[str], dataset_name: str) -> None:
    """Raise an error if the supplied key is not unique."""
    duplicate_count = (
        df.groupBy(*key_columns)
          .count()
          .filter(F.col("count") > 1)
          .count()
    )
    if duplicate_count > 0:
        raise ValueError(
            f"Primary key validation failed for {dataset_name}. "
            f"Duplicate key rows found for columns: {key_columns}"
        )


def validate_foreign_key(
    child_df: DataFrame,
    parent_df: DataFrame,
    child_columns: list[str],
    parent_columns: list[str],
    relationship_name: str,
) -> None:
    """Raise an error if child rows do not match a parent key."""
    join_condition = reduce(
        lambda acc, cond: acc & cond,
        [
            F.col(f"c.{child_col}") == F.col(f"p.{parent_col}")
            for child_col, parent_col in zip(child_columns, parent_columns)
        ],
    )

    orphan_count = (
        child_df.alias("c")
                .join(parent_df.alias("p"), join_condition, "left_anti")
                .count()
    )

    if orphan_count > 0:
        raise ValueError(
            f"Foreign key validation failed for {relationship_name}. "
            f"Orphan rows found: {orphan_count}"
        )


def log_row_count(table_name: str) -> None:
    """Print row count for a saved table."""
    count = spark.table(fq_table(table_name)).count()
    print(f"{fq_table(table_name)} -> {count:,} rows")


# ==============================================================================
# STAGE 1: RAW TABLES
# ==============================================================================
def load_raw_tables() -> None:
    raw_products_df = read_csv_as_raw(PATHS["products"])
    raw_sales_order_detail_df = read_csv_as_raw(PATHS["sales_order_detail"])
    raw_sales_order_header_df = read_csv_as_raw(PATHS["sales_order_header"])

    require_columns(
        raw_products_df,
        [
            "ProductID", "ProductDesc", "ProductNumber", "MakeFlag", "Color",
            "SafetyStockLevel", "ReorderPoint", "StandardCost", "ListPrice", "Size",
            "SizeUnitMeasureCode", "Weight", "WeightUnitMeasureCode",
            "ProductCategoryName", "ProductSubCategoryName",
        ],
        "products.csv",
    )

    require_columns(
        raw_sales_order_detail_df,
        [
            "SalesOrderID", "SalesOrderDetailID", "OrderQty",
            "ProductID", "UnitPrice", "UnitPriceDiscount",
        ],
        "sales_order_detail.csv",
    )

    require_columns(
        raw_sales_order_header_df,
        [
            "SalesOrderID", "OrderDate", "ShipDate", "OnlineOrderFlag",
            "AccountNumber", "CustomerID", "SalesPersonID", "Freight",
        ],
        "sales_order_header.csv",
    )

    write_delta_table(raw_products_df, TABLES["raw_products"])
    write_delta_table(raw_sales_order_detail_df, TABLES["raw_sales_order_detail"])
    write_delta_table(raw_sales_order_header_df, TABLES["raw_sales_order_header"])

    log_row_count(TABLES["raw_products"])
    log_row_count(TABLES["raw_sales_order_detail"])
    log_row_count(TABLES["raw_sales_order_header"])


# ==============================================================================
# STAGE 2: STORE TABLES
# ==============================================================================
def build_store_products() -> DataFrame:
    """
    Build store_products with explicit typing.

    Data quality note:
    products.csv can contain duplicate ProductID values. To support ProductID as the
    logical primary key, keep the most complete record per ProductID.
    """
    raw_df = spark.table(fq_table(TABLES["raw_products"]))

    typed_df = (
        raw_df.select(
            F.col("ProductID").cast(T.IntegerType()).alias("ProductID"),
            normalize_blank_string("ProductDesc").cast(T.StringType()).alias("ProductDesc"),
            normalize_blank_string("ProductNumber").cast(T.StringType()).alias("ProductNumber"),
            F.col("MakeFlag").cast(T.BooleanType()).alias("MakeFlag"),
            normalize_blank_string("Color").cast(T.StringType()).alias("Color"),
            F.col("SafetyStockLevel").cast(T.IntegerType()).alias("SafetyStockLevel"),
            F.col("ReorderPoint").cast(T.IntegerType()).alias("ReorderPoint"),
            F.col("StandardCost").cast(MONEY_TYPE).alias("StandardCost"),
            F.col("ListPrice").cast(MONEY_TYPE).alias("ListPrice"),
            normalize_blank_string("Size").cast(T.StringType()).alias("Size"),
            normalize_blank_string("SizeUnitMeasureCode").cast(T.StringType()).alias("SizeUnitMeasureCode"),
            F.col("Weight").cast(MEASURE_TYPE).alias("Weight"),
            normalize_blank_string("WeightUnitMeasureCode").cast(T.StringType()).alias("WeightUnitMeasureCode"),
            normalize_blank_string("ProductCategoryName").cast(T.StringType()).alias("ProductCategoryName"),
            normalize_blank_string("ProductSubCategoryName").cast(T.StringType()).alias("ProductSubCategoryName"),
        )
    )

    completeness_columns = [
        "ProductDesc", "ProductNumber", "MakeFlag", "Color", "SafetyStockLevel",
        "ReorderPoint", "StandardCost", "ListPrice", "Size", "SizeUnitMeasureCode",
        "Weight", "WeightUnitMeasureCode", "ProductCategoryName", "ProductSubCategoryName",
    ]

    completeness_score = sum(
        F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))
        for c in completeness_columns
    )

    dedupe_window = Window.partitionBy("ProductID").orderBy(
        F.desc(completeness_score),
        F.desc_nulls_last("ProductCategoryName"),
        F.desc_nulls_last("ProductSubCategoryName"),
        F.desc_nulls_last("ProductNumber"),
    )

    store_df = (
        typed_df
            .withColumn("_rn", F.row_number().over(dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
    )

    return store_df


def build_store_sales_order_detail() -> DataFrame:
    """Build store_sales_order_detail with explicit typing."""
    raw_df = spark.table(fq_table(TABLES["raw_sales_order_detail"]))

    return (
        raw_df.select(
            F.col("SalesOrderID").cast(T.IntegerType()).alias("SalesOrderID"),
            F.col("SalesOrderDetailID").cast(T.IntegerType()).alias("SalesOrderDetailID"),
            F.col("OrderQty").cast(T.IntegerType()).alias("OrderQty"),
            F.col("ProductID").cast(T.IntegerType()).alias("ProductID"),
            F.col("UnitPrice").cast(MONEY_TYPE).alias("UnitPrice"),
            F.col("UnitPriceDiscount").cast(MONEY_TYPE).alias("UnitPriceDiscount"),
        )
    )


def build_store_sales_order_header() -> DataFrame:
    raw_df = spark.table(fq_table(TABLES["raw_sales_order_header"]))

    order_date_raw = F.trim(F.col("OrderDate"))
    ship_date_raw = F.trim(F.col("ShipDate"))

    order_date = (
        F.when(
            order_date_raw.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(order_date_raw, "yyyy-MM-dd")
        )
        .when(
            order_date_raw.rlike(r"^\d{4}-\d{2}$"),
            F.to_date(F.concat(order_date_raw, F.lit("-01")), "yyyy-MM-dd")
        )
        .otherwise(F.lit(None).cast(T.DateType()))
    )

    ship_date = (
        F.when(
            ship_date_raw.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(ship_date_raw, "yyyy-MM-dd")
        )
        .when(
            ship_date_raw.rlike(r"^\d{4}-\d{2}$"),
            F.to_date(F.concat(ship_date_raw, F.lit("-01")), "yyyy-MM-dd")
        )
        .otherwise(F.lit(None).cast(T.DateType()))
    )

    return (
        raw_df.select(
            F.col("SalesOrderID").cast(T.IntegerType()).alias("SalesOrderID"),
            order_date.alias("OrderDate"),
            ship_date.alias("ShipDate"),
            F.col("OnlineOrderFlag").cast(T.BooleanType()).alias("OnlineOrderFlag"),
            normalize_blank_string("AccountNumber").cast(T.StringType()).alias("AccountNumber"),
            F.col("CustomerID").cast(T.IntegerType()).alias("CustomerID"),
            F.col("SalesPersonID").cast(T.IntegerType()).alias("SalesPersonID"),
            F.col("Freight").cast(MONEY_TYPE).alias("Freight"),
        )
    )


def create_store_tables() -> None:
    store_products_df = build_store_products()
    store_sales_order_detail_df = build_store_sales_order_detail()
    store_sales_order_header_df = build_store_sales_order_header()

    write_delta_table(store_products_df, TABLES["store_products"])
    write_delta_table(store_sales_order_detail_df, TABLES["store_sales_order_detail"])
    write_delta_table(store_sales_order_header_df, TABLES["store_sales_order_header"])

    # Logical keys from the case study:
    #   - store_products.ProductID (PK after de-duplication)
    #   - store_sales_order_header.SalesOrderID (PK)
    #   - store_sales_order_detail.SalesOrderDetailID (PK)
    #   - store_sales_order_detail.SalesOrderID -> store_sales_order_header.SalesOrderID (FK)
    #   - store_sales_order_detail.ProductID -> store_products.ProductID (FK)
    validate_primary_key(store_products_df, ["ProductID"], "store_products")
    validate_primary_key(store_sales_order_header_df, ["SalesOrderID"], "store_sales_order_header")
    validate_primary_key(store_sales_order_detail_df, ["SalesOrderDetailID"], "store_sales_order_detail")

    validate_foreign_key(
        child_df=store_sales_order_detail_df,
        parent_df=store_sales_order_header_df,
        child_columns=["SalesOrderID"],
        parent_columns=["SalesOrderID"],
        relationship_name="store_sales_order_detail.SalesOrderID -> store_sales_order_header.SalesOrderID",
    )

    validate_foreign_key(
        child_df=store_sales_order_detail_df,
        parent_df=store_products_df,
        child_columns=["ProductID"],
        parent_columns=["ProductID"],
        relationship_name="store_sales_order_detail.ProductID -> store_products.ProductID",
    )

    log_row_count(TABLES["store_products"])
    log_row_count(TABLES["store_sales_order_detail"])
    log_row_count(TABLES["store_sales_order_header"])


# ==============================================================================
# STAGE 3: publish_product
# ==============================================================================
def create_publish_product() -> None:
    store_products_df = spark.table(fq_table(TABLES["store_products"]))

    category_missing = (
        F.col("ProductCategoryName").isNull() |
        (F.trim(F.col("ProductCategoryName")) == "")
    )

    derived_category = (
        F.when(F.col("ProductSubCategoryName").isin(SUBCATEGORY_CLOTHING), F.lit("Clothing"))
         .when(F.col("ProductSubCategoryName").isin(SUBCATEGORY_ACCESSORIES), F.lit("Accessories"))
         .when(
             F.col("ProductSubCategoryName").contains("Frames") |
             F.col("ProductSubCategoryName").isin(SUBCATEGORY_COMPONENTS_EXACT),
             F.lit("Components"),
         )
    )

    publish_product_df = (
        store_products_df
            .withColumn(
                "Color",
                F.when(F.col("Color").isNull() | (F.trim(F.col("Color")) == ""), F.lit("N/A"))
                 .otherwise(F.col("Color"))
            )
            .withColumn(
                "ProductCategoryName",
                F.when(category_missing, derived_category)
                 .otherwise(F.col("ProductCategoryName"))
            )
    )

    write_delta_table(publish_product_df, TABLES["publish_product"])
    log_row_count(TABLES["publish_product"])


# ==============================================================================
# STAGE 4: publish_orders
# ==============================================================================
def create_publish_orders() -> None:
    detail_df = spark.table(fq_table(TABLES["store_sales_order_detail"])).alias("d")
    header_df = spark.table(fq_table(TABLES["store_sales_order_header"])).alias("h")

    # Business-day lead time, excluding Saturdays and Sundays.
    # Counts weekdays in [OrderDate, ShipDate), so ShipDate itself is not counted.
    lead_time_expr = F.expr("""
        CASE
            WHEN OrderDate IS NULL OR ShipDate IS NULL THEN NULL
            WHEN ShipDate > OrderDate THEN
                size(
                    filter(
                        sequence(OrderDate, date_sub(ShipDate, 1), interval 1 day),
                        x -> dayofweek(x) NOT IN (1, 7)
                    )
                )
            ELSE 0
        END
    """)

    publish_orders_df = (
        detail_df.join(
            header_df,
            F.col("d.SalesOrderID") == F.col("h.SalesOrderID"),
            "inner",
        )
        .withColumn("LeadTimeInBusinessDays", lead_time_expr.cast(T.IntegerType()))
        .withColumn(
            "TotalLineExtendedPrice",
            (
                F.col("d.OrderQty") *
                (F.col("d.UnitPrice") - F.col("d.UnitPriceDiscount"))
            ).cast(MONEY_TYPE),
        )
        .select(
            # All fields from SalesOrderDetail
            F.col("d.SalesOrderID"),
            F.col("d.SalesOrderDetailID"),
            F.col("d.OrderQty"),
            F.col("d.ProductID"),
            F.col("d.UnitPrice"),
            F.col("d.UnitPriceDiscount"),

            # All fields from SalesOrderHeader except SalesOrderID
            F.col("h.OrderDate"),
            F.col("h.ShipDate"),
            F.col("h.OnlineOrderFlag"),
            F.col("h.AccountNumber"),
            F.col("h.CustomerID"),
            F.col("h.SalesPersonID"),
            F.col("h.Freight").alias("TotalOrderFreight"),

            # Required calculated columns
            F.col("LeadTimeInBusinessDays"),
            F.col("TotalLineExtendedPrice"),
        )
    )

    write_delta_table(publish_orders_df, TABLES["publish_orders"])
    log_row_count(TABLES["publish_orders"])


# ==============================================================================
# STAGE 5: ANALYSIS
# ==============================================================================
def create_analysis_outputs() -> None:
    orders_with_product_df = (
        spark.table(fq_table(TABLES["publish_orders"])).alias("o")
             .join(
                 spark.table(fq_table(TABLES["publish_product"])).alias("p"),
                 F.col("o.ProductID") == F.col("p.ProductID"),
                 "left",
             )
    )

    # 1) Which color generated the highest revenue each year?
    color_revenue_by_year_df = (
        orders_with_product_df
            .withColumn("OrderYear", F.year("OrderDate"))
            .groupBy("OrderYear", "Color")
            .agg(F.sum("TotalLineExtendedPrice").alias("Revenue"))
    )

    top_color_by_year_df = (
        color_revenue_by_year_df
            .withColumn(
                "row_num",
                F.row_number().over(
                    Window.partitionBy("OrderYear").orderBy(F.desc("Revenue"), F.asc("Color"))
                ),
            )
            .filter(F.col("row_num") == 1)
            .drop("row_num")
            .orderBy("OrderYear")
    )

    # 2) What is the average LeadTimeInBusinessDays by ProductCategoryName?
    avg_leadtime_by_category_df = (
        orders_with_product_df
            .groupBy("ProductCategoryName")
            .agg(F.avg("LeadTimeInBusinessDays").alias("AvgLeadTimeInBusinessDays"))
            .orderBy("ProductCategoryName")
    )

    write_delta_table(top_color_by_year_df, TABLES["analysis_top_color_by_year"])
    write_delta_table(avg_leadtime_by_category_df, TABLES["analysis_avg_leadtime_by_category"])

    print("Top color by revenue each year:")
    top_color_by_year_df.show(truncate=False)

    print("Average LeadTimeInBusinessDays by ProductCategoryName:")
    avg_leadtime_by_category_df.show(truncate=False)


# ==============================================================================
# MAIN
# ==============================================================================
def main() -> None:
    initialize_schema()
    load_raw_tables()
    create_store_tables()
    create_publish_product()
    create_publish_orders()
    create_analysis_outputs()

    print("Pipeline completed successfully.")
    print("Created tables:")
    for table_name in TABLES.values():
        print(f" - {fq_table(table_name)}")


main()