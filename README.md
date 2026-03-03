# Databricks Interview Case Study - PySpark Pipeline

## Overview

This Databricks notebook implements a full medallion-style pipeline in PySpark for the interview case study.

It covers the required steps:

1. Load the three source CSV files into `raw_*` tables.
2. Review and cast data into typed `store_*` tables.
3. Apply product master transformations into `publish_product`.
4. Join sales order tables and apply order transformations into `publish_orders`.
5. Produce the two requested analytical outputs and persist them as tables.

The submitted requirements explicitly ask for these five deliverables: raw ingestion, typed storage, product transformations, sales order transformations, and two business analysis outputs. This notebook implements all of them. Source requirement summary: fileciteturn0file0

## Repository Intent

This code is designed to run as a Databricks notebook and persist outputs as Delta tables inside a configurable schema.

Default configuration:

- `SCHEMA = "case_study"`
- `BASE_PATH = "/Volumes/workspace/default/test"`
- `CATALOG = None` (optional)

## How the Notebook Maps to the Requirements

### 1. Data Loading

**Requirement:** Load the three provided files and store them with a `raw_` prefix. fileciteturn0file0

**Implementation:**

- `load_raw_tables()` reads:
  - `products.csv`
  - `sales_order_detail.csv`
  - `sales_order_header.csv`
- `read_csv_as_raw()` loads all columns as strings (`inferSchema=False`) so the raw layer remains a faithful landing zone.
- The resulting Delta tables are saved as:
  - `raw_products`
  - `raw_sales_order_detail`
  - `raw_sales_order_header`

**Why this is good:**
Keeping raw data untyped avoids accidental type coercion during ingestion and preserves source fidelity.

### 2. Data Review and Storage

**Requirement:** Review data, assign data types, identify primary/foreign keys, and store transformed tables using a `store_` prefix. fileciteturn0file0

**Implementation:**

- `build_store_products()` casts product fields to appropriate types:
  - IDs and stock fields -> `IntegerType`
  - flags -> `BooleanType`
  - money/measure fields -> `DecimalType(18,4)`
  - text fields -> `StringType`
- `build_store_sales_order_detail()` casts detail fields into typed numeric/decimal columns.
- `build_store_sales_order_header()` parses dates and casts flags, IDs, and freight.
- `create_store_tables()` writes:
  - `store_products`
  - `store_sales_order_detail`
  - `store_sales_order_header`

**Primary and foreign keys implemented in code:**

- `store_products.ProductID` -> primary key
- `store_sales_order_header.SalesOrderID` -> primary key
- `store_sales_order_detail.SalesOrderDetailID` -> primary key
- `store_sales_order_detail.SalesOrderID` -> foreign key to `store_sales_order_header.SalesOrderID`
- `store_sales_order_detail.ProductID` -> foreign key to `store_products.ProductID`

**Validation logic:**

- `validate_primary_key()` fails if duplicates exist.
- `validate_foreign_key()` fails if orphan child rows exist.

**Extra robustness beyond the requirement:**

- `build_store_products()` handles duplicate `ProductID` values by keeping the most complete record using a window function and a completeness score.
- This is a practical enhancement because the requirement asks for `ProductID` to function as a key, and duplicate source rows would otherwise break that assumption.

### 3. Product Master Transformations

**Requirement:** Build `publish_product` with two rules: replace null `Color` with `N/A`, and derive missing `ProductCategoryName` from `ProductSubCategoryName` using the specified mapping rules. fileciteturn0file0

**Implementation:**

- `create_publish_product()` reads `store_products`.
- Replaces null/blank `Color` with `N/A`.
- Fills missing `ProductCategoryName` using:
  - `Gloves, Shorts, Socks, Tights, Vests` -> `Clothing`
  - `Locks, Lights, Headsets, Helmets, Pedals, Pumps` -> `Accessories`
  - subcategory contains `Frames` or equals `Wheels, Saddles` -> `Components`
- Writes the result to `publish_product`.

**Requirement fit:**
This section matches the product transformation rules exactly.

### 4. Sales Order Transformations

**Requirement:** Join SalesOrderDetail and SalesOrderHeader on `SalesOrderID`, calculate business-day lead time, calculate total line extended price, and create `publish_orders` with all detail columns plus all header columns except `SalesOrderID` while renaming `Freight` to `TotalOrderFreight`. fileciteturn0file0

**Implementation:**

- `create_publish_orders()` joins:
  - `store_sales_order_detail` as detail
  - `store_sales_order_header` as header
- Computes `LeadTimeInBusinessDays` using Spark SQL functions:
  - generates a date sequence
  - excludes Saturdays and Sundays
  - returns weekday count
- Computes `TotalLineExtendedPrice` as:
  - `OrderQty * (UnitPrice - UnitPriceDiscount)`
- Selects:
  - all fields from SalesOrderDetail
  - all fields from SalesOrderHeader except duplicate `SalesOrderID`
  - renames `Freight` to `TotalOrderFreight`
- Writes the result to `publish_orders`.

**Important implementation note:**
The lead-time logic counts weekdays in the range `[OrderDate, ShipDate)`, which means it includes the order date and excludes the ship date. That is a reasonable interpretation of business-day difference, but if your stakeholders define lead time differently, this is the one place that may need alignment.

### 5. Analysis Questions

**Requirement:** Answer two questions based on transformed data. fileciteturn0file0

**Implementation:**

`create_analysis_outputs()` joins `publish_orders` with `publish_product` and produces:

1. `analysis_top_color_by_year`
   - groups revenue by `OrderYear` and `Color`
   - uses `TotalLineExtendedPrice` as revenue
   - ranks colors per year with a window function
   - keeps the top revenue color per year

2. `analysis_avg_leadtime_by_category`
   - groups by `ProductCategoryName`
   - calculates average `LeadTimeInBusinessDays`

The notebook saves both outputs as Delta tables and prints them for review.

## Code Structure

### Configuration

- `CATALOG`, `SCHEMA`, `BASE_PATH`
- `PATHS` defines CSV locations.
- `TABLES` centralizes all table names.
- Decimal precision is standardized through:
  - `MONEY_TYPE = DecimalType(18,4)`
  - `MEASURE_TYPE = DecimalType(18,4)`

### Utility Functions

- `fq_table()` builds fully qualified table names.
- `initialize_schema()` creates/selects schema.
- `read_csv_as_raw()` standardizes CSV ingestion.
- `write_delta_table()` centralizes Delta overwrite logic.
- `require_columns()` performs fail-fast source schema validation.
- `normalize_blank_string()` converts blanks to nulls.
- `validate_primary_key()` and `validate_foreign_key()` enforce integrity.
- `log_row_count()` prints row counts after writes.

### Main Entry Point

`main()` runs the pipeline in this order:

1. `initialize_schema()`
2. `load_raw_tables()`
3. `create_store_tables()`
4. `create_publish_product()`
5. `create_publish_orders()`
6. `create_analysis_outputs()`

This sequencing is correct and reflects the dependency chain between layers.

## Why This Implementation Is Strong

- Clear layered design: raw -> store -> publish -> analysis
- Explicit typing instead of implicit inference
- Built-in schema validation and referential integrity checks
- Handles dirty source data (blank strings, duplicate products, partial dates)
- Uses Delta tables for durable outputs in Databricks
- Keeps table names and paths centralized for easier maintenance

## Assumptions and Edge Cases

### Duplicate products

The code assumes `ProductID` should behave as the logical primary key. If duplicates appear in the source, it keeps the most complete record. This is not explicitly required, but it is a sensible defensive data-quality choice.

### Blank strings

Blank strings are normalized to null for key text columns before transformations. This prevents empty-string values from being treated differently than true nulls.

### Partial dates

If `OrderDate` or `ShipDate` is provided as `yyyy-MM`, the code converts it to the first day of that month. Invalid formats become null.

### Lead time when dates are missing or reversed

- If either date is null, lead time becomes null.
- If `ShipDate <= OrderDate`, lead time becomes `0`.

## How to Run

1. Upload the three CSV files to the configured Databricks volume path.
2. Update `BASE_PATH`, `SCHEMA`, and optionally `CATALOG` if needed.
3. Attach the notebook to a Spark cluster.
4. Run the notebook.
5. Review the generated Delta tables in the target schema.

## Tables Created

### Raw layer
- `raw_products`
- `raw_sales_order_detail`
- `raw_sales_order_header`

### Store layer
- `store_products`
- `store_sales_order_detail`
- `store_sales_order_header`

### Publish layer
- `publish_product`
- `publish_orders`

### Analysis layer
- `analysis_top_color_by_year`
- `analysis_avg_leadtime_by_category`

## Suggested Improvements

If you want to take this from interview-ready to production-ready, consider:

- adding unit tests for transformation functions
- parameterizing paths through widgets or job parameters
- adding data quality metrics (null rates, rejection counts, duplicate counts)
- partitioning larger tables if data volume grows
- capturing audit columns such as load timestamp and source file name
- adding expectations or constraints with Delta Live Tables / Unity Catalog policies

## Bottom Line

The notebook satisfies the case study requirements and also adds useful production-minded safeguards (data validation, duplicate handling, and cleaner normalization). The only item worth explicitly confirming with reviewers is the exact business definition of `LeadTimeInBusinessDays`, because the implementation uses an inclusive order date and exclusive ship date calculation.
