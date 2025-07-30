#!/usr/bin/env python3
"""
Spark job for transforming Chilean export data (FOB)
Compatible with real Chilean export CSV structure and PostgreSQL schema
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import os

def create_spark_session():
    """Create Spark session with MinIO configuration"""
    spark = SparkSession.builder \
        .appName("ChileanExportsDataTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def read_export_data(spark):
    """Read Chilean export CSV data from MinIO"""
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .option("escape", '"') \
            .csv("s3a://etl-data/raw/local_data/*.csv")
        
        print(f"Successfully read {df.count()} records from Chilean export data")
        print(f"Columns detected: {df.columns}")
        
        return df
        
    except Exception as e:
        print(f"Error reading export data: {str(e)}")
        raise

def transform_export_data(df):
    """Apply transformations to Chilean export data"""
    print("Starting Chilean export data transformations...")
    print(f"Original columns: {df.columns}")
    column_mappings = {}
    
    for col in df.columns:
        col_clean = col.strip()
        col_upper = col_clean.upper()
        
        if 'FOB' in col_upper and ('US$' in col_upper or 'USD' in col_upper or 'DOLAR' in col_upper):
            column_mappings[col_clean] = 'US$ FOB'
        elif 'FLETE' in col_upper and ('US$' in col_upper or 'USD' in col_upper):
            column_mappings[col_clean] = 'US$ FLETE'
        elif 'SEGURO' in col_upper and ('US$' in col_upper or 'USD' in col_upper):
            column_mappings[col_clean] = 'US$ SEGURO'
        elif 'CIF' in col_upper and ('US$' in col_upper or 'USD' in col_upper):
            column_mappings[col_clean] = 'US$ CIF'
        elif 'PAIS' in col_upper and 'DESTINO' in col_upper:
            column_mappings[col_clean] = 'PAIS DE DESTINO'
        elif col_upper == 'PRODUCTO' and 'DESCRIPCION' not in col_upper:
            column_mappings[col_clean] = 'PRODUCTO'
        elif 'PESO' in col_upper and 'BRUTO' in col_upper:
            column_mappings[col_clean] = 'PESO BRUTO'
        elif col_upper == 'CANTIDAD' or ('CANTIDAD' in col_upper and 'UNIDAD' not in col_upper):
            column_mappings[col_clean] = 'CANTIDAD'
        elif 'EXPORTADOR' in col_upper and 'RUT' in col_upper:
            column_mappings[col_clean] = 'RUT PROBABLE EXPORTADOR'
        elif 'REGION' in col_upper and ('ORIGEN' in col_upper):
            column_mappings[col_clean] = 'REGION DE ORIGEN'
        elif 'COMUNA' in col_upper and 'EXPORTADOR' in col_upper:
            column_mappings[col_clean] = 'COMUNA EXPORTADOR'
        elif 'PUERTO' in col_upper and ('EMBARQUE' in col_upper or 'SALIDA' in col_upper):
            column_mappings[col_clean] = 'PUERTO EMBARQUE'
        elif 'VIA' in col_upper and 'TRANSPORTE' in col_upper:
            column_mappings[col_clean] = 'VIA DE TRANSPORTE'
        elif col_upper in ['DIA', 'DAY']:
            column_mappings[col_clean] = 'DIA'
        elif col_upper in ['MES', 'MONTH']:
            column_mappings[col_clean] = 'MES'
        elif col_upper in ['AÑO', 'ANO', 'YEAR', 'ANIO', "AÃ'O"]:
            column_mappings[col_clean] = 'YEAR'
        elif 'SOURCE_FILE' in col_upper:
            column_mappings[col_clean] = 'SOURCE_FILE'
        elif 'EXTRACTION_DATE' in col_upper:
            column_mappings[col_clean] = 'EXTRACTION_DATE'
    
    for old_col, new_col in column_mappings.items():
        df = df.withColumnRenamed(old_col, new_col)
    
    print(f"Applied {len(column_mappings)} column mappings")
    print(f"Columns after mapping: {df.columns}")
    
    print("Cleaning and validating export data...")
    if 'US$ FOB' in df.columns:
        df = df.withColumn("FOB_CLEAN", 
                          when(col("US$ FOB").isNull(), lit(0))
                          .otherwise(
                              regexp_replace(
                                  regexp_replace(col("US$ FOB").cast("string"), "[,$]", ""),
                                  "[^0-9.-]", ""
                              ).cast("double")
                          ))
        df = df.filter(col("FOB_CLEAN") > 0)
    
    if 'PESO BRUTO' in df.columns:
        df = df.withColumn("PESO_BRUTO_CLEAN", 
                          when(col("PESO BRUTO").isNull(), lit(0))
                          .otherwise(
                              regexp_replace(col("PESO BRUTO").cast("string"), "[^0-9.]", "")
                              .cast("double")
                          ))
    if 'CANTIDAD' in df.columns:
        df = df.withColumn("CANTIDAD_CLEAN", 
                          when(col("CANTIDAD").isNull(), lit(0))
                          .otherwise(
                              regexp_replace(col("CANTIDAD").cast("string"), "[^0-9.]", "")
                              .cast("double")
                          ))

    print("Creating date fields...")
    if all(col_name in df.columns for col_name in ['DIA', 'MES', 'YEAR']):
        df = df.withColumn("EXPORT_DATE", 
                          to_date(concat_ws("-", 
                                          col("YEAR").cast("string"), 
                                          lpad(col("MES").cast("string"), 2, "0"),
                                          lpad(col("DIA").cast("string"), 2, "0")
                                         ), "yyyy-MM-dd"))
    elif all(col_name in df.columns for col_name in ['MES', 'YEAR']):
        df = df.withColumn("EXPORT_DATE", 
                          to_date(concat_ws("-", 
                                          col("YEAR").cast("string"), 
                                          lpad(col("MES").cast("string"), 2, "0"),
                                          lit("01")
                                         ), "yyyy-MM-dd"))
    
    print("Standardizing text fields...")
    if 'PAIS DE DESTINO' in df.columns:
        df = df.withColumn("PAIS_DESTINO_CLEAN", 
                          trim(upper(regexp_replace(col("PAIS DE DESTINO"), "[^A-Za-z\\s]", ""))))
    
    if 'PRODUCTO' in df.columns:
        df = df.withColumn("PRODUCTO_CLEAN", 
                          trim(upper(col("PRODUCTO"))))
    
    if 'REGION DE ORIGEN' in df.columns:
        df = df.withColumn("REGION_ORIGEN_CLEAN", 
                          trim(upper(col("REGION DE ORIGEN"))))
    
    print("Calculating derived metrics...")
    if 'FOB_CLEAN' in df.columns and 'PESO_BRUTO_CLEAN' in df.columns:
        df = df.withColumn("FOB_PER_KG", 
                          when((col("PESO_BRUTO_CLEAN") > 0) & (col("FOB_CLEAN") > 0), 
                               col("FOB_CLEAN") / col("PESO_BRUTO_CLEAN"))
                          .otherwise(lit(None)))
    
    if 'FOB_CLEAN' in df.columns and 'CANTIDAD_CLEAN' in df.columns:
        df = df.withColumn("FOB_PER_UNIT", 
                          when((col("CANTIDAD_CLEAN") > 0) & (col("FOB_CLEAN") > 0), 
                               col("FOB_CLEAN") / col("CANTIDAD_CLEAN"))
                          .otherwise(lit(None)))
    if 'FOB_CLEAN' in df.columns:
        df = df.withColumn("EXPORT_SIZE_CATEGORY",
                          when(col("FOB_CLEAN") >= 1000000, "Large (>$1M)")
                          .when(col("FOB_CLEAN") >= 100000, "Medium ($100K-$1M)")
                          .when(col("FOB_CLEAN") >= 10000, "Small ($10K-$100K)")
                          .otherwise("Micro (<$10K)"))
    if 'REGION_ORIGEN_CLEAN' in df.columns:
        df = df.withColumn("REGION_GROUP",
                          when(col("REGION_ORIGEN_CLEAN").contains("METROPOLITANA"), "Central")
                          .when(col("REGION_ORIGEN_CLEAN").contains("VALPARAISO"), "Central")
                          .when(col("REGION_ORIGEN_CLEAN").contains("OHIGGINS"), "Central")
                          .when(col("REGION_ORIGEN_CLEAN").contains("MAULE"), "Central")
                          .when(col("REGION_ORIGEN_CLEAN").contains("BIOBIO"), "South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("ARAUCANIA"), "South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("RIOS"), "South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("LAGOS"), "South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("AYSEN"), "Far South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("MAGALLANES"), "Far South")
                          .when(col("REGION_ORIGEN_CLEAN").contains("ANTOFAGASTA"), "North")
                          .when(col("REGION_ORIGEN_CLEAN").contains("ATACAMA"), "North")
                          .when(col("REGION_ORIGEN_CLEAN").contains("COQUIMBO"), "North")
                          .when(col("REGION_ORIGEN_CLEAN").contains("TARAPACA"), "Far North")
                          .when(col("REGION_ORIGEN_CLEAN").contains("ARICA"), "Far North")
                          .otherwise("Other"))
    
    df = df.withColumn("PROCESSED_AT", current_timestamp()) \
           .withColumn("PROCESSING_DATE", current_date())
    
    final_count = df.count()
    print(f"Transformation completed. Final record count: {final_count}")
    
    return df

def create_export_aggregations(df):
    """Create various aggregations of the export data"""
    print("Creating export data aggregations...")
    
    aggregations_created = []
    
    if all(col_name in df.columns for col_name in ['YEAR', 'MES', 'PAIS_DESTINO_CLEAN', 'FOB_CLEAN']):
        monthly_by_country = df.groupBy("YEAR", "MES", "PAIS_DESTINO_CLEAN") \
            .agg(
                count("*").alias("EXPORT_COUNT"),
                sum("FOB_CLEAN").alias("TOTAL_FOB_USD"),
                avg("FOB_CLEAN").alias("AVG_FOB_USD"),
                sum(when(col("PESO_BRUTO_CLEAN").isNotNull(), col("PESO_BRUTO_CLEAN")).otherwise(0)).alias("TOTAL_WEIGHT_KG"),
                countDistinct(when(col("RUT PROBABLE EXPORTADOR").isNotNull(), col("RUT PROBABLE EXPORTADOR"))).alias("UNIQUE_EXPORTERS")
            ) \
            .filter(col("TOTAL_FOB_USD") > 0) \
            .orderBy("YEAR", "MES", desc("TOTAL_FOB_USD"))
        
        aggregations_created.append("monthly_by_country")
        print(f"Created monthly by country aggregation: {monthly_by_country.count()} records")
    else:
        monthly_by_country = spark.createDataFrame([], StructType([]))
        print("Skipped monthly by country aggregation - missing required columns")
    
    if all(col_name in df.columns for col_name in ['REGION_ORIGEN_CLEAN', 'FOB_CLEAN', 'YEAR']):
        regional_performance = df.filter(col("REGION_ORIGEN_CLEAN").isNotNull()) \
            .groupBy("REGION_ORIGEN_CLEAN", "YEAR") \
            .agg(
                count("*").alias("TOTAL_EXPORTS"),
                sum("FOB_CLEAN").alias("TOTAL_FOB_USD"),
                avg("FOB_CLEAN").alias("AVG_FOB_USD"),
                countDistinct(when(col("PAIS_DESTINO_CLEAN").isNotNull(), col("PAIS_DESTINO_CLEAN"))).alias("UNIQUE_DESTINATIONS"),
                countDistinct(when(col("PRODUCTO_CLEAN").isNotNull(), col("PRODUCTO_CLEAN"))).alias("UNIQUE_PRODUCTS")
            ) \
            .filter(col("TOTAL_FOB_USD") > 0) \
            .orderBy("YEAR", desc("TOTAL_FOB_USD"))
        
        aggregations_created.append("regional_performance")
        print(f"Created regional performance aggregation: {regional_performance.count()} records")
    else:
        regional_performance = spark.createDataFrame([], StructType([]))
        print("Skipped regional performance aggregation - missing required columns")
    
    if all(col_name in df.columns for col_name in ['PRODUCTO_CLEAN', 'FOB_CLEAN', 'YEAR']):
        product_performance = df.filter(col("PRODUCTO_CLEAN").isNotNull()) \
            .groupBy("PRODUCTO_CLEAN", "YEAR") \
            .agg(
                count("*").alias("EXPORT_COUNT"),
                sum("FOB_CLEAN").alias("TOTAL_FOB_USD"),
                avg("FOB_CLEAN").alias("AVG_FOB_USD"),
                avg(when(col("FOB_PER_KG").isNotNull(), col("FOB_PER_KG"))).alias("AVG_FOB_PER_KG"),
                countDistinct(when(col("PAIS_DESTINO_CLEAN").isNotNull(), col("PAIS_DESTINO_CLEAN"))).alias("DESTINATION_DIVERSITY")
            ) \
            .filter(col("TOTAL_FOB_USD") > 0) \
            .orderBy("YEAR", desc("TOTAL_FOB_USD"))
        
        aggregations_created.append("product_performance")
        print(f"Created product performance aggregation: {product_performance.count()} records")
    else:
        product_performance = spark.createDataFrame([], StructType([]))
        print("Skipped product performance aggregation - missing required columns")
    
    print(f"Successfully created aggregations: {aggregations_created}")
    return monthly_by_country, regional_performance, product_performance

def write_to_minio(df, path_suffix, mode="overwrite"):
    """Write DataFrame to MinIO"""
    try:
        output_path = f"s3a://etl-data/processed/local_data/{path_suffix}"
        
        df.coalesce(1) \
          .write \
          .mode(mode) \
          .option("header", "true") \
          .csv(output_path)
        
        print(f"Successfully wrote {df.count()} records to {output_path}")
        
    except Exception as e:
        print(f"Error writing to MinIO {output_path}: {str(e)}")
        raise

def main():
    """Main execution function"""
    print("=== Starting Chilean Export Data Transformation Job ===")
    
    try:
        spark = create_spark_session()
        
        df = read_export_data(spark)
        
        if df.count() == 0:
            print("No export data found to process")
            return
        
        df_transformed = transform_export_data(df)

        monthly_by_country, regional_performance, product_performance = create_export_aggregations(df_transformed)
        write_to_minio(df_transformed, "processed_export_data")
        if monthly_by_country.count() > 0:
            write_to_minio(monthly_by_country, "monthly_exports_by_country")
        
        if regional_performance.count() > 0:
            write_to_minio(regional_performance, "regional_export_performance")
        
        if product_performance.count() > 0:
            write_to_minio(product_performance, "product_export_performance")
        print("\n=== Export Data Processing Statistics ===")
        print(f"Total records processed: {df_transformed.count()}")
        
        if 'YEAR' in df_transformed.columns:
            years_count = df_transformed.select('YEAR').distinct().count()
            print(f"Years covered: {years_count}")
        
        if 'PAIS_DESTINO_CLEAN' in df_transformed.columns:
            countries_count = df_transformed.select('PAIS_DESTINO_CLEAN').distinct().count()
            print(f"Destination countries: {countries_count}")
        
        if 'FOB_CLEAN' in df_transformed.columns:
            total_fob = df_transformed.agg(sum("FOB_CLEAN")).collect()[0][0]
            if total_fob:
                print(f"Total FOB value: ${total_fob:,.2f} USD")
        
        print("\n=== Sample Processed Export Data ===")
        sample_columns = []
        available_columns = df_transformed.columns
        
        for col_name in ['YEAR', 'MES', 'PAIS_DESTINO_CLEAN', 'PRODUCTO_CLEAN', 'FOB_CLEAN', 'EXPORT_SIZE_CATEGORY', 'REGION_ORIGEN_CLEAN']:
            if col_name in available_columns:
                sample_columns.append(col_name)
        
        if sample_columns:
            df_transformed.select(*sample_columns).show(10, truncate=False)
        else:
            available_cols = df_transformed.columns[:10]
            df_transformed.select(*available_cols).show(10, truncate=False)
        
        print("=== Chilean Export Data Transformation Job Completed Successfully! ===")
        
    except Exception as e:
        print(f"Job failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()