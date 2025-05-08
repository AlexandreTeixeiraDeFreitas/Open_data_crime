#!/usr/bin/env python3
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql.functions import col, to_timestamp, lit, when

# Schéma de la table crimes
crime_schema = StructType([
    StructField("cmplnt_num", StringType()),
    StructField("addr_pct_cd", IntegerType()),
    StructField("boro_nm", StringType()),
    StructField("cmplnt_fr_dt", TimestampType()),
    StructField("cmplnt_fr_tm", StringType()),
    StructField("cmplnt_to_dt", TimestampType()),
    StructField("cmplnt_to_tm", StringType()),
    StructField("crm_atpt_cptd_cd", StringType()),
    StructField("hadevelopt", StringType()),
    StructField("housing_psa", IntegerType()),
    StructField("jurisdiction_code", IntegerType()),
    StructField("juris_desc", StringType()),
    StructField("ky_cd", IntegerType()),
    StructField("law_cat_cd", StringType()),
    StructField("loc_of_occur_desc", StringType()),
    StructField("ofns_desc", StringType()),
    StructField("parks_nm", StringType()),
    StructField("patrol_boro", StringType()),
    StructField("pd_cd", IntegerType()),
    StructField("pd_desc", StringType()),
    StructField("prem_typ_desc", StringType()),
    StructField("rpt_dt", TimestampType()),
    StructField("station_name", StringType()),
    StructField("susp_age_group", StringType()),
    StructField("susp_race", StringType()),
    StructField("susp_sex", StringType()),
    StructField("transit_district", IntegerType()),
    StructField("vic_age_group", StringType()),
    StructField("vic_race", StringType()),
    StructField("vic_sex", StringType()),
    StructField("x_coord_cd", IntegerType()),
    StructField("y_coord_cd", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

train_schema = StructType([
    StructField("id", StringType()),
    StructField("status", StringType())
])

schema_event = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("event_name", StringType(), True),
    StructField("start_date_time", TimestampType(), True),
    StructField("end_date_time", TimestampType(), True),
    StructField("event_agency", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_borough", StringType(), True),
    StructField("event_location", StringType(), True),
    StructField("event_street_side", StringType(), True),
    StructField("street_closure_type", StringType(), True),
    StructField("community_board", StringType(), True),
    StructField("police_precinct", StringType(), True)
])


def cast_columns_to_schema(df, schema):
    for field in schema.fields:
        if field.name not in df.columns:
            default_value = lit(0) if isinstance(field.dataType, (IntegerType, DoubleType)) else lit(None)
            df = df.withColumn(field.name, default_value.cast(field.dataType))
        else:
            if isinstance(field.dataType, TimestampType):
                df = df.withColumn(field.name, to_timestamp(col(field.name)))
            else:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df.select([f.name for f in schema.fields])