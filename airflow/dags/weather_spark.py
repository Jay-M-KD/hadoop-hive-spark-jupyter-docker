from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_weather_etl_pipeline',
    default_args=default_args,
    description='Spark-based ETL pipeline for weather data processing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'weather', 'etl', 'bigdata'],
)

# Configuration
CITIES = ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai', 'Paris', 'Berlin', 'Moscow']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
API_KEY = 'fd1f6f6cc6659b69dd9097beb9ac8707'  # Replace with actual API key
DATA_DIR = '/tmp/spark_weather_data'
SPARK_HOME = '/opt/spark'  # Adjust based on your Spark installation
HDFS_PATH = '/user/weather_data'  # HDFS path for storing data

def extract_weather_data(**context):
    """Extract weather data and prepare for Spark processing"""
    print("Starting weather data extraction for Spark processing...")
    
    weather_data = []
    
    for city in CITIES:
        try:
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'
            }
            
            print(f"Fetching weather data for {city}...")
            response = requests.get(BASE_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                weather_data.append(data)
                print(f"✓ Successfully fetched data for {city}")
            else:
                print(f"✗ Failed to fetch data for {city}: {response.status_code}")
                
        except Exception as e:
            print(f"✗ Error fetching data for {city}: {str(e)}")
            continue
    
    if not weather_data:
        raise ValueError("No weather data was successfully extracted!")
    
    # Save raw data for Spark processing
    os.makedirs(DATA_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_data_path = f"{DATA_DIR}/raw_weather_{timestamp}.json"
    
    with open(raw_data_path, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    print(f"Raw data saved to {raw_data_path}")
    print(f"Extracted data for {len(weather_data)} cities")
    
    # Store the file path for next tasks
    context['task_instance'].xcom_push(key='raw_data_path', value=raw_data_path)
    context['task_instance'].xcom_push(key='timestamp', value=timestamp)
    
    return raw_data_path

def prepare_spark_job(**context):
    """Prepare Spark job script and configuration"""
    print("Preparing Spark job configuration...")
    
    timestamp = context['task_instance'].xcom_pull(task_ids='extract_weather_data', key='timestamp')
    
    # Create Spark job script
    spark_script = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Initialize Spark session
spark = SparkSession.builder \\
    .appName("WeatherETLPipeline") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("Starting Spark weather data processing...")

try:
    # Read raw JSON data
    raw_df = spark.read.option("multiline", "true").json("{DATA_DIR}/raw_weather_{timestamp}.json")
    
    print(f"Loaded {{raw_df.count()}} records from raw data")
    
    # Define schema for flattened data
    weather_schema = StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("feels_like_celsius", DoubleType(), True),
        StructField("humidity_percent", IntegerType(), True),
        StructField("pressure_hpa", IntegerType(), True),
        StructField("weather_main", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("wind_speed_mps", DoubleType(), True),
        StructField("wind_direction_degrees", IntegerType(), True),
        StructField("cloudiness_percent", IntegerType(), True),
        StructField("visibility_meters", IntegerType(), True),
        StructField("sunrise_timestamp", LongType(), True),
        StructField("sunset_timestamp", LongType(), True),
        StructField("data_timestamp", LongType(), True),
        StructField("extraction_datetime", StringType(), True)
    ])
    
    # Transform and flatten the nested JSON structure
    transformed_df = raw_df.select(
        col("name").alias("city"),
        col("sys.country").alias("country"),
        round(col("main.temp"), 1).alias("temperature_celsius"),
        round(col("main.feels_like"), 1).alias("feels_like_celsius"),
        col("main.humidity").alias("humidity_percent"),
        col("main.pressure").alias("pressure_hpa"),
        col("weather").getItem(0).getField("main").alias("weather_main"),
        col("weather").getItem(0).getField("description").alias("weather_description"),
        coalesce(col("wind.speed"), lit(0.0)).alias("wind_speed_mps"),
        coalesce(col("wind.deg"), lit(0)).alias("wind_direction_degrees"),
        col("clouds.all").alias("cloudiness_percent"),
        coalesce(col("visibility"), lit(0)).alias("visibility_meters"),
        col("sys.sunrise").alias("sunrise_timestamp"),
        col("sys.sunset").alias("sunset_timestamp"),
        col("dt").alias("data_timestamp"),
        lit("{datetime.now().isoformat()}").alias("extraction_datetime")
    )
    
    # Add computed columns
    enhanced_df = transformed_df \\
        .withColumn("temperature_fahrenheit", 
                   round((col("temperature_celsius") * 9/5) + 32, 1)) \\
        .withColumn("temperature_category",
                   when(col("temperature_celsius") < 0, "freezing")
                   .when(col("temperature_celsius") < 10, "cold")
                   .when(col("temperature_celsius") < 20, "cool")
                   .when(col("temperature_celsius") < 30, "warm")
                   .otherwise("hot")) \\
        .withColumn("humidity_category",
                   when(col("humidity_percent") < 30, "low")
                   .when(col("humidity_percent") < 60, "moderate")
                   .otherwise("high")) \\
        .withColumn("wind_category",
                   when(col("wind_speed_mps") < 2, "calm")
                   .when(col("wind_speed_mps") < 6, "light")
                   .when(col("wind_speed_mps") < 12, "moderate")
                   .otherwise("strong")) \\
        .withColumn("extraction_date", lit("{datetime.now().strftime('%Y-%m-%d')}")) \\
        .withColumn("processing_timestamp", current_timestamp())
    
    # Cache the dataframe for multiple operations
    enhanced_df.cache()
    
    print(f"Transformed {{enhanced_df.count()}} records")
    
    # Data quality checks
    print("Performing data quality checks...")
    
    # Check for null values in critical columns
    critical_cols = ["city", "temperature_celsius", "humidity_percent"]
    for col_name in critical_cols:
        null_count = enhanced_df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            print(f"WARNING: {{null_count}} null values found in {{col_name}}")
    
    # Temperature range validation
    temp_stats = enhanced_df.agg(
        min("temperature_celsius").alias("min_temp"),
        max("temperature_celsius").alias("max_temp"),
        avg("temperature_celsius").alias("avg_temp")
    ).collect()[0]
    
    print(f"Temperature stats: Min={{temp_stats['min_temp']}}°C, Max={{temp_stats['max_temp']}}°C, Avg={{temp_stats['avg_temp']:.1f}}°C")
    
    if temp_stats['min_temp'] < -50 or temp_stats['max_temp'] > 60:
        print("WARNING: Temperature values outside reasonable range!")
    
    # Generate summary statistics
    print("Generating summary statistics...")
    
    city_stats = enhanced_df.groupBy("city", "country") \\
        .agg(
            avg("temperature_celsius").alias("avg_temp"),
            avg("humidity_percent").alias("avg_humidity"),
            avg("pressure_hpa").alias("avg_pressure"),
            first("weather_main").alias("weather_condition"),
            first("temperature_category").alias("temp_category"),
            first("humidity_category").alias("humidity_category")
        ) \\
        .orderBy("avg_temp", ascending=False)
    
    # Show top cities by temperature
    print("Top 5 warmest cities:")
    city_stats.show(5, truncate=False)
    
    # Weather condition distribution
    print("Weather condition distribution:")
    enhanced_df.groupBy("weather_main") \\
        .count() \\
        .orderBy("count", ascending=False) \\
        .show()
    
    # Temperature category distribution
    print("Temperature category distribution:")
    enhanced_df.groupBy("temperature_category") \\
        .count() \\
        .orderBy("count", ascending=False) \\
        .show()
    
    # Save processed data in multiple formats
    output_path = "{DATA_DIR}/processed_weather_{timestamp}"
    
    # Save as Parquet (columnar format, efficient for analytics)
    print(f"Saving data as Parquet to {{output_path}}.parquet")
    enhanced_df.coalesce(1).write.mode("overwrite").parquet(f"{{output_path}}.parquet")
    
    # Save as JSON for compatibility
    print(f"Saving data as JSON to {{output_path}}.json")
    enhanced_df.coalesce(1).write.mode("overwrite").json(f"{{output_path}}.json")
    
    # Save summary statistics
    print(f"Saving city statistics to {{output_path}}_city_stats.parquet")
    city_stats.coalesce(1).write.mode("overwrite").parquet(f"{{output_path}}_city_stats.parquet")
    
    # Create a detailed report
    report_data = [
        f"Weather Data Processing Report",
        f"Generated on: {datetime.now().isoformat()}",
        f"Processing timestamp: {timestamp}",
        f"",
        f"Dataset Overview:",
        f"- Total records processed: {{enhanced_df.count()}}",
        f"- Cities covered: {{enhanced_df.select('city').distinct().count()}}",
        f"- Temperature range: {{temp_stats['min_temp']}}°C to {{temp_stats['max_temp']}}°C",
        f"- Average temperature: {{temp_stats['avg_temp']:.1f}}°C",
        f"",
        f"Output files generated:",
        f"- Parquet: {{output_path}}.parquet",
        f"- JSON: {{output_path}}.json", 
        f"- City Stats: {{output_path}}_city_stats.parquet"
    ]
    
    # Save report
    report_path = f"{{output_path}}_processing_report.txt"
    with open(report_path, 'w') as f:
        f.write('\\n'.join(report_data))
    
    print(f"Processing report saved to {{report_path}}")
    print("Spark weather data processing completed successfully!")
    
except Exception as e:
    print(f"Error during Spark processing: {{str(e)}}")
    raise e
finally:
    spark.stop()
"""
    
    # Save Spark script
    script_path = f"{DATA_DIR}/spark_weather_job_{timestamp}.py"
    with open(script_path, 'w') as f:
        f.write(spark_script)
    
    print(f"Spark job script saved to {script_path}")
    context['task_instance'].xcom_push(key='spark_script_path', value=script_path)
    
    return script_path

def print_context(**kwargs):
    """Print context information"""
    print("Spark Weather ETL Pipeline - Context Information")
    print(f"Execution date: {kwargs['ds']}")
    print(f"Task instance: {kwargs['task_instance']}")
    return 'Context printed successfully'

# Define tasks
start_task = PythonOperator(
    task_id='start_task',
    python_callable=print_context,
    dag=dag,
)

# Create necessary directories
create_directories = BashOperator(
    task_id='create_directories',
    bash_command=f'mkdir -p {DATA_DIR} && echo "Directories created successfully"',
    dag=dag,
)

# Extract weather data
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag,
)

# Prepare Spark job
prepare_spark_task = PythonOperator(
    task_id='prepare_spark_job',
    python_callable=prepare_spark_job,
    dag=dag,
)

# Run Spark job using BashOperator
spark_job = BashOperator(
    task_id='run_spark_job',
    bash_command='''
    TIMESTAMP="{{ task_instance.xcom_pull(task_ids='extract_weather_data', key='timestamp') }}"
    SCRIPT_PATH="{{ task_instance.xcom_pull(task_ids='prepare_spark_job', key='spark_script_path') }}"
    
    echo "Running Spark job with timestamp: $TIMESTAMP"
    echo "Using script: $SCRIPT_PATH"
    
    # Run Spark job (adjust spark-submit path as needed)
    spark-submit \\
        --master local[*] \\
        --conf spark.sql.adaptive.enabled=true \\
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
        --driver-memory 2g \\
        --executor-memory 2g \\
        "$SCRIPT_PATH"
    
    if [ $? -eq 0 ]; then
        echo "Spark job completed successfully"
    else
        echo "Spark job failed"
        exit 1
    fi
    ''',
    dag=dag,
)

# Data validation task
validate_output = BashOperator(
    task_id='validate_spark_output',
    bash_command='''
    TIMESTAMP="{{ task_instance.xcom_pull(task_ids='extract_weather_data', key='timestamp') }}"
    OUTPUT_DIR="{{ params.data_dir }}/processed_weather_$TIMESTAMP"
    
    echo "Validating Spark job output..."
    
    # Check if Parquet files exist
    if [ -d "$OUTPUT_DIR.parquet" ]; then
        echo "✓ Parquet output found"
        FILE_COUNT=$(find "$OUTPUT_DIR.parquet" -name "*.parquet" | wc -l)
        echo "  - Parquet files: $FILE_COUNT"
    else
        echo "✗ Parquet output missing"
        exit 1
    fi
    
    # Check if JSON files exist
    if [ -d "$OUTPUT_DIR.json" ]; then
        echo "✓ JSON output found"
    else
        echo "✗ JSON output missing"
        exit 1
    fi
    
    # Check if city stats exist
    if [ -d "${OUTPUT_DIR}_city_stats.parquet" ]; then
        echo "✓ City statistics found"
    else
        echo "✗ City statistics missing"
        exit 1
    fi
    
    # Check processing report
    if [ -f "${OUTPUT_DIR}_processing_report.txt" ]; then
        echo "✓ Processing report found"
        echo "Report contents:"
        cat "${OUTPUT_DIR}_processing_report.txt"
    else
        echo "✗ Processing report missing"
    fi
    
    echo "Output validation completed successfully"
    ''',
    params={'data_dir': DATA_DIR},
    dag=dag,
)

# Cleanup old files (keep last 7 days)
cleanup_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command=f'''
    echo "Cleaning up old weather data files..."
    find {DATA_DIR} -name "raw_weather_*" -type f -mtime +7 -delete
    find {DATA_DIR} -name "processed_weather_*" -type d -mtime +7 -exec rm -rf {{}} + 2>/dev/null || true
    find {DATA_DIR} -name "spark_weather_job_*" -type f -mtime +7 -delete
    echo "Cleanup completed"
    ''',
    dag=dag,
)

# Define task dependencies
start_task >> create_directories >> extract_task >> prepare_spark_task >> spark_job >> validate_output >> cleanup_task