from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import pandas as pd
import json
import os
from pathlib import Path

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'weather_data_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract weather data, transform it, and save to files',
    schedule_interval='@daily',  # Run once per day
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'weather', 'api', 'beginner']
)

# Configuration
CITIES = ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather'
# Note: You'll need to get a free API key from openweathermap.org
API_KEY = 'fd1f6f6cc6659b69dd9097beb9ac8707'  # Replace with actual API key
OUTPUT_DIR = '/tmp/weather_data'

def extract_weather_data(**context):
    """
    Extract weather data from OpenWeatherMap API for multiple cities
    """
    print("Starting weather data extraction...")
    
    weather_data = []
    
    for city in CITIES:
        try:
            # API request parameters
            params = {
                'q': city,
                'appid': API_KEY,
                'units': 'metric'  # Get temperature in Celsius
            }
            
            print(f"Fetching weather data for {city}...")
            response = requests.get(BASE_URL, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                weather_data.append(data)
                print(f"✓ Successfully fetched data for {city}")
            else:
                print(f"✗ Failed to fetch data for {city}: {response.status_code}")
                # Log error but continue with other cities
                
        except requests.exceptions.RequestException as e:
            print(f"✗ Network error for {city}: {str(e)}")
            continue
        except Exception as e:
            print(f"✗ Unexpected error for {city}: {str(e)}")
            continue
    
    if not weather_data:
        raise ValueError("No weather data was successfully extracted!")
    
    # Save raw data for transformation step
    raw_data_path = f"{OUTPUT_DIR}/raw_weather_data.json"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with open(raw_data_path, 'w') as f:
        json.dump(weather_data, f, indent=2)
    
    print(f"Raw data saved to {raw_data_path}")
    print(f"Extracted data for {len(weather_data)} cities")
    
    return raw_data_path

def transform_weather_data(**context):
    """
    Transform raw weather data into a clean, structured format
    """
    print("Starting data transformation...")
    
    # Get the raw data file path from previous task
    raw_data_path = context['task_instance'].xcom_pull(task_ids='extract_weather_data')
    
    # Load raw data
    with open(raw_data_path, 'r') as f:
        raw_data = json.load(f)
    
    # Transform data
    transformed_records = []
    
    for record in raw_data:
        try:
            # Extract and clean relevant fields
            transformed_record = {
                'city': record['name'],
                'country': record['sys']['country'],
                'temperature_celsius': round(record['main']['temp'], 1),
                'feels_like_celsius': round(record['main']['feels_like'], 1),
                'humidity_percent': record['main']['humidity'],
                'pressure_hpa': record['main']['pressure'],
                'weather_main': record['weather'][0]['main'],
                'weather_description': record['weather'][0]['description'],
                'wind_speed_mps': record.get('wind', {}).get('speed', 0),
                'wind_direction_degrees': record.get('wind', {}).get('deg', 0),
                'cloudiness_percent': record['clouds']['all'],
                'visibility_meters': record.get('visibility', 0),
                'sunrise_timestamp': record['sys']['sunrise'],
                'sunset_timestamp': record['sys']['sunset'],
                'data_timestamp': record['dt'],
                'extraction_date': datetime.now().strftime('%Y-%m-%d'),
                'extraction_datetime': datetime.now().isoformat()
            }
            
            # Add computed fields
            transformed_record['temperature_fahrenheit'] = round(
                (transformed_record['temperature_celsius'] * 9/5) + 32, 1
            )
            
            # Categorize weather conditions
            temp = transformed_record['temperature_celsius']
            if temp < 0:
                transformed_record['temperature_category'] = 'freezing'
            elif temp < 10:
                transformed_record['temperature_category'] = 'cold'
            elif temp < 20:
                transformed_record['temperature_category'] = 'cool'
            elif temp < 30:
                transformed_record['temperature_category'] = 'warm'
            else:
                transformed_record['temperature_category'] = 'hot'
            
            # Categorize humidity
            humidity = transformed_record['humidity_percent']
            if humidity < 30:
                transformed_record['humidity_category'] = 'low'
            elif humidity < 60:
                transformed_record['humidity_category'] = 'moderate'
            else:
                transformed_record['humidity_category'] = 'high'
            
            transformed_records.append(transformed_record)
            print(f"✓ Transformed data for {transformed_record['city']}")
            
        except KeyError as e:
            print(f"✗ Missing field in record for {record.get('name', 'unknown')}: {e}")
            continue
        except Exception as e:
            print(f"✗ Error transforming record for {record.get('name', 'unknown')}: {e}")
            continue
    
    if not transformed_records:
        raise ValueError("No records were successfully transformed!")
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(transformed_records)
    
    # Add data quality checks
    print(f"Data quality summary:")
    print(f"- Total records: {len(df)}")
    print(f"- Cities covered: {df['city'].nunique()}")
    print(f"- Temperature range: {df['temperature_celsius'].min()}°C to {df['temperature_celsius'].max()}°C")
    print(f"- Humidity range: {df['humidity_percent'].min()}% to {df['humidity_percent'].max()}%")
    
    # Save transformed data
    transformed_data_path = f"{OUTPUT_DIR}/transformed_weather_data.json"
    df.to_json(transformed_data_path, orient='records', indent=2)
    
    print(f"Transformed data saved to {transformed_data_path}")
    return transformed_data_path

def save_to_multiple_formats(**context):
    """
    Save the transformed data in multiple formats (CSV, JSON, Parquet)
    """
    print("Starting data saving process...")
    
    # Get transformed data path from previous task
    transformed_data_path = context['task_instance'].xcom_pull(task_ids='transform_weather_data')
    
    # Load transformed data
    df = pd.read_json(transformed_data_path)
    
    # Create timestamped directory for this run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_dir = f"{OUTPUT_DIR}/weather_data_{timestamp}"
    os.makedirs(run_dir, exist_ok=True)
    
    # Save in multiple formats
    formats_saved = []
    
    try:
        # Save as CSV
        csv_path = f"{run_dir}/weather_data.csv"
        df.to_csv(csv_path, index=False)
        formats_saved.append(f"CSV: {csv_path}")
        print(f"✓ Saved CSV file: {csv_path}")
        
        # Save as JSON (pretty formatted)
        json_path = f"{run_dir}/weather_data.json"
        df.to_json(json_path, orient='records', indent=2)
        formats_saved.append(f"JSON: {json_path}")
        print(f"✓ Saved JSON file: {json_path}")
        
        # Save as Parquet (if possible)
        try:
            parquet_path = f"{run_dir}/weather_data.parquet"
            df.to_parquet(parquet_path, index=False)
            formats_saved.append(f"Parquet: {parquet_path}")
            print(f"✓ Saved Parquet file: {parquet_path}")
        except ImportError:
            print("⚠ Parquet format not available (install pyarrow/fastparquet)")
        
        # Create a summary report
        summary_path = f"{run_dir}/data_summary.txt"
        with open(summary_path, 'w') as f:
            f.write(f"Weather Data ETL Pipeline Summary\n")
            f.write(f"Generated on: {datetime.now().isoformat()}\n")
            f.write(f"Data extraction date: {df['extraction_date'].iloc[0]}\n\n")
            f.write(f"Dataset Statistics:\n")
            f.write(f"- Total records: {len(df)}\n")
            f.write(f"- Cities: {', '.join(df['city'].unique())}\n")
            f.write(f"- Temperature range: {df['temperature_celsius'].min()}°C to {df['temperature_celsius'].max()}°C\n")
            f.write(f"- Average temperature: {df['temperature_celsius'].mean():.1f}°C\n")
            f.write(f"- Average humidity: {df['humidity_percent'].mean():.1f}%\n\n")
            f.write(f"Files generated:\n")
            for fmt in formats_saved:
                f.write(f"- {fmt}\n")
        
        formats_saved.append(f"Summary: {summary_path}")
        print(f"✓ Created summary report: {summary_path}")
        
    except Exception as e:
        print(f"✗ Error saving data: {str(e)}")
        raise
    
    print(f"Data successfully saved in {len(formats_saved)} formats")
    return run_dir

def validate_data_quality(**context):
    """
    Perform data quality checks on the final dataset
    """
    print("Starting data quality validation...")
    
    # Get the run directory from previous task
    run_dir = context['task_instance'].xcom_pull(task_ids='save_to_multiple_formats')
    csv_path = f"{run_dir}/weather_data.csv"
    
    # Load the final dataset
    df = pd.read_csv(csv_path)
    
    # Define quality checks
    quality_issues = []
    
    # Check 1: No missing critical fields
    critical_fields = ['city', 'temperature_celsius', 'humidity_percent']
    for field in critical_fields:
        missing_count = df[field].isna().sum()
        if missing_count > 0:
            quality_issues.append(f"Missing values in {field}: {missing_count}")
    
    # Check 2: Reasonable temperature ranges
    if df['temperature_celsius'].min() < -50 or df['temperature_celsius'].max() > 60:
        quality_issues.append("Temperature values outside reasonable range (-50°C to 60°C)")
    
    # Check 3: Humidity values within valid range
    if df['humidity_percent'].min() < 0 or df['humidity_percent'].max() > 100:
        quality_issues.append("Humidity values outside valid range (0-100%)")
    
    # Check 4: All expected cities present
    expected_cities = set(CITIES)
    actual_cities = set(df['city'].unique())
    missing_cities = expected_cities - actual_cities
    if missing_cities:
        quality_issues.append(f"Missing data for cities: {missing_cities}")
    
    # Check 5: Recent data (within last day)
    extraction_date = pd.to_datetime(df['extraction_date'].iloc[0])
    if (datetime.now() - extraction_date).days > 1:
        quality_issues.append("Data is more than 1 day old")
    
    # Report results
    if quality_issues:
        print("⚠ Data quality issues found:")
        for issue in quality_issues:
            print(f"  - {issue}")
        # In production, you might want to fail the pipeline or send alerts
    else:
        print("✓ All data quality checks passed!")
    
    # Save quality report
    quality_report_path = f"{run_dir}/data_quality_report.txt"
    with open(quality_report_path, 'w') as f:
        f.write(f"Data Quality Report\n")
        f.write(f"Generated on: {datetime.now().isoformat()}\n\n")
        if quality_issues:
            f.write("Issues found:\n")
            for issue in quality_issues:
                f.write(f"- {issue}\n")
        else:
            f.write("✓ All quality checks passed!\n")
        
        f.write(f"\nDataset overview:\n")
        f.write(f"- Records: {len(df)}\n")
        f.write(f"- Cities: {len(df['city'].unique())}\n")
        f.write(f"- Date range: {df['extraction_date'].min()} to {df['extraction_date'].max()}\n")
    
    print(f"Quality report saved to: {quality_report_path}")
    return len(quality_issues) == 0

# Define tasks
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_weather_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_weather_data',
    python_callable=transform_weather_data,
    dag=dag
)

save_task = PythonOperator(
    task_id='save_to_multiple_formats',
    python_callable=save_to_multiple_formats,
    dag=dag
)

quality_check_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)

# Create output directory
create_dir_task = BashOperator(
    task_id='create_output_directory',
    bash_command=f'mkdir -p {OUTPUT_DIR}',
    dag=dag
)

# Cleanup old files (keep last 7 days)
cleanup_task = BashOperator(
    task_id='cleanup_old_files',
    bash_command=f'find {OUTPUT_DIR} -name "weather_data_*" -type d -mtime +7 -exec rm -rf {{}} +',
    dag=dag
)

# Define task dependencies
create_dir_task >> extract_task >> transform_task >> save_task >> quality_check_task >> cleanup_task