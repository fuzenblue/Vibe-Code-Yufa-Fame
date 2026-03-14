import pandas as pd
import os
from airflow.models import Variable

def _get_data_dir():
    """Return the pipeline's data/ directory (creating it if needed)."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir

def load_taxi_model(**context):
    """
    Load transformed taxi data into star-schema CSV files.

    Args:
        **context: Airflow context dictionary
    """
    # Pull transformed CSV path from XCom
    ti = context['ti']
    transformed_path = ti.xcom_pull(task_ids='transform_taxi_data')

    # Load data with pandas
    df = pd.read_csv(transformed_path)

    # Calculate additional measures
    df['speed_mph'] = df['trip_distance'] / (df['trip_duration_minutes'] / 60)
    df['fare_per_mile'] = df['fare_amount'] / df['trip_distance'].where(df['trip_distance'] > 0, 1)

    # Prepare dim_time: unique hours/days/weekends lookup
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    dim_time_df = df[['pickup_datetime']].copy()
    dim_time_df['hour'] = dim_time_df['pickup_datetime'].dt.hour
    dim_time_df['day_of_week'] = dim_time_df['pickup_datetime'].dt.dayofweek
    dim_time_df['is_weekend'] = dim_time_df['day_of_week'] >= 5
    dim_time_df = dim_time_df.drop_duplicates(subset=['hour', 'day_of_week', 'is_weekend']).reset_index(drop=True)
    dim_time_df['time_id'] = dim_time_df.index + 1
    dim_time_df = dim_time_df[['time_id', 'hour', 'day_of_week', 'is_weekend']]

    # Save dim_time to CSV
    data_dir = _get_data_dir()
    dim_time_path = os.path.join(data_dir, 'dim_time.csv')
    dim_time_df.to_csv(dim_time_path, index=False)

    # Prepare dim_payment: payment_type dimension
    dim_payment_df = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment_df['payment_id'] = dim_payment_df.index + 1
    dim_payment_df = dim_payment_df[['payment_id', 'payment_type']]

    # Save dim_payment to CSV
    dim_payment_path = os.path.join(data_dir, 'dim_payment.csv')
    dim_payment_df.to_csv(dim_payment_path, index=False)

    # Prepare fact_trips: grain is one row per trip with FKs to dimensions + measures
    fact_df = df.copy()
    fact_df['hour'] = fact_df['pickup_datetime'].dt.hour
    fact_df['day_of_week'] = fact_df['pickup_datetime'].dt.dayofweek
    fact_df['is_weekend'] = fact_df['day_of_week'] >= 5

    # Merge time_id
    fact_df = fact_df.merge(dim_time_df[['time_id', 'hour', 'day_of_week', 'is_weekend']],
                           on=['hour', 'day_of_week', 'is_weekend'], how='left')

    # Merge payment_id
    fact_df = fact_df.merge(dim_payment_df[['payment_id', 'payment_type']],
                           on='payment_type', how='left')

    # Select columns for fact table
    fact_df = fact_df[['time_id', 'payment_id', 'fare_amount', 'trip_distance',
                      'trip_duration_minutes', 'speed_mph', 'fare_per_mile', 'passenger_count']]
    fact_df['trip_id'] = fact_df.index + 1
    fact_df = fact_df[['trip_id', 'time_id', 'payment_id', 'fare_amount', 'trip_distance',
                      'trip_duration_minutes', 'speed_mph', 'fare_per_mile', 'passenger_count']]

    # Save fact_trips to CSV
    fact_trips_path = os.path.join(data_dir, 'fact_trips.csv')
    fact_df.to_csv(fact_trips_path, index=False)

    # Log row counts for each table after save
    print(f"Saved {len(dim_time_df)} rows to dim_time.csv")
    print(f"Saved {len(dim_payment_df)} rows to dim_payment.csv")
    print(f"Saved {len(fact_df)} rows to fact_trips.csv")

    # Return paths or summary
    return {
        'dim_time_path': dim_time_path,
        'dim_payment_path': dim_payment_path,
        'fact_trips_path': fact_trips_path
    }