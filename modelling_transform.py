import pandas as pd

def add_prefix(df, prefix, exclude_cols):
    """Renames columns by adding a prefix, except for excluded columns."""
    return df.rename(columns=lambda x: f'{prefix}_{x}' if x not in exclude_cols else x)

def merge_zone_data(taxi_df, zone_df, location_id_col, prefix):
    """Merges zone data with taxi data, adding a prefix to zone data columns."""
    zone_df_prefixed = add_prefix(zone_df, prefix, ['LocationID'])
    return taxi_df.merge(zone_df_prefixed, how='left', left_on=location_id_col, right_on='LocationID').drop('LocationID', axis=1)

def create_dimension_tables(df):
    """Creates various dimension tables from the main dataframe."""
    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    dim_datetime = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].copy()
    dim_datetime['pick_hour'] = df['tpep_pickup_datetime'].dt.hour
    dim_datetime['pick_day'] = df['tpep_pickup_datetime'].dt.day
    dim_datetime['pick_weekday'] = df['tpep_pickup_datetime'].dt.weekday
    dim_datetime['pick_month'] = df['tpep_pickup_datetime'].dt.month
    dim_datetime['pick_year'] = df['tpep_pickup_datetime'].dt.year
    dim_datetime['drop_hour'] = df['tpep_dropoff_datetime'].dt.hour
    dim_datetime['drop_day'] = df['tpep_dropoff_datetime'].dt.day
    dim_datetime['drop_weekday'] = df['tpep_dropoff_datetime'].dt.weekday
    dim_datetime['drop_month'] = df['tpep_dropoff_datetime'].dt.month
    dim_datetime['drop_year'] = df['tpep_dropoff_datetime'].dt.year
    dim_datetime['datetime_id'] = dim_datetime.index
    
    dim_passenger_count = df[['passenger_count']].copy()
    dim_passenger_count['passenger_count_id'] = dim_passenger_count.index
    
    dim_trip_distance = df[['trip_distance']].copy()
    dim_trip_distance['trip_distance_id'] = dim_trip_distance.index

    rate_code_type = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride"
    }
    dim_rate_code = df[['RatecodeID']].copy()
    dim_rate_code['rate_code_id'] = dim_rate_code.index
    dim_rate_code['rate_code_name'] = dim_rate_code['RatecodeID'].map(rate_code_type)
    
    dim_pickup_location = df[['PULocationID']].copy()
    dim_pickup_location['pickup_location_id'] = dim_pickup_location.index
    
    dim_dropoff_location = df[['DOLocationID']].copy()
    dim_dropoff_location['dropoff_location_id'] = dim_dropoff_location.index
    
    payment_type_name = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    dim_payment_type = df[['payment_type']].copy()
    dim_payment_type['payment_type_id'] = dim_payment_type.index
    dim_payment_type['payment_type_name'] = dim_payment_type['payment_type'].map(payment_type_name)
    
    return df, dim_datetime, dim_passenger_count, dim_trip_distance, dim_rate_code, dim_pickup_location, dim_dropoff_location, dim_payment_type

def create_fact_table(df, dim_datetime, dim_passenger_count, dim_trip_distance, dim_rate_code, dim_pickup_location, dim_dropoff_location, dim_payment_type):
    """Creates the fact table by merging dimension tables with the main dataframe."""
    return df.merge(dim_passenger_count, left_on='trip_id', right_on='passenger_count_id') \
            .merge(dim_trip_distance, left_on='trip_id', right_on='trip_distance_id') \
            .merge(dim_rate_code, left_on='trip_id', right_on='rate_code_id') \
            .merge(dim_pickup_location, left_on='trip_id', right_on='pickup_location_id') \
            .merge(dim_dropoff_location, left_on='trip_id', right_on='dropoff_location_id') \
            .merge(dim_datetime, left_on='trip_id', right_on='datetime_id') \
            .merge(dim_payment_type, left_on='trip_id', right_on='payment_type_id') \
            [['trip_id', 'VendorID', 'datetime_id', 'passenger_count_id', 'trip_distance_id', 'rate_code_id',
              'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id', 'payment_type_id', 'fare_amount',
              'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']]

if __name__ == "__main__":

    """Main function to process the taxi and zone data and create the fact table."""
    taxi_df = df1.copy()
    zone_df = df2.copy()
    
    # Merge pickup and dropoff zone data
    joined_df = merge_zone_data(taxi_df, zone_df, 'PULocationID', 'PU')
    joined_df = merge_zone_data(joined_df, zone_df, 'DOLocationID', 'DO')
    
    # Create dimension tables
    df, dim_datetime, dim_passenger_count, dim_trip_distance, dim_rate_code, dim_pickup_location, dim_dropoff_location, dim_payment_type = create_dimension_tables(joined_df)
    
    # Create fact table
    fact_table = create_fact_table(df, dim_datetime, dim_passenger_count, dim_trip_distance, dim_rate_code, dim_pickup_location, dim_dropoff_location, dim_payment_type)
    
    # Store all dataframes as dictionaries in a dictionary
    all_dfs = {
        "fact_table": fact_table.to_dict(orient='dict'),
        "dim_datetime": dim_datetime.to_dict(orient='dict'),
        "dim_passenger_count": dim_passenger_count.to_dict(orient='dict'),
        "dim_trip_distance": dim_trip_distance.to_dict(orient='dict'),
        "dim_rate_code": dim_rate_code.to_dict(orient='dict'),
        "dim_pickup_location": dim_pickup_location.to_dict(orient='dict'),
        "dim_dropoff_location": dim_dropoff_location.to_dict(orient='dict'),
        "dim_payment_type": dim_payment_type.to_dict(orient='dict')
    }

    print("All dataframes have been converted to dictionaries and stored in 'all_dfs'.")
