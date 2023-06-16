if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    datetime_df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_df['tpep_pickup_datetime'] = datetime_df['tpep_pickup_datetime']
    datetime_df['pick_hour'] = datetime_df['tpep_pickup_datetime'].dt.hour
    datetime_df['pick_day'] = datetime_df['tpep_pickup_datetime'].dt.day
    datetime_df['pick_month'] = datetime_df['tpep_pickup_datetime'].dt.month
    datetime_df['pick_year'] = datetime_df['tpep_pickup_datetime'].dt.year
    datetime_df['pick_weekday'] = datetime_df['tpep_pickup_datetime'].dt.weekday

    datetime_df['tpep_dropoff_datetime'] = datetime_df['tpep_dropoff_datetime']
    datetime_df['drop_hour'] = datetime_df['tpep_dropoff_datetime'].dt.hour
    datetime_df['drop_day'] = datetime_df['tpep_dropoff_datetime'].dt.day
    datetime_df['drop_month'] = datetime_df['tpep_dropoff_datetime'].dt.month
    datetime_df['drop_year'] = datetime_df['tpep_dropoff_datetime'].dt.year
    datetime_df['drop_weekday'] = datetime_df['tpep_dropoff_datetime'].dt.weekday


    datetime_df['datetime_id'] = datetime_df.index

    datetime_df = datetime_df[['datetime_id', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                            'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    passenger_count_df = df[['passenger_count']].reset_index(drop=True)
    passenger_count_df['passenger_count_id'] = passenger_count_df.index
    passenger_count_df = passenger_count_df[['passenger_count_id','passenger_count']]

    trip_distance_df = df[['trip_distance']].reset_index(drop=True)
    trip_distance_df['trip_distance_id'] = trip_distance_df.index
    trip_distance_df = trip_distance_df[['trip_distance_id','trip_distance']]

    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }

    rate_code_df = df[['RatecodeID']].reset_index(drop=True)
    rate_code_df['Ratecode_id'] = rate_code_df.index
    rate_code_df['Ratecode_name'] = rate_code_df['RatecodeID'].map(rate_code_type)
    rate_code_df = rate_code_df[['Ratecode_id','Ratecode_name','RatecodeID']]

    location_df = df[['PULocationID', 'DOLocationID']].reset_index(drop=True)
    location_df['Location_id'] = location_df.index
    location_df = location_df[['Location_id', 'PULocationID', 'DOLocationID']]

    fare_df = df[['fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']].reset_index(drop=True)
    fare_df['fare_id'] = fare_df.index
    fare_df = fare_df[['fare_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']]

    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }
    payment_type_df = df[['payment_type']].reset_index(drop=True)
    payment_type_df['payment_type_id'] = payment_type_df.index
    payment_type_df['payment_type_name'] = payment_type_df['payment_type'].map(payment_type_name)
    payment_type_df = payment_type_df[['payment_type_id','payment_type','payment_type_name']]

    fact_table = df.merge(passenger_count_df, left_on='main_id', right_on='passenger_count_id') \
                .merge(trip_distance_df, left_on='main_id', right_on='trip_distance_id') \
                .merge(rate_code_df, left_on='main_id', right_on='Ratecode_id') \
                .merge(location_df, left_on='main_id', right_on='Location_id')\
                .merge(datetime_df, left_on='main_id', right_on='datetime_id') \
                .merge(fare_df, left_on='main_id', right_on='fare_id') \
                .merge(payment_type_df, left_on='main_id', right_on='payment_type_id') \
                [['main_id','VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'Ratecode_id', 'Location_id', 'fare_id',
                'payment_type_id', 'store_and_fwd_flag']]

    # dataframes = [datetime_df, passenger_count_df, trip_distance_df, rate_code_df, location_df, fare_df, payment_type_df, fact_table]

    # combined_dict = {key: value for df in dataframes for key, value in df.to_dict(orient='dict').items()}
    
    return {
        "datetime_df_2020": datetime_df.to_dict(orient='dict'),
        "passenger_count_df_2020": passenger_count_df.to_dict(orient='dict'),
        "trip_distance_df_2020": trip_distance_df.to_dict(orient='dict'),
        "rate_code_df_2020": rate_code_df.to_dict(orient='dict'),
        "location_df_2020": location_df.to_dict(orient='dict'),
        "fare_df_2020": fare_df.to_dict(orient='dict'),
        "payment_type_df_2020": payment_type_df.to_dict(orient='dict'),
        "fact_table_2020": fact_table.to_dict(orient='dict')
        }



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

