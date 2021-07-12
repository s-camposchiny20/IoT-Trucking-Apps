import json
import time
import pandas as pd
import sqlalchemy
from kafka import KafkaConsumer, KafkaProducer

KAFKA_BROKER_URL = '172.18.0.3:6667'
ENGINE = sqlalchemy.create_engine('hive://maria_dev:maria_dev@172.18.0.2:10000/default',
                                  connect_args={'auth': 'LDAP'})


def append_message():
    if message.topic == 'trucking_data_truck_enriched':
        truck['results'].append(message.value)
    else:
        traffic['results'].append(message.value)


def send_data():
    current_data = joined_data[joined_data['eventTime'] == event_time]
    sending_data = current_data[['eventTime', 'truckId', 'driverId', 'routeId', 'routeName',
                                 'congestionLevel', 'speed', 'latitude', 'longitude', 'foggy', 'rainy',
                                 'windy']]
    if len(sending_data) > 0:
        producer.send('trucking_data_joined', value=sending_data.to_json(orient="records"))


def prepare_and_insert_data():
    grouped_data = joined_data.groupby(by=['driverId', 'eventType']). \
        agg({'eventTime': 'count', 'speed': 'mean', 'foggy': 'sum', 'rainy': 'sum',
             'windy': 'sum', 'congestionLevel': 'mean'}).reset_index()
    renamed_data = grouped_data.rename({'driverId': 'driver_id', 'eventType': 'event_type', 'eventTime': 'event_count',
                                        'congestionLevel': 'congestion_level'}, axis=1)
    renamed_data['id'] = renamed_data['driver_id'].apply(lambda value: str(round(time.time())) + str(value))
    final_data = renamed_data.set_index('id').reset_index()
    final_data.to_sql('driver_events', ENGINE, if_exists='append', index=False, method='multi')


if __name__ == '__main__':

    topics = ['trucking_data_truck_enriched', 'trucking_data_traffic']

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))[0]
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: value.encode()
    )

    truck = {"results": []}
    traffic = {"results": []}

    event_time = None
    batch_time = None

    for message in consumer:
        if event_time is None:
            event_time = message.value['eventTime']
            batch_time = event_time

        if message.value['eventTime'] == event_time:
            append_message()
        else:
            try:
                truck_data = pd.DataFrame(truck['results'])
                traffic_data = pd.DataFrame(traffic['results'])
                joined_data = truck_data.merge(traffic_data, how='inner', on=['eventTime', 'routeId'])
            except Exception as e:
                print(e)
            else:
                send_data()
                event_time = message.value['eventTime']
                if event_time > batch_time + 10_000:
                    prepare_and_insert_data()
                    batch_time = event_time
                    truck = {"results": []}
                    traffic = {"results": []}
                append_message()
