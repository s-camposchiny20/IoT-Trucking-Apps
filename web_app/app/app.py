import json
import pandas as pd
import plotly.graph_objects as go
import pydeck as pdk
import sqlalchemy
import streamlit as st
from kafka import KafkaConsumer

KAFKA_BROKER_URL = '172.18.0.3:6667' 
transactions_topic = 'trucking_data_joined'

engine = sqlalchemy.create_engine('hive://maria_dev:maria_dev@172.18.0.2:10000/default',
                                  connect_args={'auth': 'LDAP'})


def render_map(data):
    return pdk.Deck(
        map_style=pdk.map_styles.ROAD, 
        initial_view_state=pdk.ViewState(
            latitude=38.653285,
            longitude=-90.3835452,
            zoom=4.8,
            pitch=0,
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=data,
                get_position='[longitude, latitude]',
                get_color='[200, 30, speed * 2.5, 200]',
                get_radius=10000
            )
        ]
    )


def get_risk_factor(engine):
    query = 'SELECT * FROM driver_riskfactor'
    driver_riskfactor = pd.read_sql(query, engine)
    driver_riskfactor['normal_events'] = driver_riskfactor['total_events'] - driver_riskfactor['abnormal_events']
    driver_riskfactor['risk_factor'] = driver_riskfactor['risk_factor'].apply(lambda value: round(value, 2))
    return driver_riskfactor


def render_plot(data):
    fig = go.Figure(data=[
        go.Bar(name='Normal', x=data['driver_id'], y=data['normal_events'],
               hovertext=data['risk_level'] + ' risk'),
        go.Bar(name='Abnormal', x=data['driver_id'], y=data['abnormal_events'],
               hovertext=data['risk_level'] + ' risk', text=data['risk_factor'],
               textposition='outside')
    ])
    fig.update_layout(barmode='stack')
    return fig


st.title('Live Truck Map')

st.markdown('This live map shows the current location of all trucks. Additional information about the driver, route, '
            'speed and traffic is given.')

consumer = KafkaConsumer(
    transactions_topic,
    bootstrap_servers=KAFKA_BROKER_URL,
    value_deserializer=lambda value: json.loads(value)
)

latitude = None
longitude = None

dataframe_placeholder = st.empty()
map_placeholder = st.empty()

st.title('Driver Risk Factor')
st.markdown('This plot shows information about each driver and their risk factor.')

risk_factor = get_risk_factor(engine)
plot = render_plot(risk_factor)
st.plotly_chart(plot)

for message in consumer:
    data = pd.DataFrame(message.value)
    with dataframe_placeholder:
        st.dataframe(data[['driverId', 'routeName', 'speed', 'congestionLevel']].sort_values(by='driverId'))
    with map_placeholder:
        geo = render_map(data)
        st.pydeck_chart(geo)
