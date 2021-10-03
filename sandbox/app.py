from flask import Flask, render_template
from py2neo import Graph
from typing import List
import pandas as pd
import json
import os

app = Flask(__name__)

DN_HOST = os.environ['NEO4J_HOST']
DB_PORT = os.environ['NEO4J_PORT']
DB_USERNAME = os.environ['NEO4J_USERNAME']
DB_PW = os.environ['NEO4J_PASSWORD']
DB_NAME = os.environ['NEO4J_DATABASE_NAME']

graph = Graph(
    host=DN_HOST,
    port=DB_PORT,
    user=DB_USERNAME,
    password=DB_PW,
    name=DB_NAME
)


def process_py2neo_df(source_df: pd.DataFrame, node_id: str) -> pd.DataFrame:
    """
        Process neo4j results py2neo to a pandas DataFrame.
    """
    return (
        pd.concat([
            source_df.drop([node_id], axis=1),
            source_df[node_id].apply(pd.Series)
        ], axis=1)
        .pipe(lambda df: df.loc[:, ~df.columns.duplicated()])
        # .pipe(pd.DataFrame.dropna)
    )


def neo4j_df_to_records(df: pd.DataFrame) -> List[object]:
    return [
        row.dropna().to_dict() for _, row in
        df.iterrows()
    ]


@app.route('/')
def index():
    return (
      """
      API to convert Py2Neo data responses to 
      the right format for Grafana.
      """
    )


@app.route('/nodes')
def nodes_without_label():
    q = "MATCH (n) RETURN n, id(n) as id"
    df: pd.DataFrame = graph.run(q).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(
            df=process_py2neo_df(source_df=df, node_id='n')
        )
    )


@app.route('/nodes/<label>')
def nodes_with_label(label):
    q = "MATCH (n:%s) RETURN n, id(n) as id" % label.capitalize()
    df: pd.DataFrame = graph.run(q).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(
            df=process_py2neo_df(source_df=df, node_id='n')
        )
    )


@app.route('/edges')
def edges_without_label():
    schema_query = """
    MATCH (n)-[r]->(m) 
    RETURN type(r) AS mainStat, id(r) as id, id(n) as source, id(m) as target
    """
    df = graph.run(schema_query).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(df=df)
    )


@app.route('/edges/<label>')
def edges_with_label(label):
    schema_query = f"""
      MATCH (n)-[r:{label.upper()}]->(m) 
      RETURN type(r) AS mainStat, id(r) as id, id(n) as source, id(m) as target
    """
    df = graph.run(schema_query).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(df=df)
    )


# DEMO SPECIFIC API QUERIES -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*
@app.route('/nodes/device/<device_name>/sensors')
def nodes_device_sensors(device_name):
    """
    Gets the nodes for the following query
    match rel=(d:Device {name: 'SLO'})-[:HAS_SENSOR]->(s:Sensor) return rel
    return rel
    """
    device_query = f"""
      match (d:Device {{name: '{device_name}'}})
      return d, id(d) as id
    """
    sensors_query = f"""
      match (d:Device {{name: '{device_name}'}})-[r:HAS_SENSOR]->(s:Sensor) 
      return s, id(s) as id
    """

    device_df = (
        graph.run(device_query).to_data_frame()
        .pipe(process_py2neo_df, node_id='d')
    )
    sensors_df = (
        graph.run(sensors_query).to_data_frame()
        .pipe(process_py2neo_df, node_id='s')
    )
    output_df = pd.concat([device_df, sensors_df])
    return json.dumps(
        neo4j_df_to_records(df=output_df)
    )


@app.route('/nodes/system/<system_name>/devices')
def nodes_system_devices(system_name):
    """
    Gets the nodes for the following query
    match rel=(s:System {name: 'Gas_Turbine_Pwr_Train'})-[r:HAS_DEVICE]-(:Device)
    return rel
    """
    system_query = f"""
      match (s:System {{name: '{system_name}'}})
      return s, id(s) as id
    """
    devices_query = f"""
      match (s:System {{name: '{system_name}'}})-[r:HAS_DEVICE]->(d:Device) 
      return d, id(d) as id
    """

    system_df = (
        graph.run(system_query).to_data_frame()
        .pipe(process_py2neo_df, node_id='s')
    )
    devices_df = (
        graph.run(devices_query).to_data_frame()
        .pipe(process_py2neo_df, node_id='d')
    )
    output_df = pd.concat([system_df, devices_df])
    return json.dumps(
        neo4j_df_to_records(df=output_df)
    )


@app.route('/edges/device/<device_name>/sensors')
def edges_device_sensors(device_name):
    """
    Gets the edges for the following query
    match rel=(d:Device {name: 'SLO'})-[:HAS_SENSOR]->(s:Sensor) return rel
    return rel
    """
    q = f"""
      match (d:Device {{name: '{device_name}'}})-[r:HAS_SENSOR]->(s:Sensor) 
      return type(r) as mainStat, id(r) as id, id(d) as source, id(s) as target
    """
    df = graph.run(q).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(df=df)
    )


@app.route('/edges/system/<device_name>/devices')
def edges_system_devices(device_name):
    """
    Gets the edges for the following query
    match rel=(s:System {name: 'Gas_Turbine_Pwr_Train'})-[r:HAS_DEVICE]-(:Device)
    return rel
    """
    q = f"""
      match (s:System {{name: '{device_name}'}})-[r:HAS_DEVICE]-(d:Device) 
      return type(r) as mainStat, id(r) as id, id(s) as source, id(d) as target
      
    """
    df = graph.run(q).to_data_frame()
    return json.dumps(
        neo4j_df_to_records(df=df)
    )


if __name__ == "__main__":
    app.run()
