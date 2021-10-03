from flask import Flask, render_template
from py2neo import Graph
import pandas as pd
import json
import os
app = Flask(__name__)

DN_HOST = os.environ['MY_HOST']
DB_PORT = os.environ['MY_PORT']
DB_USERNAME = os.environ['MY_USERNAME']
DB_PW = os.environ['MY_PASSWORD']
DB_NAME = os.environ['DATABASE_NAME']


graph = Graph(
  host=DN_HOST,
  port=DB_PORT,
  user=DB_USERNAME,
  password=DB_PW,
  name=DB_NAME
)


@app.route('/')
def index():
  return 'Mikkel\'s API that converts Neo4j dataresponses to the right format for Grafana.'


@app.route('/nodes')
def nodes_without_label():
  schema_query = "MATCH (n) RETURN n, id(n) as id"
  df = graph.run(schema_query).to_data_frame()
  df = pd.concat([df.drop(['n'], axis=1), df['n'].apply(pd.Series)], axis=1)
  df = df.loc[:,~df.columns.duplicated()]
  
  return json.dumps([row.dropna().to_dict() for index,row in df.iterrows()])


@app.route('/nodes/<label>')
def nodes_with_label(label):
  schema_query = "MATCH (n:%s) RETURN n, id(n) as id" % label.capitalize()
  df = graph.run(schema_query).to_data_frame()
  df = pd.concat([df.drop(['n'], axis=1), df['n'].apply(pd.Series)], axis=1)
  df = df.loc[:,~df.columns.duplicated()]
  
  return json.dumps([row.dropna().to_dict() for index,row in df.iterrows()])


@app.route('/edges')
def edges_without_label():
  schema_query = "MATCH (n)-[r]->(m) RETURN type(r) AS mainStat, id(r) as id, id(n) as source, id(m) as target"
  df = graph.run(schema_query).to_data_frame()

  return json.dumps([row.to_dict() for index,row in df.iterrows()])


@app.route('/edges/<label>')
def edges_with_label(label):
  schema_query = "MATCH (n)-[r:%s]->(m) RETURN type(r) AS mainStat, id(r) as id, id(n) as source, id(m) as target" % label.upper()
  df = graph.run(schema_query).to_data_frame()

  return json.dumps([row.to_dict() for index,row in df.iterrows()])
