
from flask import Flask, render_template
import json
import requests
app = Flask(__name__)

@app.route('/')
def index():
  return 'Mikkel\'s API that converts Neo4j dataresponses to the right format for Grafana.'

@app.route('/nodes')
def nodes_without_label():
  URL = "http://localhost:7474/db/neo4j/tx"

  data = {
    "statements": [
        {
            "statement": "MATCH (n) RETURN n"
        }
    ]
  }

  r = requests.post(url = URL, auth=('neo4j', 'test'), json=data).json()
  nodes = []
  for i in r['results'][0]['data']:
    item = dict(i['row'][0], **i['meta'][0])
    item.pop('deleted', None)
    nodes.append(item)

  return json.dumps(nodes)

@app.route('/nodes/<label>')
def nodes_with_label(label):
  URL = "http://localhost:7474/db/neo4j/tx"

  data = {
    "statements": [
        {
            "statement": "MATCH (n:%s) RETURN n" % label.capitalize()
        }
    ]
  }

  r = requests.post(url = URL, auth=('neo4j', 'test'), json=data).json()
  nodes = []
  for i in r['results'][0]['data']:
    item = dict(i['row'][0], **i['meta'][0])
    item.pop('deleted', None)
    item['label'] = label
    nodes.append(item)

  return json.dumps(nodes)

@app.route('/edges')
def edges_without_label():
  URL = "http://localhost:7474/db/neo4j/tx"

  data = {
    "statements": [
        {
            "statement": "MATCH p=()-->() RETURN p"
        }
    ]
  }

  r = requests.post(url = URL, auth=('neo4j', 'test'), json=data).json()
  nodes = []
  for i in r['results'][0]['data']:
      row = fix_row(i['row'][0])
      meta = fix_meta(i['meta'][0])
      item = dict(row, **meta)
      nodes.append(item)

  return json.dumps(nodes)

@app.route('/edges/<label>')
def edges_with_label(label):
  URL = "http://localhost:7474/db/neo4j/tx"

  data = {
    "statements": [
        {
            "statement": "MATCH p=()-[r:%s]->() RETURN p" % label.upper()
        }
    ]
  }

  r = requests.post(url = URL, auth=('neo4j', 'test'), json=data).json()
  nodes = []
  for i in r['results'][0]['data']:
      row = fix_row(i['row'][0])
      meta = fix_meta(i['meta'][0])
      item = dict(row, **meta)
      item['label'] = label
      nodes.append(item)

  return json.dumps(nodes)

@app.route('/full')
def full_without_label():
  URL = "http://localhost:7474/db/neo4j/tx"

  data = {
    "statements": [
        {
            "statement": "MATCH (n)-[r]->(m) RETURN n,r,m"
        }
    ]
  }

  r = requests.post(url = URL, auth=('neo4j', 'test'), json=data).json()
  nodes = []
  for i in r['results'][0]['data']:
      node1 = dict(i['row'][0], **i['meta'][0])
      node2 = dict(i['row'][2], **i['meta'][2])
      relation = dict(i['row'][1], **fix_meta(i['meta']))
      nodes.append(node1)
      nodes.append(relation)
      nodes.append(node2)

  return json.dumps(nodes)

def fix_meta(meta):
    meta[0].pop('deleted', None)
    meta[1].pop('deleted', None)
    meta[2].pop('deleted', None)
    meta[0].pop('type', None)
    meta[2].pop('type', None)
    meta[0]['source'] =  meta[0].pop('id')
    meta[2]['target'] =  meta[2].pop('id')
    items = dict(meta[0], **meta[1], **meta[2])
    return items

def fix_row(row):
    items = dict(row[0], **row[1], **row[2])
    return items