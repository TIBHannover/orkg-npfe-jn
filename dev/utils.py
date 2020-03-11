import requests
import io
import sys
import csv
import pandas as pd
import numpy as np
from io import StringIO
from urllib.parse import urlencode
from datetime import datetime, timedelta
from pytz import timezone
from matplotlib import pyplot as plt
from dateutil import tz
from hashlib import md5
from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, XSD
from rdflib.plugins.sparql.results.csvresults import CSVResultSerializer
from shortid import ShortId
from orkg import ORKG
from dateutil import parser
sid = ShortId()
host = "http://localhost:8000"
orkg = ORKG(host=host)

'''
To avoid that rdflib Graph create a new namespace (happened when the generated ID starts with a number or -)
'''
def generate_sid():
    r = sid.generate()
    while r[0].isdigit() or r.startswith(('-', '_')):
        r = sid.generate()
    return r
    
def query(g, q, dtype={}):
    serializer = CSVResultSerializer(g.query(q))
    output = io.BytesIO()
    serializer.serialize(output)
    
    return pd.read_csv(StringIO(output.getvalue().decode()), dtype=dtype)

# ORKG API endpoints
api = host+'/api/'
api_resources = '{}resources/'.format(api)
api_predicates = '{}predicates/'.format(api)
api_literals = '{}literals/'.format(api)
api_statements = '{}statements/'.format(api)
api_classes = '{}classes/'.format(api)
# The RDF Data Cube Vocabulary index
cube = dict()
cube['DataSet'] = URIRef('http://purl.org/linked-data/cube#DataSet')
cube['DataStructureDefinition'] = URIRef('http://purl.org/linked-data/cube#DataStructureDefinition')
cube['Observation'] = URIRef('http://purl.org/linked-data/cube#Observation')
cube['ComponentSpecification'] = URIRef('http://purl.org/linked-data/cube#ComponentSpecification')
cube['ComponentProperty'] = URIRef('http://purl.org/linked-data/cube#ComponentProperty')
cube['DimensionProperty'] = URIRef('http://purl.org/linked-data/cube#DimensionProperty')
cube['MeasureProperty'] = URIRef('http://purl.org/linked-data/cube#MeasureProperty')
cube['AttributeProperty'] = URIRef('http://purl.org/linked-data/cube#AttributeProperty')
cube['dataSet'] = URIRef('http://purl.org/linked-data/cube#dataSet')
cube['structure'] = URIRef('http://purl.org/linked-data/cube#structure')
cube['component'] = URIRef('http://purl.org/linked-data/cube#component')
cube['componentProperty'] = URIRef('http://purl.org/linked-data/cube#componentProperty')
cube['componentAttachment'] = URIRef('http://purl.org/linked-data/cube#componentAttachment')
cube['dimension'] = URIRef('http://purl.org/linked-data/cube#dimension')
cube['attribute'] = URIRef('http://purl.org/linked-data/cube#attribute')
cube['measure'] = URIRef('http://purl.org/linked-data/cube#measure')
cube['order'] = URIRef('http://purl.org/linked-data/cube#order')
# Vocabulary Classes
cube_classes = [cube['DataSet'],cube['DataStructureDefinition'],cube['Observation'],cube['ComponentSpecification'],
              cube['ComponentProperty'],cube['DimensionProperty'],cube['MeasureProperty'],cube['AttributeProperty']]  

def getTitle(g):
    res = query(g, """
        PREFIX bibo: <http://purl.org/ontology/bibo/>
        SELECT ?title   WHERE {
          ?a a bibo:Article .  
          ?a dc:title ?title . 
        }
    """)
    return res.loc[0].title if (res.size > 0) else ''

def getDoi(g):
    res = query(g, """
        PREFIX bibo: <http://purl.org/ontology/bibo/>
        SELECT ?doi   WHERE {
          ?a a bibo:Article .  
          ?a bibo:doi ?doi .
        }
    """)
    return res.loc[0].doi if (res.size > 0) else ''

def getDate(g):
    res = query(g, """
        PREFIX bibo: <http://purl.org/ontology/bibo/>
        SELECT ?date   WHERE {
          ?a a bibo:Article .  
          ?a dc:date ?date .
        }
    """)
    return str(res.loc[0].date) if (res.size > 0) else ''

def getResearchField(g):
    res = query(g, """
        PREFIX orkg: <http://orkg.org/core#>
        SELECT ?researchField   WHERE {
          ?a a bibo:Article .  
          ?a orkg:hasResearchField ?researchField .
        }
    """)
    return res.loc[0].researchField.split('#')[-1] if (res.size > 0) else ''

def getAuthors(g):
    return [{'label':'{0} {1}'.format(author['firstname'], author['lastname'])} for index, author in query(g, """
        PREFIX bibo: <http://purl.org/ontology/bibo/>
        PREFIX dc: <http://purl.org/dc/terms/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        SELECT ?firstname ?lastname
        WHERE {
          ?a a bibo:Article .                              
          ?a dc:creator ?c .
          ?c foaf:givenname ?firstname .
          ?c foaf:family_name ?lastname .
          ?c schema:position ?position .
        }
        ORDER BY xsd:integer(?position)
    """).iterrows()]

def getLabelOfNode(g,nodeID):
    return g.value(BNode(nodeID), URIRef('http://orkg.org/core#label'))


yieldsPredicate = 'P1'
researchProblemPredicate = 'P32'
qbDatasetClass = 'QBDataset'
def getResearchProblems(g, contributionID):
    researchProblems = [ResearchProblem for s, p , ResearchProblem in g.triples((BNode(contributionID), URIRef('http://orkg.org/core#addresses'), None))]
    researchProblemsArray=[]
    for researchProblemID in researchProblems:
        researchProblemLabel = getLabelOfNode(g,researchProblemID)
        if not researchProblemLabel:
            researchProblemResource = orkg.resources.by_id(researchProblemID.split('#')[-1])
            if not researchProblemResource:
                raise Exception('Research problem not found : {}'.format(researchProblemID.split('#')[-1]))
            else:
                researchProblemsArray.append({"@id": researchProblemID.split('#')[-1]})
        else:
            researchProblemsArray.append({"label": researchProblemLabel.value})
    return researchProblemsArray

def getResearchResults(g, contributionID):
    researchResults = [ResearchResult for s, p , ResearchResult in g.triples((BNode(contributionID), URIRef('http://orkg.org/core#yields'), None))]
    researchResultsArray=[]
    for researchResultID in researchResults:
        researchResultLabel = getLabelOfNode(g,researchResultID)
        if not researchResultLabel:
            researchResultResource = orkg.resources.by_id(researchResultID.split(':')[-1])
            if not researchResultResource:
                raise Exception('Research result not found : {}'.format(researchResultID.split(':')[-1]))
            else:
                researchResultsArray.append({"@id": researchResultID.split('#')[-1]})
        else:
            researchResultsArray.append({"label": researchResultLabel.value})
    return researchResultsArray


def save_paper(g):
    contributionIDs = [c['ResearchContribution'] for index,c in query(g, """
        PREFIX bibo: <http://purl.org/ontology/bibo/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?ResearchContribution WHERE {
          ?a a bibo:Article .                              
          ?a orkg:hasResearchContribution ?ResearchContribution .
        }
    """).iterrows()]
    
    paper = {
        "predicates": [],
        "paper": {
            "title": getTitle(g),
            "doi": getDoi(g),
            "authors": getAuthors(g),
            "publicationMonth": parser.parse(getDate(g)).month if len(getDate(g)) > 4 else '',
            "publicationYear": parser.parse(getDate(g)).year if len(getDate(g)) > 4 else getDate(g),
            "researchField": getResearchField(g),
            "contributions":[
                {
                    "name": getLabelOfNode(g,contributionID) if getLabelOfNode(g,contributionID) else "Contribution {0}".format(index+1),
                    "values":{
                        yieldsPredicate: getResearchResults(g, contributionID), 
                        researchProblemPredicate: getResearchProblems(g, contributionID)
                    }
                } 
                for index, contributionID in enumerate(contributionIDs)
            ]
        }
    }
    return orkg.papers.add(paper)


def store(g):
    resources = {}
    predicates = {}
    for s, p, o in g:    
        if ((p.n3() in [RDF.type.n3()]) and o in cube_classes) or p.n3() in [RDFS.label.n3()]:
            continue 
            
        resources, s_id = get_id(resources, api_resources, s, g)
        resources, p_id = get_id(resources, api_predicates, p, g)
        
        if type(o) is Literal:
            cls = 'literal'
            resources, o_id = get_id(resources, api_literals, o, g)
        else:
            cls = 'resource'
            resources, o_id = get_id(resources, api_resources, o, g)
        
        requests.post(api_statements,
                      json={'subject_id': s_id, 'predicate_id': p_id, 'object_id': o_id}, 
                      headers={'Content-Type':'application/json'})
    dataset_node = str([s for s, p, o in g.triples((None, RDF.type, cube['DataSet']))][0])
    return resources[dataset_node]


def get_id(resources, api, r, g):
    if not str(r) in resources or api==api_predicates:
        l = None
        l_classes = []
        if type(r) is Literal:
            l = r   
        else:
            l = g.value(r, RDFS.label)
            Ts = [o for s, p , o in g.triples( (r, RDF.type, None))]
            # Set the classes of a ressource
            for T in Ts:
                if(T in cube_classes):
                    lc = 'qb:'+T.split('#')[-1]
                    if lc != 'qb:DataSet': # Beceause we use a fixed ID for qb:DataSet class
                        l_class = requests.get(api_classes, params={'q':lc, 'exact': 'true'}, headers={'Content-Type':'application/json', 'Accept':'application/json'}).json()
                        if len(l_class) == 0:
                            l_classes.append(requests.post(api_classes, json={'label':lc}, headers={'Content-Type':'application/json'}).json()['id'])
                        if len(l_class) == 1:
                            l_classes.append(l_class[0]['id'])
                    else:
                        res = requests.get(api_classes+qbDatasetClass+'/', headers={'Content-Type':'application/json', 'Accept':'application/json'})
                        if res.status_code == 404:
                            l_classes.append(requests.post(api_classes, json={'label':lc, 'id':qbDatasetClass}, headers={'Content-Type':'application/json'}).json()['id'])
                        else:
                            l_classes.append(res.json()['id'])

            # Name a predicate with the ressource ID
            if (api==api_predicates):
                if len(Ts) > 0:
                    if not str(r) in resources:
                        l = requests.post(api_resources, json={'label':l, 'classes':l_classes}, headers={'Content-Type':'application/json'}).json()['id']
                        resources[str(r)] = l
                    else:
                        l = resources[str(r)]

        if l is None:
            raise Exception('Label is none for resource {}'.format(r))
        if (api==api_predicates):
            j = requests.get(api, params={'q':l, 'exact': 'true'}, headers={'Content-Type':'application/json', 'Accept':'application/json'}).json()
            if len(j) == 0:
                return resources, requests.post(api, json={'label':l, 'classes':l_classes}, headers={'Content-Type':'application/json'}).json()['id']
            if len(j) == 1:
                return resources, j[0]['id']
        else:
            resource_id = requests.post(api, json={'label':l, 'classes':l_classes}, headers={'Content-Type':'application/json'}).json()['id']
            resources[str(r)] = resource_id
            return resources, resource_id
    else:
        return resources, resources[str(r)]



def save_dataset(dataset, title, dimensions):
    gds = Graph()
    # Vocabulary properties labels
    gds.add((RDF.type, RDFS.label, Literal('type')))
    gds.add((RDFS.label, RDFS.label, Literal('label')))
    gds.add((cube['dataSet'] , RDFS.label, Literal('dataSet')))
    gds.add((cube['structure'] , RDFS.label, Literal('structure')))
    gds.add((cube['component'] , RDFS.label, Literal('component')))  
    gds.add((cube['componentProperty'] , RDFS.label, Literal('component Property')))
    gds.add((cube['componentAttachment'] , RDFS.label, Literal('component Attachment')))
    gds.add((cube['dimension'] , RDFS.label, Literal('dimension')))
    gds.add((cube['attribute'] , RDFS.label, Literal('attribute')))
    gds.add((cube['measure'] , RDFS.label, Literal('measure')))
    gds.add((cube['order'] , RDFS.label, Literal('order')))
    # Name spaces
    gds.bind('orkg', 'http://orkg.org/vocab/')
    gds.bind('esuc', 'http://orkg.org/vocab/esuc/')
    gds.bind('qb', 'http://purl.org/linked-data/cube#')
    # BNodes
    ds = URIRef('http://orkg.org/vocab/esuc/{}'.format(generate_sid())) # theDataset
    dsd = URIRef('http://orkg.org/vocab/esuc/{}'.format(generate_sid())) # theDataStructureDefinition

    '''
    Dataset
    '''
    gds.add((ds, RDF.type, cube['DataSet']))
    gds.add((ds, RDFS.label, Literal(str(title))))
    gds.add((ds, cube['structure'], dsd))
    '''
    DataStructureDefinition
    '''
    gds.add((dsd, RDF.type, cube['DataStructureDefinition']))
    gds.add((dsd, RDFS.label, Literal('Data Structure Definition ESUC')))
        
    cs = dict()
    dt = dict()
    for index, column in enumerate(dataset.columns, start=1):
        cs[column] = URIRef('http://orkg.org/vocab/esuc/{}'.format(generate_sid()))
        dt[column] = URIRef('http://orkg.org/vocab/esuc/{}'.format(generate_sid()))
        gds.add((dsd, cube['component'], cs[column]))
        
        gds.add((cs[column], RDF.type, cube['ComponentSpecification']))
        gds.add((cs[column], RDFS.label, Literal('Component Specification '+column)))
        gds.add((cs[column], cube['order'], Literal(index)))

        if column in dimensions:
            gds.add((cs[column], cube['dimension'], dt[column]))
            gds.add((dt[column], RDF.type, cube['DimensionProperty']))
        else:
            gds.add((cs[column], cube['measure'], dt[column]))
            gds.add((dt[column], RDF.type, cube['MeasureProperty']))
        gds.add((dt[column], RDF.type, cube['ComponentProperty']))
        gds.add((dt[column], RDFS.label, Literal(column)))
        
    for index, row in dataset.iterrows():
        bno = URIRef('http://orkg.org/vocab/esuc/{}'.format(generate_sid()))
        gds.add((bno, RDF.type, cube['Observation']))
        gds.add((bno, RDFS.label, Literal('Observation #{}'.format(index+1))))
        gds.add((bno, cube['dataSet'], ds))
        for column in dataset.columns:
            gds.add((bno, dt[column], Literal(str(row[column]))))
    dataset_resource_id = store(gds)
    return dataset_resource_id