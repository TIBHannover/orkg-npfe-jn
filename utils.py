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
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, RDFS, XSD
from rdflib.plugins.sparql.results.csvresults import CSVResultSerializer


configuration = {
    'Hyytiälä': {
        'smear_table': 'HYY_DMPS',
        'smear_variables': 'd316e1,d355e1,d398e1,d447e1,d501e1,d562e1,d631e1,d708e1,d794e1,'\
                           'd891e1,d100e2,d112e2,d126e2,d141e2,d158e2,d178e2,d200e2,d224e2,'\
                           'd251e2,d282e2,d316e2,d355e2,d398e2,d447e2,d501e2,d562e2,d631e2,'\
                           'd708e2,d794e2,d891e2,d100e3,d112e3,d126e3,d141e3,d158e3,d178e3,d200e3',
        'num_var_less_than_10nm': 10,
        'identifier': URIRef('http://sws.geonames.org/656888/'),
        'name': 'Hyytiälä',
        'encoded_name':'hyytiaelae',
        'countryCode': 'FI',
        'locationMap': URIRef('http://www.geonames.org/656888/hyytiaelae.html'),
        'latitude': '61.84562',
        'longitude': '24.29077',
        'package_id_descriptions': 'npfe_descriptions_at_hyytiaelae',
        'package_id_plots': 'npfe_plots_at_hyytiaelae'
    },
    'Puijo': {
        'smear_table': 'PUI_dmps_tot',
        'smear_variables': 'ch01,ch02,ch03,ch04,ch05,ch06,ch07,ch08,ch09,ch10,ch11,ch12,ch13,ch14,ch15,ch16,'\
                           'ch17,ch18,ch19,ch20,ch21,ch22,ch23,ch24,ch25,ch26,ch27,ch28,ch29,ch30,ch31,ch32,'\
                           'ch33,ch34,ch35,ch36,ch37,ch38,ch39,ch40',
        'num_var_less_than_10nm': 10,
        'identifier': URIRef('http://sws.geonames.org/640784/'),
        'name': 'Puijo',
        'encoded_name': 'puijo',
        'countryCode': 'FI',
        'locationMap': URIRef('http://www.geonames.org/640784/puijo.html'),
        'latitude': '62.91667',
        'longitude': '27.65'
    },
    'Värriö': {
        'smear_table': 'VAR_DMPS',
        'smear_variables': 'd316e1,d355e1,d398e1,d447e1,d501e1,d562e1,d631e1,d708e1,d794e1,'\
                           'd891e1,d100e2,d112e2,d126e2,d141e2,d158e2,d178e2,d200e2,d224e2,'\
                           'd251e2,d282e2,d316e2,d355e2,d398e2,d447e2,d501e2,d562e2,d631e2,'\
                           'd708e2,d794e2,d891e2,d100e3,d112e3,d126e3,d141e3,d158e3,d178e3,d200e3',
        'num_var_less_than_10nm': 10,
        'identifier': URIRef('http://sws.geonames.org/828747/'),
        'name': 'Värriö',
        'encoded_name': 'vaerrioe',
        'countryCode': 'FI',
        'locationMap': URIRef('http://www.geonames.org/828747/vaerrioe.html'),
        'latitude': '67.46535',
        'longitude': '27.99231'
    },
    'Class Ia': {
        'identifier': URIRef('http://avaa.tdata.fi/web/smart/smear/ClassIa'),
        'label': 'Class Ia',
        'comment': 'Very clear and strong event'
    },
    'Class Ib': {
        'identifier': URIRef('http://avaa.tdata.fi/web/smart/smear/ClassIb'),
        'label': 'Class Ib',
        'comment': 'Unclear event'
    },
    'Class II': {
        'identifier': URIRef('http://avaa.tdata.fi/web/smart/smear/ClassII'),
        'label': 'Class II',
        'comment': 'Event with little confidence level'
    }
}

place = 'Hyytiälä'
ns = 'http://avaa.tdata.fi/web/smart/smear/'
tz_helsinki = timezone('Europe/Helsinki')

LODE = dict()
DUL = dict()
GeoNames = dict()
WGS84 = dict()
SMEAR = dict()
SimpleFeatures = dict()
GeoSPARQL = dict()
Time = dict()
obo = dict()
prov = dict()
orkg = dict()

LODE['Event'] = URIRef('http://linkedevents.org/ontology/Event')
LODE['atPlace'] = URIRef('http://linkedevents.org/ontology/atPlace')
LODE['atTime'] = URIRef('http://linkedevents.org/ontology/atTime')
LODE['inSpace'] = URIRef('http://linkedevents.org/ontology/inSpace')
LODE['involved'] = URIRef('http://linkedevents.org/ontology/involved')
DUL['Place'] = URIRef('http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#Place')
GeoNames['Feature'] = URIRef('http://www.geonames.org/ontology#Feature')
GeoNames['name'] = URIRef('http://www.geonames.org/ontology#name')
GeoNames['countryCode'] = URIRef('http://www.geonames.org/ontology#countryCode')
GeoNames['locationMap'] = URIRef('http://www.geonames.org/ontology#locationMap')
WGS84['SpatialThing'] = URIRef('http://www.w3.org/2003/01/geo/wgs84_pos#SpatialThing')
WGS84['lat'] = URIRef('http://www.w3.org/2003/01/geo/wgs84_pos#lat')
WGS84['long'] = URIRef('http://www.w3.org/2003/01/geo/wgs84_pos#long')
SMEAR['Classification'] = URIRef('http://avaa.tdata.fi/web/smart/smear/Classification')
SMEAR['hasClassification'] = URIRef('http://avaa.tdata.fi/web/smart/smear/hasClassification')
SimpleFeatures['Point'] = URIRef('http://www.opengis.net/ont/sf#Point')
GeoSPARQL['asWKT'] = URIRef('http://www.opengis.net/ont/geosparql#asWKT')
GeoSPARQL['wktLiteral'] = URIRef('http://www.opengis.net/ont/geosparql#wktLiteral')
Time['Instant'] = URIRef('http://www.w3.org/2006/time#Instant')
Time['Interval'] = URIRef('http://www.w3.org/2006/time#Interval')
Time['TemporalUnit'] = URIRef('http://www.w3.org/2006/time#TemporalUnit')
Time['hasTime'] = URIRef('http://www.w3.org/2006/time#hasTime')
Time['hasBeginning'] = URIRef('http://www.w3.org/2006/time#hasBeginning')
Time['hasEnd'] = URIRef('http://www.w3.org/2006/time#hasEnd')
Time['inXSDDateTime'] = URIRef('http://www.w3.org/2006/time#inXSDDateTime')
obo['is about'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000136')
obo['scalar measurement datum'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000032')
obo['has measurement unit label'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000039')
obo['has measurement value'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000004')
obo['time unit'] = URIRef('http://purl.obolibrary.org/obo/UO_0000003')
obo['hour'] = URIRef('http://purl.obolibrary.org/obo/UO_0000032')
obo['average value'] = URIRef('http://purl.obolibrary.org/obo/OBI_0000679')
obo['is_specified_output_of'] = URIRef('http://purl.obolibrary.org/obo/OBI_0000312')
obo['has_specified_output'] = URIRef('http://purl.obolibrary.org/obo/OBI_0000299')
obo['arithmetic mean calculation'] = URIRef('http://purl.obolibrary.org/obo/OBI_0200079')
obo['has_specified_input'] = URIRef('http://purl.obolibrary.org/obo/OBI_0000293')
obo['data set'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000100')
obo['has part'] = URIRef('http://purl.obolibrary.org/obo/BFO_0000051')
obo['data item'] = URIRef('http://purl.obolibrary.org/obo/IAO_0000027')
obo['atmospheric aerosol formation event'] = URIRef('http://purl.obolibrary.org/obo/ENVO_01001359')
prov['Entity'] = URIRef('http://www.w3.org/ns/prov#Entity')
prov['Activity'] = URIRef('http://www.w3.org/ns/prov#Activity')
prov['Agent'] = URIRef('http://www.w3.org/ns/prov#Agent')
prov['wasDerivedFrom'] = URIRef('http://www.w3.org/ns/prov#wasDerivedFrom')
prov['wasGeneratedBy'] = URIRef('http://www.w3.org/ns/prov#wasGeneratedBy')
prov['used'] = URIRef('http://www.w3.org/ns/prov#used')
prov['startedAtTime'] = URIRef('http://www.w3.org/ns/prov#startedAtTime')
prov['endedAtTime'] = URIRef('http://www.w3.org/ns/prov#endedAtTime')
orkg['ResearchResult'] = URIRef('http://orkg.org/core#ResearchResult')

g = Graph()

g.bind('lode', 'http://linkedevents.org/ontology/')
g.bind('dul', 'http://www.ontologydesignpatterns.org/ont/dul/DUL.owl#')
g.bind('gn', 'http://www.geonames.org/ontology#')
g.bind('wgs84', 'http://www.w3.org/2003/01/geo/wgs84_pos#')
g.bind('smear', 'http://avaa.tdata.fi/web/smart/smear/')
g.bind('sf', 'http://www.opengis.net/ont/sf#')
g.bind('geosparql', 'http://www.opengis.net/ont/geosparql#')
g.bind('time', 'http://www.w3.org/2006/time#')
g.bind('obo', 'http://purl.obolibrary.org/obo/')
g.bind('prov', 'http://www.w3.org/ns/prov#')


def data(date):
    time_from = timezone('Europe/Helsinki').localize(datetime.strptime(date, '%Y-%m-%d'))
    time_to = time_from + timedelta(days=1)

    try:
        smear_table = configuration[place]['smear_table']
        smear_variables = configuration[place]['smear_variables']
    except LookupError:
        print('Place not found in configuration [place = {}, places = {}]'.format(place, configuration.keys()))
        return list()

    query = {'table': smear_table, 'quality': 'ANY', 'averaging': 'NONE', 'type': 'NONE',
             'from': str(time_from), 'to': str(time_to), 'variables': smear_variables}
    url = 'https://avaa.tdata.fi/smear-services/smeardata.jsp?' + urlencode(query)
    response = requests.post(url)

    return pd.read_csv(io.StringIO(response.text))


def plot(data):
    d = data.copy(deep=True)
    d = d.ix[:, 6:].values
    m = len(d)
    n = len(d[0])
    x = range(0, m)
    y = range(0, n)
    x, y = np.meshgrid(x, y)
    z = np.transpose(np.array([row[1:] for row in d]).astype(np.float))
    plt.figure(figsize=(10, 5), dpi=100)
    plt.pcolormesh(x, y, z)
    plt.plot((0, x.max()), (y.max()/2, y.max()/2), "r-")
    plt.colorbar()
    plt.xlim(right=m-1)
    x_ticks = np.arange(x.min(), x.max(), 6)
    x_labels = range(x_ticks.size)
    plt.xticks(x_ticks, x_labels)
    plt.xlabel('Hours')
    y_ticks = np.arange(y.min(), y.max(), 6)
    y_labels = ['3.16', '6.31', '12.6', '25.1', '50.1', '100']
    plt.yticks(y_ticks, y_labels)
    plt.ylabel('Diameter [nm]')
    plt.ylim(top=n-1)
    plt.show()

    
def event(day, beginning, end, classification):
    point = 'POINT ({} {})'.format(configuration[place]['longitude'], configuration[place]['latitude'])

    beginning_datetime = tz_helsinki.localize(datetime.strptime('{} {}'.format(day, beginning), '%Y-%m-%d %H:%M'))
    end_datetime = tz_helsinki.localize(datetime.strptime('{} {}'.format(day, end), '%Y-%m-%d %H:%M'))
    beginning_isoformat = beginning_datetime.isoformat()
    end_isoformat = end_datetime.isoformat()
    time_isoformat = '{}/{}'.format(beginning_isoformat, end_isoformat)
    datetime_now = datetime.utcnow().replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())

    event_uri = URIRef('{}{}'.format(ns, md5('{}{}'.format(day, place).encode()).hexdigest()))
    geometry_uri = URIRef('{}{}'.format(ns, md5(point.encode()).hexdigest()))
    time_uri = URIRef('{}{}'.format(ns, md5(time_isoformat.encode()).hexdigest()))
    beginning_uri = URIRef('{}{}'.format(ns, md5(beginning_isoformat.encode()).hexdigest()))
    end_uri = URIRef('{}{}'.format(ns, md5(end_isoformat.encode()).hexdigest()))
    place_uri = configuration[place]['identifier']
    classification_uri = configuration[classification]['identifier']

    g.add((event_uri, RDF.type, LODE['Event']))
    g.add((event_uri, LODE['atPlace'], place_uri))
    g.add((event_uri, LODE['inSpace'], geometry_uri))
    g.add((event_uri, LODE['atTime'], time_uri))
    g.add((event_uri, SMEAR['hasClassification'], classification_uri))
    g.add((place_uri, RDF.type, DUL['Place']))
    g.add((place_uri, RDF.type, GeoNames['Feature']))
    g.add((place_uri, GeoNames['name'], Literal(configuration[place]['name'], datatype=XSD.string)))
    g.add((place_uri, GeoNames['countryCode'], Literal(configuration[place]['countryCode'], datatype=XSD.string)))
    g.add((place_uri, GeoNames['locationMap'], configuration[place]['locationMap']))
    g.add((place_uri, WGS84['lat'], Literal(configuration[place]['latitude'], datatype=XSD.double)))
    g.add((place_uri, WGS84['long'], Literal(configuration[place]['longitude'], datatype=XSD.double)))
    g.add((classification_uri, RDF.type, SMEAR['Classification']))
    g.add((classification_uri, RDFS.label, Literal(configuration[classification]['label'], datatype=XSD.string)))
    g.add((classification_uri, RDFS.comment, Literal(configuration[classification]['comment'], datatype=XSD.string)))
    g.add((geometry_uri, RDF.type, SimpleFeatures['Point']))
    g.add((geometry_uri, RDF.type, WGS84['SpatialThing']))
    g.add((geometry_uri, GeoSPARQL['asWKT'], Literal(point, datatype=GeoSPARQL['wktLiteral'])))
    g.add((time_uri, RDF.type, Time['Interval']))
    g.add((time_uri, Time['hasBeginning'], beginning_uri))
    g.add((time_uri, Time['hasEnd'], end_uri))
    g.add((beginning_uri, RDF.type, Time['Instant']))
    g.add((beginning_uri, Time['inXSDDateTime'], Literal(beginning_isoformat, datatype=XSD.dateTime)))
    g.add((end_uri, RDF.type, Time['Instant']))
    g.add((end_uri, Time['inXSDDateTime'], Literal(end_isoformat, datatype=XSD.dateTime)))


def events():
    q = """
    SELECT ?place ?beginning ?end ?classification ?uri
    WHERE {
    ?uri rdf:type lode:Event .
    ?uri lode:atTime ?atTime .
    ?atTime time:hasBeginning ?hasBeginning .
    ?hasBeginning time:inXSDDateTime ?beginning .
    ?atTime time:hasEnd ?hasEnd .
    ?hasEnd time:inXSDDateTime ?end .
    ?uri lode:atPlace ?atPlace .
    ?atPlace gn:name ?place .
    ?atPlace wgs84:lat ?latitude .
    ?atPlace wgs84:long ?longitude .
    ?uri smear:hasClassification ?hasClassification .
    ?hasClassification rdfs:label ?classification .
    }
    ORDER BY ASC(?beginning)
    """
    
    df = query(q, {'classification': 'str', 'place': 'str', 'latitude': 'float', 'longitude': 'float', 'uri': 'str'})
    df.beginning = pd.to_datetime(df.beginning, utc=True).dt.tz_convert('Europe/Helsinki')
    df.end = pd.to_datetime(df.end, utc=True).dt.tz_convert('Europe/Helsinki')
    
    return df

def duration(value, df):
    event_uris = df['uri']
    unit = 'hour'
    datetime_now = datetime.utcnow().replace(tzinfo=tz.tzutc()).astimezone(tz.tzlocal())

    datum_uri = URIRef('{}{}'.format(ns, md5('{}{}'.format(datetime_now, 'datum').encode()).hexdigest()))
    arithmetic_mean_calculation_uri = URIRef('{}{}'.format(ns, md5('{}{}'.format(datetime_now, 'arithmetic_mean_calculation').encode()).hexdigest()))
    dataset_uri = URIRef('{}{}'.format(ns, md5('{}{}'.format(datetime_now, 'dataset').encode()).hexdigest()))

    g.add((obo['atmospheric aerosol formation event'], RDFS.label, Literal('atmospheric aerosol formation event')))
    g.add((obo['scalar measurement datum'], RDFS.label, Literal('scalar measurement datum')))
    g.add((obo['has measurement unit label'], RDFS.label, Literal('has measurement unit label')))
    g.add((obo['has measurement value'], RDFS.label, Literal('has measurement value')))
    g.add((obo['time unit'], RDFS.label, Literal('time unit')))
    g.add((obo['hour'], RDFS.label, Literal('hour')))
    g.add((obo['average value'], RDFS.label, Literal('average value')))
    g.add((obo['is_specified_output_of'], RDFS.label, Literal('is_specified_output_of')))
    g.add((obo['has_specified_output'], RDFS.label, Literal('has_specified_output')))
    g.add((obo['arithmetic mean calculation'], RDFS.label, Literal('arithmetic mean calculation')))
    g.add((obo['has_specified_input'], RDFS.label, Literal('has_specified_input')))
    g.add((obo['data set'], RDFS.label, Literal('data set')))
    g.add((obo['has part'], RDFS.label, Literal('has part')))
    g.add((obo['data item'], RDFS.label, Literal('data item')))

    g.add((obo['hour'], RDF.type, obo['time unit']))
    g.add((arithmetic_mean_calculation_uri, RDF.type, obo['arithmetic mean calculation']))
    g.add((dataset_uri, RDF.type, obo['data set']))
    g.add((datum_uri, RDF.type, obo['scalar measurement datum']))
    g.add((datum_uri, RDF.type, obo['average value']))
    g.add((datum_uri, RDF.type, orkg['ResearchResult']))
    g.add((datum_uri, obo['is about'], obo['atmospheric aerosol formation event']))

    g.add((datum_uri, obo['has measurement value'], Literal(value, datatype=XSD.decimal)))
    g.add((datum_uri, obo['has measurement unit label'], obo['hour']))
    g.add((datum_uri, obo['is_specified_output_of'], arithmetic_mean_calculation_uri))
    g.add((arithmetic_mean_calculation_uri, obo['has_specified_output'], datum_uri))
    g.add((arithmetic_mean_calculation_uri, obo['has_specified_input'], dataset_uri))

    for event_uri in event_uris:
        g.add((URIRef(event_uri), RDF.type, obo['data item']))
        g.add((dataset_uri, obo['has part'], URIRef(event_uri)))

    g.add((dataset_uri, RDF.type, prov['Entity']))
    g.add((datum_uri, RDF.type, prov['Entity']))
    g.add((arithmetic_mean_calculation_uri, RDF.type, prov['Activity']))
    g.add((datum_uri, prov['wasDerivedFrom'], dataset_uri))
    g.add((datum_uri, prov['wasGeneratedBy'], arithmetic_mean_calculation_uri))
    g.add((arithmetic_mean_calculation_uri, prov['used'], dataset_uri))
    g.add((arithmetic_mean_calculation_uri, prov['startedAtTime'], Literal(datetime_now.isoformat(), datatype=XSD.dateTime)))
    g.add((arithmetic_mean_calculation_uri, prov['endedAtTime'], Literal(datetime_now.isoformat(), datatype=XSD.dateTime)))
    
    return datum_uri.toPython()

def parse(url):
    g.parse(url, format='rdfa')
    
def query(q, dtype={}):
    serializer = CSVResultSerializer(g.query(q))
    output = io.BytesIO()
    serializer.serialize(output)
    
    return pd.read_csv(StringIO(output.getvalue().decode()), dtype=dtype)