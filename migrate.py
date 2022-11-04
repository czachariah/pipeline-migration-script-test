from etleap.api import EtleapApi, EtleapApiException

# US API credentials
us_etleap_access_key = '...'
us_etleap_secret_key = '...'

# EU API credentials
eu_etleap_access_key = '...'
eu_etleap_secret_key = '...'

# Maps a connection from the US to the EU environment
connection_map = {
    'c0Bz8SOK': 'BTIfFpQw',
    'JSS8ABBJ': 'rOpIIrZl'
}

# Only migrated pipelines for sources in this list
sources_to_migrate = ['c0Bz8SOK']

us_client = EtleapApi(us_etleap_access_key, us_etleap_secret_key)
pipelines = us_client.get_pipelines()

from_source = [ p for p in pipelines if p.source['connectionId'] in sources_to_migrate ]

for p in from_source:
    if (p.destination['connectionId'] in connection_map.keys()):
        p.destination['connectionId'] = connection_map[p.destination['connectionId']]
    if (p.source['connectionId'] in connection_map.keys()):
         p.source['connectionId'] = connection_map[p.source['connectionId']]


eu_client = EtleapApi(prod_etleap_access_key, prod_etleap_secret_key, "https://app.eu.etleap.com/api/v2/")

try: 
    for p in from_source:
        eu_client.create_pipeline(p)
except EtleapApiException as e:
    print(e.error_text)
