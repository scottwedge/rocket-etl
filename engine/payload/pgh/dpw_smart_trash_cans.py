import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import post_process, default_job_setup, push_to_datastore, fetch_city_file
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class smartTrashCansSchema(pl.BaseSchema):
    container_id = fields.String(allow_none=False)
    receptacle_model_id = fields.String(allow_none=False)
    assignment_date = fields.DateTime(allow_none=False)
    last_updated_date = fields.DateTime(allow_none=False)
    group_name = fields.String(allow_none=False)
    address = fields.String(allow_none=False)
    city = fields.String(allow_none=False)
    state = fields.String(allow_none=False)
    zip = fields.String(allow_none=False)
    neighborhood = fields.String(allow_none=False)
    dpw_division = fields.String(allow_none=False)
    council_district = fields.String(allow_none=False)
    ward = fields.String(allow_none=False)
    fire_zone = fields.String(allow_none=False)
    x = fields.Float(allow_none=False)
    y = fields.Float(allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def fix_datetimes(self, data):
        for k, v in data.items():
            if 'date' in k:
                if v:
                    try:
                        data[k] = parser.parse(v).isoformat()
                    except:
                        data[k] = None

smart_trash_cans_package_id = "b1282e47-6a70-4f18-98df-f081e7406e34" # Production version of Smart Trash Cans package
#pli_violations_package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package

jobs = [
    {
        'source_dir': '',
        'source_file': 'smart_trash_containers.csv',
        'schema': smartTrashCansSchema,
        'package': smart_trash_cans_package_id,
        'resource_name': 'Smart Trash Containers',
    },
#    {
#        'source_dir': '',
#        'source_file': 'smart_trash_containers.geojson',
#        'schema': None,
#        'destinations': ['ckan_filestore'],
#        'package': smart_trash_cans_package_id,
#        'resource_name': 'Smart Trash Containers (GeoJSON)'
#    },
]

def process_job(**kwparameters):
    job = kwparameters['job']
    use_local_files = kwparameters['use_local_files']
    clear_first = kwparameters['clear_first']
    test_mode = kwparameters['test_mode']
    target, local_directory, loader_config_string, destinations, destination_file_path, destination_directory = default_job_setup(job)
    ## BEGIN CUSTOMIZABLE SECTION ##
    file_connector = pl.FileConnector
    config_string = ''
    encoding = 'utf-8-sig'
    if not use_local_files:
        fetch_city_file(job)
    primary_key_fields=['container_id']
    upload_method = 'upsert'
    ## END CUSTOMIZABLE SECTION ##

    resource_id = push_to_datastore(job, file_connector, target, config_string, encoding, loader_config_string, primary_key_fields, test_mode, clear_first, upload_method)
    return [resource_id] # Return a complete list of resource IDs affected by this call to process_job.
