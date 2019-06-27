import csv, json, requests, sys, traceback
from datetime import datetime
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import post_process, default_job_setup, push_to_datastore, fetch_city_file
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

coords = {}

class pliViolationsSchema(pl.BaseSchema):
    street_num = fields.String(dump_to="STREET_NUM", allow_none=True)
    street_name = fields.String(dump_to="STREET_NAME", allow_none=True)
    inspection_date = fields.Date(dump_to="INSPECTION_DATE", allow_none=True)
    case_number = fields.String(dump_to="CASE_NUMBER", allow_none=True)
    inspection_result = fields.String(dump_to="INSPECTION_RESULT", allow_none=True)
    dpw_requests = fields.String(dump_to="DPW_REQUEST", allow_none=True)
    violation = fields.String(dump_to="VIOLATION", allow_none=True)
    next_action = fields.String(load_only=True, allow_none=True)
    location = fields.String(dump_to="LOCATION", allow_none=True)
    corrective_action = fields.String(dump_to="CORRECTIVE_ACTION", allow_none=True)
    next_action_date = fields.Date(load_only=True, allow_none=True)
    parcel = fields.String(dump_to="PARCEL", allow_none=True)
    docket_number = fields.String(load_only=True, allow_none=True)
    court_dates = fields.String(load_only=True, allow_none=True)
    neighborhood = fields.String(dump_to="NEIGHBORHOOD", allow_none=True)
    council_district = fields.String(dump_to="COUNCIL_DISTRICT", allow_none=True)
    ward = fields.String(dump_to="WARD", allow_none=True)
    tract = fields.String(dump_to="TRACT", allow_none=True)
    public_works_division = fields.String(dump_to="PUBLIC_WORKS_DIVISION", allow_none=True)
    pli_division = fields.String(dump_to="PLI_DIVISION", allow_none=True)
    police_zone = fields.String(dump_to="POLICE_ZONE", allow_none=True)
    fire_zone = fields.String(dump_to="FIRE_ZONE", allow_none=True)
    x = fields.Float(dump_to="X", dump_only=True, allow_none=True)
    y = fields.Float(dump_to="Y", dump_only=True, allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_date(self, data):
        for k, v in data.items():
            if 'date' in k and 'court' not in k:
                if v:
                    try:
                        data[k] = datetime.strptime(v, "%m/%d/%Y").date().isoformat()
                    except:
                        data[k] = None

    @post_load
    def geocode(self, data):
        if 'parcel' in data and data['parcel'] in coords:
            data['x'] = coords[data['parcel']]['x']
            data['y'] = coords[data['parcel']]['y']
            for area in ['NEIGHBORHOOD', 'TRACT', 'COUNCIL_DISTRICT', 'PLI_DIVISION', 'POLICE_ZONE', 'FIRE_ZONE',
                         'PUBLIC_WORKS_DIVISION', 'WARD']:
                data[area.lower()] = coords[data['parcel']][area]
        else:
            data['x'], data['y'] = None, None


pli_violations_package_id = "d660edf8-9157-45ad-a282-50822badfaae" # Production version of PLI Violations package
#pli_violations_package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package

jobs = [
    {
        'package': pli_violations_package_id,
        'source_dir': '',
        'source_file': 'pliExportParcel.csv',
        'resource_name': 'Pittsburgh PLI Violations Report',
        'schema': pliViolationsSchema
    },
]

def process_job(job,use_local_files,clear_first,test_mode):
    target, local_directory, destination = default_job_setup(job)
    ## BEGIN CUSTOMIZABLE SECTION ##
    file_connector = pl.FileConnector
    config_string = ''
    encoding = 'utf-8-sig'
    if not use_local_files:
        fetch_city_file(job)
    primary_key_fields=['CASE_NUMBER'] # This is from pli_violations_no_shell.py
    #primary_key_fields=['CASE_NUMBER', 'VIOLATION', 'LOCATION', 'CORRECTIVE_ACTION'] # This is from an old job: tools:jobs/pli/pli_violations.py
    upload_method = 'upsert'

    # Geocoding Stuff
    areas = ['NEIGHBORHOOD', 'TRACT', 'COUNCIL_DISTRICT', 'PLI_DIVISION', 
            'POLICE_ZONE', 'FIRE_ZONE',
            'PUBLIC_WORKS_DIVISION', 'WARD']

    parcel_file = local_directory + "parcel_areas.csv"

    with open(parcel_file) as f:
        dr = csv.DictReader(f)
        for row in dr:
            coords[row['PIN']] = {'x': row['x'],
                                  'y': row['y']}
            for area in areas:
                coords[row['PIN']][area] = row[area]
    ## END CUSTOMIZABLE SECTION ##

    resource_id = push_to_datastore(job, file_connector, target, config_string, encoding, destination, primary_key_fields, test_mode, clear_first, upload_method)
    return [resource_id] # Return a complete list of resource IDs affected by this call to process_job.
