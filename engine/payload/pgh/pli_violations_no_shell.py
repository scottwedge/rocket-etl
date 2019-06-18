import csv, json, requests, sys, traceback
from datetime import datetime

from marshmallow import fields, pre_load, post_load
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl

from engine.credentials import API_key
from engine.parameters.local_parameters import SETTINGS_FILE, PRODUCTION
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import find_resource_id, post_process, local_file_and_dir, fetch_city_file
from engine.notify import send_to_slack

CLEAR_FIRST = False

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
                        data[k] = datetime.datetime.strptime(v, "%m/%d/%Y").date().isoformat()
                    except:
                        data[k] = None

    @post_load
    def geocode(self, data):
        if data['parcel'] in coords:
            data['x'] = coords[data['parcel']]['x']
            data['y'] = coords[data['parcel']]['y']
            for area in ['NEIGHBORHOOD', 'TRACT', 'COUNCIL_DISTRICT', 'PLI_DIVISION', 'POLICE_ZONE', 'FIRE_ZONE',
                         'PUBLIC_WORKS_DIVISION', 'WARD']:
                data[area.lower()] = coords[data['parcel']][area]
        else:
            data['x'], data['y'] = None, None


pli_violations_package_id = "d660edf8-9157-45ad-a282-50822badfaae" # Production version of PLI Violations package
pli_violations_package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package

jobs = [
    {
        'package': pli_violations_package_id,
        'source_dir': '',
        'source_file': 'pliExportParcel',
        'resource_name': 'Pittsburgh PLI Violations Report',
        'schema': pliViolationsSchema
    },
]

def process_job(job,test_mode):
    print("==============\n" + job['resource_name'])
    if not use_local_files:
        fetch_city_file(job)
    target, _ = local_file_and_dir(job)
    package_id = job['package'] if not test_mode else TEST_PACKAGE_ID
    resource_name = job['resource_name']
    schema = job['schema']

    # [ ] Should a command-line 'production' or 'test' parameter be required?

    # Resource Metadata
    #package_id = 'd660edf8-9157-45ad-a282-50822badfaae'
    #resource_name = 'Pittsburgh PLI Violations Report'

    # Geocoding Stuff
    areas = ['NEIGHBORHOOD', 'TRACT', 'COUNCIL_DISTRICT', 'PLI_DIVISION', 'POLICE_ZONE', 'FIRE_ZONE',
             'PUBLIC_WORKS_DIVISION', 'WARD']

    parcel_file = SOURCE_DIR + "parcel_areas.csv"
    #target = SOURCE_DIR + "violations.csv"

    with open(parcel_file) as f:
        dr = csv.DictReader(f)
        for row in dr:
            coords[row['PIN']] = {'x': row['x'],
                                  'y': row['y']}
            for area in areas:
                coords[row['PIN']][area] = row[area]

    # Upload data to datastore
    print('Uploading tabular data...')
    curr_pipeline = pl.Pipeline(job['resource_name'] + ' pipeline', job['resource_name'] + ' Pipeline', log_status=False, chunk_size=200, settings_file=SETTINGS_FILE) \
        .connect(pl.FileConnector, target, config_string='ftp.city_ftp', encoding='utf-8-sig') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, 'production',
              fields=schema().serialize_to_ckan_fields(),
              key_fields=['CASE_NUMBER'],
              package_id=package_id,
              resource_name=resource_name,
              clear_first=CLEAR_FIRST,
              method='upsert').run()

    resource_id = find_resource_id(package_id, resource_name)
    post_process(resource_id)

def main(selected_job_codes,use_local_files=False,clear_first=False,test_mode=False):
    if selected_job_codes == []:
        selected_jobs = list(jobs)
    else:
        selected_jobs = [j for j in jobs if (j['source_file'] in selected_job_codes)]
    for job in selected_jobs:
        process_job(job,test_mode)

if __name__ == '__main__':
    args = sys.argv[1:]
    copy_of_args = list(args)
    mute_alerts = False
    use_local_files = False
    clear_first = False
    test_mode = not PRODUCTION # Use PRODUCTION boolean from parameters/local_parameters.py to set whether test_mode defaults to True or False
    job_codes = [j['source_file'] for j in jobs]
    selected_job_codes = []
    try:
        for k,arg in enumerate(copy_of_args):
            if arg in ['mute']:
                mute_alerts = True
                args.remove(arg)
            elif arg in ['local']:
                use_local_files = True
                args.remove(arg)
            elif arg in ['clear_first']:
                clear_first = True
                args.remove(arg)
            elif arg in ['test']:
                test_mode = True
                args.remove(arg)
            elif arg in job_codes:
                selected_job_codes.append(arg)
                args.remove(arg)
        if len(args) > 0:
            print("Unused command-line arguments: {}".format(args))

        main(selected_job_codes,use_local_files,clear_first,test_mode)
    except:
        e = sys.exc_info()[0]
        msg = "Error: {} : \n".format(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        msg = ''.join('!! ' + line for line in lines)
        print(msg) # Log it or whatever here
        if not mute_alerts:
            channel = "@david" if test_mode else "#etl-hell"
            send_to_slack(msg,username='PLI Violations ETL assistant',channel=channel,icon=':illuminati:')
