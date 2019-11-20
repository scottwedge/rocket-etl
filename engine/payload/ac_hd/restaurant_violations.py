import csv, json, requests, sys, traceback
import time
import re
from datetime import datetime
from dateutil import parser

from marshmallow import fields, pre_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class ViolationsSchema(pl.BaseSchema):
    encounter = fields.String(allow_none=True)
    id = fields.String(allow_none=True)
    placard_st = fields.String(allow_none=True)
    # placard_desc = fields.String(allow_none=True)
    facility_name = fields.String(allow_none=True)
    bus_st_date = fields.Date(allow_none=True)
    # category_cd = fields.String(allow_none=True)
    description = fields.String(allow_none=True)
    description_new = fields.String(allow_none=True)

    num = fields.String(allow_none=True)
    street = fields.String(allow_none=True)
    city = fields.String(allow_none=True)
    state = fields.String(allow_none=True)
    zip = fields.Float(allow_none=True)

    inspect_dt = fields.Date(allow_none=True)
    start_time = fields.Time(allow_none=True)
    end_time = fields.Time(allow_none=True)

    municipal = fields.String(allow_none=True)

    rating = fields.String(allow_none=True)
    low = fields.String(allow_none=True)
    medium = fields.String(allow_none=True)
    high = fields.String(allow_none=True)
    url = fields.String(allow_none=True)

    class Meta():
        ordered = True

    @pre_load
    def fix_dates_times(self, data):
        if data['bus_st_date']:
            #data['bus_st_date'] = datetime.strptime(data['bus_st_date'], "%m/%d/%Y %H:%M").date().isoformat()
            data['bus_st_date'] = parser.parse(data['bus_st_date']).date().isoformat()

        if data['inspect_dt']:
            #data['inspect_dt'] = datetime.strptime(data['inspect_dt'], "%m/%d/%Y %H:%M").date().isoformat()
            data['inspect_dt'] = parser.parse(data['inspect_dt']).date().isoformat()

        if data['start_time']:
            data['start_time'] = datetime.strptime(data['start_time'], "%I:%M %p").time().isoformat()

        if data['end_time']:
            data['end_time'] = datetime.strptime(data['end_time'], "%I:%M %p").time().isoformat()

        to_string = ['encounter', 'id']
        for field in to_string:
            if type(data[field]) != str:
                data[field] = str(int(data[field]))

        # Because of some issues with Excel files, two string values in the first row are getting parsed as floats instead of strings: ('encounter', 201401020037.0), ('id', 201202030011.0)

package_id = "8744b4f6-5525-49be-9054-401a2c4c2fac" # Production package for Allegheny County Restaurant/Food Facility Inspections
package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package

job_dicts = [
    {
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': 'restaurantinspectionviolations_ALT.xlsx',
        'schema': ViolationsSchema,
        'package': package_id,
        'resource_name': "Food Facility/Restaurant Inspection Violations",
    },
]

def process_job(**kwparameters):
    job = kwparameters['job']
    use_local_files = kwparameters['use_local_files']
    clear_first = kwparameters['clear_first']
    test_mode = kwparameters['test_mode']
    job.default_setup(use_local_files)
    ## BEGIN CUSTOMIZABLE SECTION ##
    ### BEGIN OVERRIDES ###
    clear_first = True
    ### END OVERRIDES ###
    config_string = ''
    encoding = None
    if not use_local_files:
        #file_connector = pl.SFTPConnector#
        config_string = 'sftp.county_sftp' # This is just used to look up parameters in the settings.json file.
    upload_method = 'insert'
    ## END CUSTOMIZABLE SECTION ##
    locators_by_destination = job.run_pipeline(config_string, encoding, test_mode, clear_first, upload_method, file_format='csv')
    return locators_by_destination # Return a dict allowing look up of final destinations of data (filepaths for local files and resource IDs for data sent to a CKAN instance).

#restaurant_violations_pipeline = pl.Pipeline('restaurant_violations_pipeline', 'Restaurant Violations',
#                                             log_status=False, chunk_size=1000) \
#    .connect(pl.SFTPConnector, target, config_string='sftp.county_sftp', encoding=None) \
#    .extract(pl.ExcelExtractor, firstline_headers=True) \
#    .schema(ViolationsSchema) \
#    .load(pl.CKANDatastoreLoader, 'production',
#          fields=ViolationsSchema().serialize_to_ckan_fields(),
#          package_id=package_id,
#          resource_name=resource_name,
#          clear_first=True,
#          method='insert').run()
