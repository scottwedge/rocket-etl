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
    zip = fields.String(allow_none=True) # This was (for no obvious
    # reason) a Float in the predecessor of this ETL job.

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
        'encoding': None,
        'connector_config_string': 'sftp.county_sftp',
        'schema': ViolationsSchema,
        'upload_method': 'insert',
        'always_clear_first': True,
        'package': package_id,
        'resource_name': "Food Facility/Restaurant Inspection Violations",
    },
]
