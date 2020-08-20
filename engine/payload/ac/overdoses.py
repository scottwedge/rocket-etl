import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


class FatalODSchema(pl.BaseSchema):
    #death_date = fields.Date(format='%m/%d/%Y', load_only=True, allow_none=True)
    #death_time = fields.Time(format='%I:%M %p', load_only=True, allow_none=True)
    #death_date_and_time = fields.DateTime(dump_only=True)
    death_date_and_time = fields.DateTime(load_from="death.date.time", allow_none=True)
    manner_of_death = fields.String(load_from='manner.of.death')
    age = fields.Integer(allow_none=True)
    sex = fields.String(allow_none=True)
    race = fields.String(allow_none=True)
    case_dispo = fields.String(load_from='case.dispo', allow_none=True) #
    combined_od1 = fields.String(load_from='combined.od1', allow_none=True)
    combined_od2 = fields.String(load_from='combined.od2', allow_none=True)
    combined_od3 = fields.String(load_from='combined.od3', allow_none=True)
    combined_od4 = fields.String(load_from='combined.od4', allow_none=True)
    combined_od5 = fields.String(load_from='combined.od5', allow_none=True)
    combined_od6 = fields.String(load_from='combined.od6', allow_none=True)
    combined_od7 = fields.String(load_from='combined.od7', allow_none=True)
    combined_od8 = fields.String(load_from='combined.od8', allow_none=True)
    combined_od9 = fields.String(load_from='combined.od9', allow_none=True)
    combined_od10 = fields.String(load_from='combined.od10', allow_none=True)
    incident_zip = fields.String(load_from="incident.zip", allow_none=True)
    decedent_zip = fields.String(load_from="decedent.zip", allow_none=True) #
    case_year = fields.Integer(load_from='case.year', allow_none=True)

    class Meta:
        ordered = True

    #@post_load
    #def combine_date_and_time(self, in_data):
    #    death_date, death_time = in_data['death_date'], in_data['death_time']
    #    today = datetime.datetime.today()
    #    if not death_time:
    #        death_time = datetime.time(0, 0, 0)
    #    try:
    #        in_data['death_date_and_time'] = datetime.datetime(
    #            death_date.year, death_date.month, death_date.day,
    #            death_time.hour, death_time.minute, death_time.second
    #        )
    #    except:
    #        in_data['death_date_and_time'] = None

    #    return

    @pre_load
    def fix_zip_codes(self, data):
        # The source file has some weird ZIP codes like "15025-CLAR"
        # which appears to be a convention of appending an abbrevation
        # for the township to distinguish between multiple neighborhoods
        # and townships in the same ZIP code.

        # Other oddballs: "15-71"
        fields = ['incident.zip']
        if 'decedent.zip' in data:
            fields.append(['decedent.zip'])
        for field in fields:
            if data[field] in ['NA']:
                data[field] = None
            elif len(data[field]) > 5:
                data[field] = data[field][:5] # This is a simple truncation
                # which is now already happening at the query level when
                # the County generates the source file, so really this 
                # step should not be necessary.
    @pre_load
    def check_manner(self, data):
        field = 'manner.of.death'
        if data[field] not in ['Accident', 'Accidents']:
            raise ValueError(f"A record with a non-allowed {field} value was detected ({data[field]}).")

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['death.date.time', 'age', 'case.year', 'sex', 'race', 'manner.of.death'] or k[:11] == 'combined.od':
                if v in ['NA']:
                    data[k] = None

accidental_overdoses_package_id = '7fb0505e-8e2c-4825-b22c-4fbee8fc8010' # Production version of Accidental Overdoses package

job_dicts = [
        {
        'source_type': 'sftp',
        'source_dir': 'accidental_fatal_overdoses',
        'source_file': 'acome_od_all.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': FatalODSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['ckan_filestore'],
        'package': accidental_overdoses_package_id,
        'resource_name': 'Fatal Accidental Overdoses'
    },
]
