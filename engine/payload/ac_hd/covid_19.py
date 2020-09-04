import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from unidecode import unidecode

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

class DeathDemographicsSchema(pl.BaseSchema):
    category = fields.String()
    demographic = fields.String()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class DeathsByDateSchema(pl.BaseSchema):
    date = fields.Date()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class CasesByPlaceSchema(pl.BaseSchema):
    neighborhood_municipality = fields.String()
    indv_tested = fields.Integer()
    cases = fields.Integer()
    deaths = fields.Integer()
    update_date = fields.Date()

    class Meta:
        ordered = True

class TestsSchema(pl.BaseSchema):
    indv_id = fields.String()
    collection_date = fields.Date()
    test_result = fields.String(allow_none=True)
    case_status = fields.String()
    hospital_flag = fields.String(allow_none=True)
    icu_flag = fields.String(allow_none=True)
    vent_flag = fields.String(allow_none=True)
    age_bucket = fields.String()
    sex = fields.String()
    race = fields.String()
    ethnicity = fields.String()
    update_date = fields.Date()

    class Meta:
        ordered = True

    @pre_load
    def fix_nas(self, data):
        for k, v in data.items():
            if k in ['test_result', 'hospital_flag', 'icu_flag', 'vent_flag']:
                if v in ['NA']:
                    data[k] = None

covid_19_package_id = '80e0ca5d-c88a-4b1a-bf5d-51d0aaeeac86' # Production version of COVID-19 testing data package

job_dicts = [
    {
        'job_code': 'death_demographics',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidDeathDemographics.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DeathDemographicsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'covid_19_death_demographics.csv', # purposes.
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Deaths by Demographic Groups'
    },
    {
        'job_code': 'deaths_by_date',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidDeathsTimeSeries.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': DeathsByDateSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'covid_19_deaths_by_date.csv', # purposes.
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Deaths by Date'
    },
    {
        'job_code': 'cases_by_place',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidMuniHoodCounts.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': CasesByPlaceSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'covid_19_cases_by_place.csv', # purposes.
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Counts by Municipality and Pittsburgh Neighborhood'
    },
    {
        'job_code': 'tests',
        'source_type': 'sftp',
        'source_dir': 'Health Department',
        'source_file': f'CovidTestingCases.csv',
        'connector_config_string': 'sftp.county_sftp',
        'encoding': 'utf-8-sig',
        'schema': TestsSchema,
        #'primary_key_fields': [],
        'always_wipe_data': True,
        'upload_method': 'insert',
        #'destinations': ['file'], # These lines are just for testing
        #'destination_file': f'covid_19_tests.csv', # purposes.
        'package': covid_19_package_id,
        'resource_name': f'Allegheny County COVID-19 Tests and Cases'
    },
]
