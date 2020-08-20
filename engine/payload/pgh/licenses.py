import csv, json, requests, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.etl_util import fetch_city_file

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def strip_time_zone_from_datetime(s):
    date_str, time_str = s.split('T')
    time_str_elements = time_str.split('-')
    if len(time_str_elements) > 1: # The second should be the time zone.
        zone = time_str_elements[1]
        return s[:-len(zone)-1]
    return s

    # If the time zone offset were in standard format, this would work
    # datetime.strptime('2019-01-04T16:41:24+0200', "%Y-%m-%dT%H:%M:%S%z")
    # but instead the time-zone offset has a colon between hours and minutes.

    # The two lines below don't work because dateutil can't handle this format.
    #dt_with_tz = parser.parse(s)
    #return dt_with_tz.strftime("%Y-%m-%dT%H:%M:%S") # This converts the datetime to one without an explicit time zone (now implicitly in local time)

class BaseLicensesSchema(pl.BaseSchema):
    license_number = fields.String(load_from='licensenumber') # This should work, even though the field names in the source file are quoted.
    license_type_name = fields.String(load_from='licensetypename')
    naics_code = fields.String(load_from='naicscode')
    business_name = fields.String(load_from='businessname')
    license_state = fields.String(load_from='licensestate')
    initial_issue_date = fields.DateTime(load_from='initialissuedate',allow_none=True)
    most_recent_issue_date = fields.DateTime(load_from='mostrecentissuedate',allow_none=True)
    effective_date = fields.DateTime(load_from='effectivedate',allow_none=True)
    expiration_date = fields.DateTime(load_from='expirationdate',allow_none=True)

    class Meta:
        ordered = True

    @pre_load
    def fix_dates(self, data):
        dates_to_fix = ['initialissuedate', 'mostrecentissuedate', 'effectivedate', 'expirationdate']
        for field in dates_to_fix:
            if data[field] == 'NA':
                data[field] = None
            else:
                data[field] = strip_time_zone_from_datetime(data[field])

class BusinessLicensesSchema(BaseLicensesSchema):
    parcel_number = fields.String(load_from="parcelnumber",allow_none=True)
    address = fields.String(load_from="addressformattedaddress",allow_none=True)
    email_address = fields.String(load_from="businesscontactemail",allow_none=True) # [ ]  Note in data dictionary that this is the business contact e-mail address.
    primary_phone_number = fields.String(load_from="businessprimaryformattednumber",allow_none=True)
    insurance_expiration_date = fields.Date(load_from="insuranceexpirationdate",allow_none=True)
    number_of_buildings = fields.Integer(load_from="numberofbuildings", allow_none=True)
    number_of_employees = fields.Integer(load_from="numberofemployees", allow_none=True)
    number_of_large_signs = fields.Integer(load_from="numberoflargesigns", allow_none=True)
    number_of_small_signs = fields.Integer(load_from="numberofsmallsigns", allow_none=True)
    number_of_signs_total = fields.Integer(load_from="numberofsignstotal", allow_none=True)
    number_of_handicap_spaces = fields.Integer(load_from="numberofhandicapspaces", allow_none=True)
    number_of_nonleased_pub_spaces = fields.Integer(load_from="numberofnonleasedpubspaces", allow_none=True)
    number_of_revgen_spaces = fields.Integer(load_from="numberofrevgenspaces", allow_none=True)
    total_number_of_spaces = fields.Integer(load_from="totalnumberofspaces", allow_none=True)
    number_of_jukeboxes = fields.Integer(load_from="numberofjukeboxes", allow_none=True)
    number_of_nongambling_machines = fields.Integer(load_from="numberofnongamblingmachines", allow_none=True)
    number_of_rooms = fields.Integer(load_from="numberofrooms", allow_none=True)
    number_of_seats = fields.Integer(load_from="numberofseats", allow_none=True)
    number_of_pool_tables = fields.Integer(load_from="numberofpooltables", allow_none=True)
    number_of_units = fields.Integer(load_from="numberofunits", allow_none=True)

    @pre_load
    def fix_nas_and_dates(self, data):
        fields_to_fix = ['parcelnumber', 'addressformattedaddress',
                'insuranceexpirationdate', 'businesscontactemail',
                'numberofbuildings', 'numberofemployees', 'numberoflargesigns', 'numberofsmallsigns',
                'numberofsignstotal', 'numberofhandicapspaces', 'numberofnonleasedpubspaces',
                'numberofrevgenspaces', 'totalnumberofspaces', 'numberofjukeboxes',
                'numberofnongamblingmachines', 'numberofrooms', 'numberofseats', 'numberofpooltables',
                'numberofunits']
        for field in fields_to_fix:
            if data[field] == 'NA':
                data[field] = None

        if data['businessprimaryformattednumber'] == "(0) -":
            data['businessprimaryformattednumber'] = None

        dates_to_fix = ['insuranceexpirationdate']
        for field in dates_to_fix:
            if data[field] is not None:
                data[field] = strip_time_zone_from_datetime(data[field])

class LicensesSchemaTemplate(BaseLicensesSchema):
    email_address = fields.String(load_from='emailaddress',allow_none=True)

class TradeLicensesSchema(LicensesSchemaTemplate):
    primary_phone_number = fields.String(load_from='primaryphonenumber',allow_none=True) # In tradeLicenses.csv, this field is a formatted phone number [(555) 555-5555],
    # whereas in the contractor licenses file, the phone number is unformatted.

    @pre_load
    def fix_nas(self, data):
        fields_to_fix = ['emailaddress', 'primaryphonenumber']
        for field in fields_to_fix:
            if data[field] == 'NA':
                data[field] = None
        if data['primaryphonenumber'] in ["0000000000", "(0) -"]:
            data['primaryphonenumber'] = None

class ContractorLicensesSchema(LicensesSchemaTemplate):
    primary_phone_number_unformatted = fields.String(load_from='primaryphonenumber',allow_none=True) # In tradeLicenses.csv, this field is a formatted phone number [(555) 555-5555],
    # whereas in the contractor licenses file, the phone number is unformatted.

    @pre_load
    def fix_nas(self, data):
        fields_to_fix = ['emailaddress', 'primaryphonenumber']
        for field in fields_to_fix:
            if data[field] == 'NA':
                data[field] = None
        if data['primaryphonenumber'] in ["0000000000", "(0) -"]:
            data['primaryphonenumber'] = None

def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_files']:
        fetch_city_file(job)

city_business_licenses_package_id = "2b5324d4-c57f-42f7-bac8-2aec26e199cf"

job_dicts = [
    {
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'businessLicenses.csv',
        'encoding': 'latin-1',
        'custom_processing': conditionally_get_city_files,
        'schema': BusinessLicensesSchema,
        'always_wipe_data': False,
        'primary_key_fields': ['license_number'],
        'upload_method': 'upsert',
        'package': city_business_licenses_package_id,
        'resource_name': 'Business Licenses',
    },
    {
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'contractorLicenses.csv',
        'encoding': 'latin-1',
        'custom_processing': conditionally_get_city_files,
        'schema': ContractorLicensesSchema,
        'always_wipe_data': False,
        'primary_key_fields': ['license_number'],
        'upload_method': 'upsert',
        'package': city_business_licenses_package_id,
        'resource_name': 'Licensed Contractors',
    },
    {
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'tradeLicenses.csv',
        'encoding': 'latin-1',
        'custom_processing': conditionally_get_city_files,
        'schema': TradeLicensesSchema,
        'always_wipe_data': False,
        'primary_key_fields': ['license_number'],
        'upload_method': 'upsert',
        'package': city_business_licenses_package_id,
        'resource_name': 'Trade Licenses',
    },
]
