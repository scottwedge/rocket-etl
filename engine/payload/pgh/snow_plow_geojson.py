import csv, json, requests, os, sys, traceback
from datetime import datetime
from dateutil import parser
from pprint import pprint

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.etl_util import local_file_and_dir, download_city_directory, resource_exists
from engine.notify import send_to_slack
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.parameters.local_parameters import SOURCE_DIR

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa


def ftp_and_upload_maybe(job, **kwparameters):
    """This script optionally obtains all files from an FTP directory,
    scans for files that have not been uploaded to CKAN, and takes the
    first such file and recodes the job to upload just it before 
    allowing the job to run.
    
    A better ETL framework would make this script smaller or non-existent."""
    use_local_files = kwparameters['use_local_files'] # Deserializing the command-line parameters
    # feels kludgy, but it also doesn't seem worth folding them into the Job object just for
    # the custom processing function and the run_pipeline function.
    _, local_target_directory = local_file_and_dir(job, SOURCE_DIR)
    local_target_directory += '/snow_plow_data'
    if not os.path.isdir(local_target_directory):
        os.makedirs(local_target_directory)
    if not use_local_files:
        download_city_directory(job, local_target_directory)
        # Downloading all of these files just to get a list of the 
        # FTP directory is not great, particularly given that these files
        # can each be hundreds of megabytes in size.
        # Better solutions would be to intercept the list of names
        # (possibly using lftp or some other program) and then 
        # using that to decide which file to obtain from the FTP server.

    # Get list of files in local_target_directory.
    datafiles = sorted(os.listdir(local_target_directory))
    # Compare list of obtained files to list of resources for the package
    effective_package_id = TEST_PACKAGE_ID if kwparameters['test_mode'] else job.package # It's 
    # necessary to work out the effective package ID here since this 
    # preprocessing is done before the package ID is switched to the 
    # package ID when in test mode.
    ic(effective_package_id)
    for datafile in datafiles:
        resource_name, _ = datafile.split('.')
        if not resource_exists(effective_package_id, resource_name):
            # Modify the job to upload just this file.
            # This is kind of a kludge until the ETL framework becomes
            # generalized to support multiple simultaneous tables 
            # (though I suppose one could also add to the jobs list,
            # assuming it can be extended), but then so is this 
            # particular ETL job.
            job.source_file = datafile
            job.resource_name = resource_name
            job.local_directory = local_target_directory
            job.target = f"{local_target_directory}/{datafile}"
            print(f"OK, let's upload {datafile}.")
            return
        else:
            print(f"Found resource with name {resource_name} in package {job.package}.")

    print("No new files to upload.")

snow_plow_geojson_package_id = "d0b56030-3391-49db-87bd-4f1c16490fbc" # Production version of Snow Plow Activity (2018-2020)

job_dicts = [
        {
        'source_type': 'local',
        'source_dir': 'SnowPlows',
        'source_file': '',
        'encoding': 'utf-8-sig',
        'schema': None,
        'custom_processing': ftp_and_upload_maybe,
        'destinations': ['ckan_filestore'],
        'package': snow_plow_geojson_package_id,
        'resource_name': 'test',
    },
]

# This script claimed to upload 2019-02-19_1000-2019-02-20_1000.geojson to package d0b56030-3391-49db-87bd-4f1c16490fbc (Snow Plow Activity) and
# 2018-11-29_1149-2018-11-30_1149.geojson to package b8b5fee7-2281-4426-a68e-2e05c6dec365 (Port Authority package), but neither resource showed
# up. Why????????
