import os, sys, requests, csv, json, traceback, re
from marshmallow import fields, pre_load, post_load

from engine.wprdc_etl import pipeline as pl

from engine.credentials import API_key
from engine.parameters.local_parameters import BASE_DIR, LOG_DIR, PRODUCTION
from engine.etl_util import fetch_city_file, find_resource_id, post_process
from engine.notify import send_to_slack

CLEAR_FIRST = False

try:
    from icecream import ic
except ImportError:  # Graceful fallback if IceCream isn't installed.
    ic = lambda *a: None if not a else (a[0] if len(a) == 1 else a)  # noqa

def import_module(path,name):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# job description specification:
# jobs is a list of jobs imported from the ETL script in engine/payload/<whatever>/<actual_script>.py
# Each job is a dict which may contain the following fields:
# Source fields: source_dir (a path, such as on a remote FTP site)
#                source_file (a file name)
#                source [To Be Added to distinguish County FTP site, from City FTP site from
#                other sites/APIs, but where should the lookup table for all this be stored?
#                Another Python file?]
# Transformation schema (where the value is the Marshmallow schema to be used for the transformation)
# fields:
# Destination    destinations (a list, like ['file','ckan']... the default value is ['ckan']; this could also
# fields:        be modified to change 'file' to particular file types; another option would be to link
#                jobs (for multi-step transformations) explicitly through destinations (or possibly the
#                source field
#                package_id (for CKAN destinations)
#                resource_name (for CKAN destinations)
#                destination_file (a file name that overrides just using the source_file name in the
#                output_files/ directory)

def main(**kwargs):
    selected_job_codes = kwargs.get('selected_job_codes', [])
    use_local_files = kwargs.get('use_local_files', False)
    clear_first = kwargs.get('clear_first', False)
    test_mode = kwargs.get('test_mode', False)
    if selected_job_codes == []:
        selected_jobs = list(jobs)
    else:
        #selected_jobs = [j for j in jobs if (j['source_file'].split('.')[0] in selected_job_codes)] # This is
        # where the extension is pulled off, turning the rest of the file name into an effective job code.
        # However, if two of the jobs have source_file names that match before the first '.' and differ
        # after (like foo.csv and foo.geojson), 'foo' would select both.

        # To handle cases where we want to also be able to pick those jobs by the full filename, when
        # no jobs are selected initially, also select by full filename, extension and all.
        selected_jobs = [j for j in jobs if ((j['source_file'].split('.')[0] in selected_job_codes) or (j['source_file'] in selected_job_codes))]

    for job in selected_jobs:
        kwparameters = dict(kwargs)
        kwparameters['job'] = job
        locators_by_destination = module.process_job(**kwparameters)
        for destination, table_locator in locators_by_destination.items():
            if destination == 'ckan':
                post_process(locators_by_destination[destination])

if __name__ == '__main__':
    if len(sys.argv) != 1:
        payload_path = sys.argv[1]
        # Clean path 1: Remove optional ".py" extension
        payload_path = re.sub('\.py$','',payload_path)
        # Clean path 2: Remove optional leading directories. This allows tab completion
        # from the level of launchpad.py, the engine directory, or the payload subdirectory.
        payload_path = re.sub('^payload\/','',payload_path)
        payload_path = re.sub('^engine\/payload\/','',payload_path)
        # Verify path.
        payload_parts = payload_path.split('/')
        payload_location = '/'.join(payload_parts[:-1])
        module_name = payload_parts[-1]
        full_payload_path = BASE_DIR + 'engine/payload/' + payload_location
        if not os.path.exists(full_payload_path):
            raise ValueError("Unable to find payload directory at {}".format(full_payload_path))
        module_path = full_payload_path + '/' + module_name + '.py'
        if not os.path.exists(module_path):
            raise ValueError("Unable to find payload module at {}".format(module_path))

        module = import_module(module_path, module_name) # We want to import jobs, process_job
        jobs = module.jobs
        for j in jobs:
            j['job_directory'] = payload_parts[-2]

        args = sys.argv[2:]
        copy_of_args = list(args)
        mute_alerts = False
        use_local_files = False
        clear_first = False
        logging = False
        test_mode = not PRODUCTION # Use PRODUCTION boolean from parameters/local_parameters.py to set whether test_mode defaults to True or False
        job_codes = [j['source_file'] for j in jobs] + [j['source_file'].split('.')[0] in jobs] # This is starting to feel a little kludgy.
        # Maybe there's a better way of checking for job codes that simplifies the programming.
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
                elif arg in ['log']:
                    logging = True
                    log_path_plus = LOG_DIR + payload_location + '/' + module_name
                    print(log_path_plus + '-out.log')
                    log_path = '/'.join(log_path_plus.split('/')[:-1])
                    if not os.path.isdir(log_path):
                        print("Creating {}".format(log_path))
                        os.makedirs(log_path)
                    sys.stdout = open(log_path_plus + '-out.log', 'w')
                    sys.stderr = open(log_path_plus + '-err.log', 'w')
                    args.remove(arg)
                elif arg in ['test']:
                    test_mode = True
                    args.remove(arg)
                elif arg in ['production']:
                    test_mode = False
                    args.remove(arg)
                elif arg in job_codes:
                    selected_job_codes.append(arg)
                    args.remove(arg)
            if len(args) > 0:
                print("Unused command-line arguments: {}".format(args))

            kwargs = {'selected_job_codes': selected_job_codes,
                'use_local_files': use_local_files,
                'clear_first': clear_first,
                'test_mode': test_mode,
                }
            main(**kwargs)
        except:
            e = sys.exc_info()[0]
            msg = "Error: {} : \n".format(e)
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            msg = ''.join('!! ' + line for line in lines)
            print(msg) # Log it or whatever here
            if not mute_alerts:
                channel = "@david" if (test_mode or not PRODUCTION) else "#etl-hell"
                send_to_slack(msg,username='{} ETL assistant'.format(payload_location),channel=channel,icon=':illuminati:')
    else:
        print("The first argument should be the payload descriptor (where the script for the job is).")
