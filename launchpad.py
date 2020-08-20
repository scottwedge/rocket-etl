import os, sys, requests, csv, json, traceback, re
from marshmallow import fields, pre_load, post_load

from engine.wprdc_etl import pipeline as pl

from engine.credentials import API_key
from engine.parameters.local_parameters import BASE_DIR, LOG_DIR, PRODUCTION
from engine.etl_util import post_process, Job
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
# job_dicts is a list of job descriptions imported from the ETL script in engine/payload/<whatever>/<actual_script>.py
# Each job_dict is a dict which may contain the following fields:
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

def code_is_in_job_dict(code, job_dict):
    """Identify jobs by a command-line-specified job code which could be 1) the full name of
    the source file, 2) the name of the source file without the extension, or 3) the
    explicitly specified 'job_code' in the job_dict."""
    if 'source_file' in job_dict:
        if code == job_dict['source_file'] or code == job_dict['source_file'].split('.')[0]:
            return True
    if 'job_code' in job_dict:
        if code == job_dict['job_code']:
            return True
    return False

def is_job_code(candidate_code, job_dicts):
    for job_dict in job_dicts:
        if code_is_in_job_dict(candidate_code, job_dict):
            return True
    return False

def select_jobs_by_code(selected_job_codes, job_dicts):
    """This function takes some job codes and from a list of job dicts
    returns the selected jobs (in object format)."""
    selected_jobs = []
    # While the double loop below is very simlar to looping over job codes
    # and then running is_job_code, the difference is that this allows
    # selection of the job_dict.
    for job_dict in job_dicts:
        for job_code in selected_job_codes:
            if code_is_in_job_dict(job_code, job_dict):
                selected_jobs.append(Job(job_dict))
    return selected_jobs

def main(**kwargs):
    selected_job_codes = kwargs.get('selected_job_codes', [])
    use_local_files = kwargs.get('use_local_files', False)
    clear_first = kwargs.get('clear_first', False)
    wipe_data = kwargs.get('wipe_data', False)
    test_mode = kwargs.get('test_mode', False)
    if selected_job_codes == []:
        selected_jobs = [Job(job_dict) for job_dict in job_dicts]
    else:
        selected_jobs = select_jobs_by_code(selected_job_codes, job_dicts)


    # [ ] Add in script-level post-processing here, allowing the data.json file of an ArcGIS
    # server to be searched for unharvested tables.
    for job in selected_jobs:
        kwparameters = dict(kwargs)
        locators_by_destination = job.process_job(**kwparameters)
        for destination, table_locator in locators_by_destination.items():
            if destination == 'ckan':
                post_process(locators_by_destination[destination])

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'test_all':
        # This is an option to find and run all jobs in the payload directories sequentially.
        # This serves as a kind of test of the ETL system after new changes have been
        # deployed.
        # What is missing from this approach is validation by checking the resulting
        # CKAN resources against some reference.

        # Searching payloads
        full_payload_path = BASE_DIR + 'engine/payload/'
        dir_paths = [f.path for f in os.scandir(full_payload_path) if f.is_dir() and f.name[:2] != '__']
        # dir_paths excludes directories that start with a double underscore.
        for dir_path in dir_paths:
            # For each payload directory, find all scripts that are valid jobs and run them in test mode.
            file_paths = [f.path for f in os.scandir(dir_path) if f.is_file() and f.name != '__init__.py' and f.name[0] != '_']
            for module_path in file_paths:
                if module_path[-3:] == '.py':
                    module_name = module_path.split('/')[-1][:-3]
                    module = import_module(module_path, module_name) # We want to import job_dicts
                    job_dicts = module.job_dicts
                    jobs_directory = module_path.split('/')[-2]
                    for job_dict in job_dicts:
                        job_dict['job_directory'] = jobs_directory # Add 'job_directory' field to each job.
                    kwargs = {'selected_job_codes': [],
                        'use_local_files': False,
                        'clear_first': False,
                        'wipe_data': False,
                        'test_mode': True,
                        }
                    try:
                        main(**kwargs) # Try to run all jobs in the module.
                    except FileNotFoundError:
                        print("*** {} terminated with a FileNotFoundError. ***".format(module))

    elif len(sys.argv) != 1:
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

        module = import_module(module_path, module_name) # We want to import job_dicts
        job_dicts = module.job_dicts
        for job_dict in job_dicts:
            job_dict['job_directory'] = payload_parts[-2]

        args = sys.argv[2:]
        copy_of_args = list(args)
        mute_alerts = False
        use_local_files = False
        clear_first = False
        wipe_data = False
        logging = False
        test_mode = not PRODUCTION # Use PRODUCTION boolean from parameters/local_parameters.py to set whether test_mode defaults to True or False
        wake_me_when_found = False
        selected_job_codes = []
        if not PRODUCTION and 'test' not in copy_of_args and 'production' not in copy_of_args:
            print("Remember that to make changes to production datasets when on a PRODUCTION = False, it's necessary to use the command-line parameter 'production'.")
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
                elif arg in ['wipe_data']:
                    wipe_data = True
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
                elif arg in ['wake_me_when_found', 'wake_me']:
                    wake_me_when_found = True
                    # This parameter may be used (for instance) to run an ETL job during periods when the file is not expected to be present on the source server.
                    # For instance, if a file appears on an FTP server just for the first two weeks of the month, there should be two cron jobs:
                    # The first has a day range of 1-14 and has a default set of command-line arguments.
                    # The second has a day range of 15-31 and appends "wake_me_when_found" to the command-line arguments of the previous cron job.
                    args.remove(arg)
                elif is_job_code(arg, job_dicts):
                    selected_job_codes.append(arg)
                    args.remove(arg)
            if len(args) > 0:
                print("Unused command-line arguments: {}".format(args))

            kwargs = {'selected_job_codes': selected_job_codes,
                'use_local_files': use_local_files,
                'clear_first': clear_first,
                'wipe_data': wipe_data,
                'test_mode': test_mode,
                }
            main(**kwargs)

            if wake_me_when_found:
                msg = "A file that was not expected (one of these: {}) has resurfaced!\nIf this was a one-time outage, you can remove the wake_me_when_found parameter from the cron job for this ETL process.\nIf this is a source file that appears on some schedule, the file has appeared at an unexpected time. The cron job date specification might need to be altered.".format(list(set([j_dict['source_file'] for j_dict in job_dicts])))
                print(msg)
                if not mute_alerts:
                    channel = "@david" if (test_mode or not PRODUCTION) else "#etl-hell"
                    send_to_slack(msg,username='{}/{} ETL assistant'.format(payload_location,module_name),channel=channel,icon=':illuminati:')
        except:
            e = sys.exc_info()[0]
            if e == FileNotFoundError and wake_me_when_found:
                print("As expected, this script threw an exception because the ETL framework could not find a source file.")
            else:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                msg = ''.join('!! ' + line for line in lines)
                print(msg) # Log it or whatever here
                if not mute_alerts:
                    channel = "@david" if (test_mode or not PRODUCTION) else "#etl-hell"
                    send_to_slack(msg,username='{}/{} ETL assistant'.format(payload_location,module_name),channel=channel,icon=':illuminati:')
    else:
        print("The first argument should be the payload descriptor (where the script for the job is).")
