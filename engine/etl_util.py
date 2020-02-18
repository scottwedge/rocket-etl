import os, ckanapi, re, sys, requests, decimal
from datetime import datetime
# It's also possible to do this in interactive mode:
# > sudo su -c "sftp -i /home/sds25/keys/pitt_ed25519 pitt@ftp.pittsburghpa.gov" sds25
from engine.wprdc_etl import pipeline as pl
from engine.parameters.remote_parameters import TEST_PACKAGE_ID
from engine.parameters.local_parameters import SETTINGS_FILE

from icecream import ic

from engine.credentials import site, API_key as API_KEY
from engine.parameters.local_parameters import SOURCE_DIR, DESTINATION_DIR

BASE_URL = 'https://data.wprdc.org/api/3/action/'

def scientific_notation_to_integer(s):
    # Source files may contain scientific-notation representations of
    # what should be integers (e.g., '2e+05'). This function can be
    # used to convert such strings to integers.
    return int(decimal.Decimal(s))

def add_datatable_view(resource):
    r = requests.post(
        BASE_URL + 'resource_create_default_resource_views',
        json={
            'resource': resource,
            'create_datastore_views': True
        },
        headers={
            'Authorization': API_KEY,
            'Content-Type': 'application/json'
        }
    )
    print(r.json())
    return r.json()['result']

def configure_datatable(view):
    # setup new view
    view['col_reorder'] = True
    view['export_buttons'] = True
    view['responsive'] = False
    r = requests.post(BASE_URL + 'resource_view_update', json=view, headers={"Authorization": API_KEY})

def reorder_views(resource, views):
    resource_id = resource['id']

    temp_view_list = [view_item['id'] for view_item in views if
                      view_item['view_type'] not in ('datatables_view',)]

    new_view_list = [datatable_view['id']] + temp_view_list
    r = requests.post(BASE_URL + 'resource_view_reorder', json={'id': resource_id, 'order': new_view_list},
                      headers={"Authorization": API_KEY})

def query_resource(site,query,API_key=None):
    """Use the datastore_search_sql API endpoint to query a CKAN resource."""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def get_package_parameter(site,package_id,parameter=None,API_key=None):
    """Gets a CKAN package parameter. If no parameter is specified, all metadata
    for that package is returned."""
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        if parameter is None:
            return metadata
        else:
            return metadata[parameter]
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

def get_resource_parameter(site,resource_id,parameter=None,API_key=None):
    # Some resource parameters you can fetch with this function are
    # 'cache_last_updated', 'package_id', 'webstore_last_updated',
    # 'datastore_active', 'id', 'size', 'state', 'hash',
    # 'description', 'format', 'last_modified', 'url_type',
    # 'mimetype', 'cache_url', 'name', 'created', 'url',
    # 'webstore_url', 'mimetype_inner', 'position',
    # 'revision_id', 'resource_type'
    # Note that 'size' does not seem to be defined for tabular
    # data on WPRDC.org. (It's not the number of rows in the resource.)
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    metadata = ckan.action.resource_show(id=resource_id)
    if parameter is None:
        return metadata
    else:
        return metadata[parameter]

def find_resource_id(package_id,resource_name):
    # Get the resource ID given the package ID and resource name.
    from engine.credentials import site, API_key
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if 'name' in r and r['name'] == resource_name:
            return r['id']
    return None

def resource_exists(package_id, resource_name):
    return find_resource_id(package_id, resource_name) is not None

def datastore_exists(package_id, resource_name):
    from engine.credentials import site, API_key
    resource_id = find_resource_id(package_id, resource_name)
    if resource_id is None:
        return False
    return get_resource_parameter(site, resource_id, 'datastore_active', API_key)

def create_data_table_view(resource):
#    [{'col_reorder': False,
#      'description': '',
#      'export_buttons': False,
#      'filterable': True,
#      'fixed_columns': False,
#      'id': '05a49a72-cb9b-4bfd-aa10-ab3655b541ac',
#      'package_id': '812527ad-befc-4214-a4d3-e621d8230563',
#      'resource_id': '117a09dd-dbe2-44c2-9553-46a05ad3f73e',
#      'responsive': False,
#      'show_fields': ['_id',
#                      'facility',
#                      'run_date',
#                      'gender_race_group',
#                      'patient_count',
#                      'gender',
#                      'race'],
#      'title': 'Data Table',
#      'view_type': 'datatables_view'}]

#    from engine.credentials import site, API_key
#    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
#    extant_views = ckan.action.resource_view_list(id=resource_id)
#    title = 'Data Table'
#    if title not in [v['title'] for v in extant_views]:
#        # CKAN's API can't accept nested JSON to enable the config parameter to be
#        # used to set export_buttons and col_reorder options.
#        # https://github.com/ckan/ckan/issues/2655
#        config_dict = {'export_buttons': True, 'col_reorder': True}
#        #result = ckan.action.resource_view_create(resource_id = resource_id, title="Data Table", view_type='datatables_view', config=json.dumps(config_dict))
#        result = ckan.action.resource_view_create(resource_id=resource_id, title="Data Table", view_type='datatables_view')

        #r = requests.get(BASE_URL + 'package_show', params={'id': package_id})
        #resources = r.json()['result']['resources']

        #good_resources = [resource for resource in resources
        #                  if resource['format'].lower() == 'csv' and resource['url_type'] in ('datapusher', 'upload')]
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    resource_id = resource['id']
    extant_views = ckan.action.resource_view_list(id=resource_id)
    title = 'Data Table'

    if resource['format'].lower() == 'csv' and resource['url_type'] in ('datapusher', 'upload') and resource['datastore_active']:
        if 'datatables_view' not in [v['view_type'] for v in extant_views]:
            print("Adding view for {}".format(resource['name']))
            datatable_view = add_datatable_view(resource)[0]
            # A view will be described like this:
            #    {'col_reorder': False,
            #    'description': '',
            #    'export_buttons': False,
            #    'filterable': True,
            #    'fixed_columns': False,
            #    'id': '3181357a-d130-460f-ac86-e54ae800f574',
            #    'package_id': '812527ad-befc-4214-a4d3-e621d8230563',
            #    'resource_id': '9fc62eb0-10b3-4e76-ba01-8883109a0693',
            #    'responsive': False,
            #    'title': 'Data Table',
            #    'view_type': 'datatables_view'}
            if 'id' in datatable_view.keys():
                configure_datatable(datatable_view)

            # reorder_views(resource, views)

    # [ ] Integrate previous attempt which avoids duplicating views with the same name:
    #if title not in [v['title'] for v in extant_views]:
    #    # CKAN's API can't accept nested JSON to enable the config parameter to be
    #    # used to set export_buttons and col_reorder options.
    #    # https://github.com/ckan/ckan/issues/2655
    #    config_dict = {'export_buttons': True, 'col_reorder': True}
    #    #result = ckan.action.resource_view_create(resource_id = resource_id, title="Data Table", view_type='datatables_view', config=json.dumps(config_dict))
    #    result = ckan.action.resource_view_create(resource_id=resource_id, title="Data Table", view_type='datatables_view')

def set_package_parameters_to_values(site,package_id,parameters,new_values,API_key):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [] # original_values = [get_package_parameter(site,package_id,p,API_key) for p in parameters]
    for p in parameters:
        try:
            original_values.append(get_package_parameter(site,package_id,p,API_key))
        except RuntimeError:
            print("Unable to obtain package parameter {}. Maybe it's not defined yet.".format(p))
    payload = {}
    payload['id'] = package_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    results = ckan.action.package_patch(**payload)
    print("Changed the parameters {} from {} to {} on package {}".format(parameters, original_values, new_values, package_id))

def set_resource_parameters_to_values(site,resource_id,parameters,new_values,API_key):
    """Sets the given resource parameters to the given values for the specified
    resource.

    This fails if the parameter does not currently exist. (In this case, use
    create_resource_parameter().)"""
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    original_values = [get_resource_parameter(site,resource_id,p,API_key) for p in parameters]
    payload = {}
    payload['id'] = resource_id
    for parameter,new_value in zip(parameters,new_values):
        payload[parameter] = new_value
    #For example,
    #   results = ckan.action.resource_patch(id=resource_id, url='#', url_type='')
    results = ckan.action.resource_patch(**payload)
    print(results)
    print("Changed the parameters {} from {} to {} on resource {}".format(parameters, original_values, new_values, resource_id))

def add_tag(package, tag='_etl'):
    tag_dicts = package['tags']
    tags = [td['name'] for td in tag_dicts]
    if tag not in tags:
        from engine.credentials import site, API_key
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        new_tag_dict = {'name': tag}
        tag_dicts.append(new_tag_dict)
        set_package_parameters_to_values(site,package['id'],['tags'],[tag_dicts],API_key)

def convert_extras_dict_to_list(extras):
    extras_list = [{'key': ekey, 'value': evalue} for ekey,evalue in extras.items()]
    return extras_list

def set_extra_metadata_field(package,key,value):
    if 'extras' in package:
        extras_list = package['extras']
        # Keep definitions and uses of extras metadata updated here:
        # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md

        # The format as obtained from the CKAN API is like this:
        #       u'extras': [{u'key': u'dcat_issued', u'value': u'2014-01-07T15:27:45.000Z'}, ...
        # not a dict, but a list of dicts.
        extras = {d['key']: d['value'] for d in extras_list}
    else:
        extras = {}

    extras[key] = value
    extras_list = convert_extras_dict_to_list(extras)
    from engine.credentials import site, API_key
    set_package_parameters_to_values(site,package['id'],['extras'],[extras_list],API_key)

def update_etl_timestamp(package,resource):
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    set_extra_metadata_field(package,key='last_etl_update',value=datetime.now().isoformat())
    # Keep definitions and uses of extras metadata updated here:
    # https://github.com/WPRDC/data-guide/blob/master/docs/metadata_extras.md

def get_resource_by_id(resource_id):
    """Get all metadata for a given resource."""
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    return ckan.action.resource_show(id=resource_id)

def get_package_by_id(package_id):
    """Get all metadata for a given resource."""
    from engine.credentials import site, API_key
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    return ckan.action.package_show(id=package_id)

def post_process(resource_id):
    # Create a DataTable view if the resource has a datastore.
    resource = get_resource_by_id(resource_id)
    package_id = resource['package_id']
    package = get_package_by_id(package_id)
    create_data_table_view(resource)
    add_tag(package, '_etl')
    update_etl_timestamp(package, resource)


def lookup_parcel(parcel_id):
    """Accept parcel ID for Allegheny County parcel and return geocoordinates."""
    site = "https://data.wprdc.org"
    resource_id = '23267115-177e-4824-89d9-185c7866270d' #2018 data
    #resource_id = "4b68a6dd-b7ea-4385-b88e-e7d77ff0b294" #2016 data
    query = 'SELECT x, y FROM "{}" WHERE "PIN" = \'{}\''.format(resource_id,parcel_id)
    results = query_resource(site,query)
    assert len(results) < 2
    if len(results) == 0:
        return None, None
    elif len(results) == 1:
        return results[0]['y'], results[0]['x']

def local_file_and_dir(jobject, base_dir, file_key='source_file'):
    # The location of the payload script (e.g., rocket-etl/engine/payload/ac_hd/script.py)
    # provides the job directory (ac_hd).
    # This is used to file the source files in a directory structure that
    # mirrors the directory structure of the jobs.
    #local_directory = "/home/sds25/wprdc-etl/source_files/{}/".format(job_directory)
    local_directory = base_dir + "{}/".format(jobject.job_directory) # Note that the
    # job_directory field is assigned by launchpad.py.
    #directory = '/'.join(date_filepath.split('/')[:-1])
    if not os.path.isdir(local_directory):
        os.makedirs(local_directory)
    local_file_path = local_directory + (getattr(jobject,file_key) if file_key in jobject.__dict__ else jobject.source_file)
    return local_file_path, local_directory

def ftp_target(jobject):
    target_path = jobject.source_file
    if jobject.source_dir != '':
        target_path = re.sub('/$','',jobject.source_dir) + '/' + target_path
    return target_path

def download_city_directory(jobject, local_target_directory):
    """For this function to be able to access the City's FTP server,
    it needs to be able to access the appropriate key file."""
    from engine.parameters.local_parameters import CITY_KEYFILEPATH
    cmd = "sftp -i {} pitt@ftp.pittsburghpa.gov:/pitt/{}/* {}".format(CITY_KEYFILEPATH, jobject.source_dir, local_target_directory)
    results = os.popen(cmd).readlines()
    for result in results:
        print(" > {}".format(result))
    return results

def fetch_city_file(jobject):
    """For this function to be able to get a file from the City's FTP server,
    it needs to be able to access the appropriate key file."""
    from engine.parameters.local_parameters import CITY_KEYFILEPATH
    filename = ftp_target(jobject)
    _, local_directory = local_file_and_dir(jobject, SOURCE_DIR)
    cmd = "sftp -i {} pitt@ftp.pittsburghpa.gov:/pitt/{} {}".format(CITY_KEYFILEPATH, filename, local_directory)
    results = os.popen(cmd).readlines()
    for result in results:
        print(" > {}".format(result))
    return results

#############################################

class Job:
    # It may be a good idea to make a BaseJob and then add different features
    # based on source_type.
    def __init__(self, job_dict):
        self.job_directory = job_dict['job_directory']
        self.source_type = job_dict['source_type']
        self.source_url_path = job_dict['source_url_path'] if 'source_url_path' in job_dict else None
        self.source_file = job_dict['source_file'] if 'source_file' in job_dict else None
        self.source_dir = job_dict['source_dir'] if 'source_dir' in job_dict else ''
        self.encoding = job_dict['encoding'] if 'encoding' in job_dict else 'utf-8' # wprdc-etl/pipeline/connectors.py also uses UTF-8 as the default encoding.
        self.connector_config_string = job_dict['connector_config_string'] if 'connector_config_string' in job_dict else ''
        self.custom_processing = job_dict['custom_processing'] if 'custom_processing' in job_dict else (lambda *args, **kwargs: None)
        self.schema = job_dict['schema'] if 'schema' in job_dict else None
        self.primary_key_fields = job_dict['primary_key_fields'] if 'primary_key_fields' in job_dict else None
        self.upload_method = job_dict['upload_method'] if 'upload_method' in job_dict else None
        self.always_clear_first = job_dict['always_clear_first'] if 'always_clear_first' in job_dict else False
        self.destinations = job_dict['destinations'] if 'destinations' in job_dict else ['ckan']
        self.destination_file = job_dict.get('destination_file', None) # What should be done if destination_file is None?
        if 'file' in self.destinations and self.destination_file is None:
            raise ValueError("Since the 'destinations' parameter includes 'file', either the 'destination_file' parameter should be set, or a reasonable default should be coded into this framework.")
        self.package = job_dict['package'] if 'package' in job_dict else None
        self.resource_name = job_dict['resource_name'] if 'resource_name' in job_dict else None
        #self.clear_first = job['clear_first'] if 'clear_first' in job else False
        self.target, self.local_directory = local_file_and_dir(self, base_dir = SOURCE_DIR)
        self.local_cache_filepath = self.local_directory + job_dict['source_file']

    def default_setup(self, use_local_files): # Rename this to reflect how it modifies parameters based on command-line-arguments.
        print("==============\n" + self.resource_name)
        if self.package == TEST_PACKAGE_ID:
            print(" *** Note that this job currently only writes to the test package. ***")

        if use_local_files:
            self.source_type = 'local'

        if self.source_type is not None:
            if self.source_type == 'http': # It's noteworthy that assigning connectors at this stage is a
                # completely different approach than the way destinations are handled currently
                # (the destination type is passed to run_pipeline which then configures the loaders)
                # but I'm experimenting with this as it seems like it might be a better way of separating
                # such things.
                self.source_connector = pl.RemoteFileConnector # This is the connector to use for files available via HTTP.
                if not use_local_files:
                    self.target = self.source_url_path + '/' + self.source_file
            elif self.source_type == 'sftp':
                self.target = ftp_target(self)
                self.source_connector = pl.SFTPConnector
            elif self.source_type == 'local':
                self.source_connector = pl.FileConnector
            else:
                raise ValueError("The source_type {} has no specified connector in default_job_setup().".format(self.source_type))
        else:
            raise ValueError("The source_type is not specified.")
            # [ ] What should we do if no source_type (or no source) is specified?

        self.destination_file_path, self.destination_directory = local_file_and_dir(self, base_dir = DESTINATION_DIR, file_key = 'destination_file')
        if use_local_files or self.source_type == 'local':
            if self.target == self.destination_file_path:
                raise ValueError("It seems like a bad idea to have the source file be the same as the destination file! Aborting pipeline execution.")

        ic(self.__dict__)
        self.loader_config_string = 'production' # Would it be useful to set the
        # loader_config_string from the command line? It was useful enough
        # in park-shark to design a way to do this, checking command-line
        # arguments aganinst a static list of known keys in the CKAN
        # settings.json file. Doing that more generally would require
        # some modification to the current command-line parsing.
        # It's doable, but with the emergence of the test_mode idea of having
        # one testing package ID, it does not seem that necessary to ALSO be
        # able to specify other destinations unless we return to using a staging
        # server. (I'm not using this now, particularly as we're drifting away
        # from even using the setting.json file, preferring to keep most stuff
        # in flat credentials.py files.)

        # Note that loader_config_string's use can be seen in the load()
        # function of Pipeline from wprdc-etl.

        # [ ] Move this to Job.__init__

    def select_extractor(self):
        extension = (self.source_file.split('.')[-1]).lower()
        if extension == 'csv':
            self.extractor = pl.CSVExtractor
        elif extension in ['xls', 'xlsx']:
            self.extractor = pl.ExcelExtractor
        else:
            raise ValueError("No known extractor for file extension .{}".format(extension))

    def run_pipeline(self, test_mode, clear_first, file_format='csv', retry_without_last_line=False):
        # This is a generalization of push_to_datastore() to optionally use
        # the new FileLoader (exporting data to a file rather than just CKAN).

        # target is a filepath which is actually the source filepath.

        # The retry_without_last_line option is a way of dealing with CSV files
        # that abruptly end mid-line.
        locators_by_destination = {}
        for destination in self.destinations:
            package_id = self.package if not test_mode else TEST_PACKAGE_ID # Should this be done elsewhere?
            # [ ] Maybe the use_local_files and test_mode and any other parameters should be applied in a discrete stage between initialization and running.

            if destination == 'ckan_filestore':
                # Maybe a pipeline is not necessary to just upload a file to a CKAN resource.
                ua = 'rocket-etl/1.0 (+https://tools.wprdc.org/)'
                ckan = ckanapi.RemoteCKAN(site, apikey=API_KEY, user_agent=ua)

                source_file_format = self.source_file.split('.')[-1].lower()
                # While wprdc_etl uses 'CSV' as a
                # format that it sends to CKAN, I'm inclined to switch to 'csv',
                # and uniformly lowercasing all file formats.

                # Though given a format of 'geojson', the CKAN API resource_create
                # response lists format as 'GeoJSON', so CKAN is doing some kind
                # of correction.
                upload_kwargs = {'package_id': package_id,
                        'format': source_file_format,
                        'url': 'dummy-value',  # ignored but required by CKAN<2.6
                        'upload': open(self.target, 'r')} # target is the source file path
                if not resource_exists(package_id, self.resource_name):
                    upload_kwargs['name'] = self.resource_name
                    result = ckan.action.resource_create(**upload_kwargs)
                    print('Creating new resource and uploading file to filestore...')
                else:
                    upload_kwargs['id'] = find_resource_id(package_id, self.resource_name)
                    result = ckan.action.resource_update(**upload_kwargs)
                    print('Uploading file to filestore...')
            elif destination == 'local_monthly_archive_zipped':
                # [ ] Break all of this LMAZ code off into one or more separate functions.
                loader = pl.FileLoader
                self.upload_method = 'insert'
                # Try using destination_directory and destination_file_path for the archives.
                # Append the year-month to the filename (before the extension).
                pathparts = self.destination_file_path.split('/')
                filenameparts = pathparts[-1].split('.')
                now = datetime.now()
                last_month_num = (now.month - 1) % 12
                year = now.year
                if last_month_num == 0:
                    last_month_num = 12
                if last_month_num == 12: # If going back to December,
                    year -= 1            # set year to last year
                last_month_str = str(last_month_num)
                if len(last_month_str) == 1:
                    last_month_str = '0' + last_month_str
                regex_parts = list(filenameparts)
                filenameparts[-2] += "_{}-{}".format(year, last_month_str)
                timestamped_filename = '.'.join(filenameparts)

                regex_parts[-2] += "_{}".format(year)
                regex_pattern = '.'.join(regex_parts[:-1]) # This is for matching filenames that should be rolled up into the same year of data.
                zip_file_name = regex_pattern + '.zip'

                pathparts[-1] = timestamped_filename
                destination_file_path = '/'.join(pathparts) # This is the new timestamped filepath.
                ic(self.destination_file_path)
                # Store the file locally

                # Zip the files with matching year in filename.
                destination_directory = '/'.join(self.destination_file_path.split('/')[:-1])
                all_files = os.listdir(destination_directory)
                list_of_files_to_compress = sorted([f for f in all_files if re.match(regex_pattern, f)])

                #cp synthesized-liens.csv zipped/liens-with-current-status-beta.csv
                zip_file_path = destination_directory + '/' + zip_file_name
                #zip zipped/liens-with-current-status-beta.zip zipped/liens-with-current-status-beta.csv
                import zipfile
                process_zip = zipfile.ZipFile(zip_file_path, 'w')
                for original_file_name in list_of_files_to_compress:
                    file_to_zip = destination_directory + '/' + original_file_name
                    process_zip.write(file_to_zip, original_file_name, compress_type=zipfile.ZIP_DEFLATED)
                process_zip.close()
                # Upload the file at zip_file_path to the appropriate resource.
                #####resource_id =  # [ ] This lmaz option needs to be finished.

                # Delete the file at zip_file_path.
                os.remove(zip_file_path)
                # Have the parameters that are being passed to curr_pipeline below correct for uplading the zipped archive? ########
            else:
                self.select_extractor()
                if destination == 'ckan':
                    loader = pl.CKANDatastoreLoader
                elif destination == 'file':
                    loader = pl.FileLoader
                    self.upload_method = 'insert' # Note that this will always append records to an existing file
                    # unless 'always_clear_first' is set to True.
                else:
                    raise ValueError("run_pipeline does not know how to handle destination = {}".format(destination))

                clear_first = clear_first or self.always_clear_first
                if clear_first:
                    if destination in ['ckan']:
                        if datastore_exists(package_id, self.resource_name):
                            print("Clearing the datastore for {}".format(self.resource_name))
                        else:
                            print("Since it makes no sense to try to clear a datastore that does not exist, clear_first is being toggled to False.")
                            clear_first = False

                # Upload data to datastore
                print('Uploading tabular data...')
                curr_pipeline = pl.Pipeline(self.resource_name + ' pipeline', self.resource_name + ' Pipeline', log_status=False, chunk_size=1000, settings_file=SETTINGS_FILE, retry_without_last_line = retry_without_last_line) \
                    .connect(self.source_connector, self.target, config_string=self.connector_config_string, encoding=self.encoding, local_cache_filepath=self.local_cache_filepath) \
                    .extract(self.extractor, firstline_headers=True) \
                    .schema(self.schema) \
                    .load(loader, self.loader_config_string,
                          filepath = self.destination_file_path,
                          file_format = file_format,
                          fields = self.schema().serialize_to_ckan_fields(),
                          key_fields = self.primary_key_fields,
                          package_id = package_id,
                          resource_name = self.resource_name,
                          clear_first = clear_first,
                          method = self.upload_method).run()

            if destination in ['ckan', 'ckan_filestore', 'local_monthly_archive_zipped']:
                resource_id = find_resource_id(package_id, self.resource_name) # This IS determined in the pipeline, so it would be nice if the pipeline would return it.
                locators_by_destination[destination] = resource_id
            elif destination in ['file']:
                locators_by_destination[destination] = self.destination_file_path
        return locators_by_destination

    def process_job(self, **kwparameters):
        #job = kwparameters['job'] # Here job is the class instance, so maybe it shouldn't be passed this way...
        use_local_files = kwparameters['use_local_files']
        clear_first = kwparameters['clear_first']
        test_mode = kwparameters['test_mode']
        self.default_setup(use_local_files)
        self.custom_processing(self, **kwparameters)
        locators_by_destination = self.run_pipeline(test_mode, clear_first, file_format='csv')
        return locators_by_destination # Return a dict allowing look up of final destinations of data (filepaths for local files and resource IDs for data sent to a CKAN instance).

def push_to_datastore(job, file_connector, target, config_string, encoding, loader_config_string, primary_key_fields, test_mode, clear_first, upload_method='upsert'):
    # This is becoming a legacy function because all the new features are going into run_pipeline,
    # but note that this is still used at present by a parking ETL job.
    package_id = job['package'] if not test_mode else TEST_PACKAGE_ID
    resource_name = job['resource_name']
    schema = job['schema']
    extractor = select_extractor(job)
    # Upload data to datastore
    if clear_first:
        print("Clearing the datastore for {}".format(job['resource_name']))
    print('Uploading tabular data...')
    curr_pipeline = pl.Pipeline(job['resource_name'] + ' pipeline', job['resource_name'] + ' Pipeline', log_status=False, chunk_size=1000, settings_file=SETTINGS_FILE) \
        .connect(file_connector, target, config_string=config_string, encoding=encoding) \
        .extract(extractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, loader_config_string,
              fields=schema().serialize_to_ckan_fields(),
              key_fields=primary_key_fields,
              package_id=package_id,
              resource_name=resource_name,
              clear_first=clear_first,
              method=upload_method).run()

    resource_id = find_resource_id(package_id, resource_name) # This IS determined in the pipeline, so it would be nice if the pipeline would return it.
    return resource_id
