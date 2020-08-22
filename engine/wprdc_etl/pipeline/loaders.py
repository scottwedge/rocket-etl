import requests, os, csv
import json
import datetime
import time

from engine.wprdc_etl.pipeline.exceptions import CKANException
from engine.credentials import site, API_key
import ckanapi

from pprint import pprint

def check_keys_in_extant_file(keys, filename):
    """Checks that the keys of filename (which has already been verified to be an existing file)
    include all the keys passed as a list to this function."""

    with open(filename, 'r') as f:
        dr = csv.DictReader(f)
        extant_fields = dr.fieldnames # This is an order-preserving list.
        outliers = set(keys).difference(set(extant_fields))
        all_in = (len(outliers) == 0)
        if not all_in:
            raise ValueError(f'The fields {outliers} do not appear in the CSV file {filename}.')
        return extant_fields

class Loader(object):
    def __init__(self, *args, **kwargs):
        pass

    def load(self, data):
        '''Main load method for Loaders to implement

        Raises:
            NotImplementedError
        '''
        raise NotImplementedError

class CKANLoader(Loader):
    """Connection to CKAN datastore"""
    # Currently CKANLoader may contain some functions that really
    # ought to be in CKANDatastoreLoader if CKANLoader is to be
    # for any resource (including files in the Filestore or just
    # plain URL links):

    # Current CKANLoader functions
    # __init__
    # get_resource_id
    # resource_exists
    # create_resource
    # create_datastore    \  These three should (and can)
    # generate_datastore   | probably be moved to
    # delete_datastore    /  CKANDatastoreLoader (though it's not urgent).
    # upsert
    # update_metadata

    # However, a previous attempt to add a CKANFilestoreLoader
    # revealed that the wprdc_etl framework is currently written
    # to require all stages in a pipeline (e.g., extractor and
    # schema), and it was concluded that it was not worth
    # coding workarounds just to upload a file (which is
    # currently being done with a small number of lines of
    # code in rocket-etl/engine/etl_util.py:run_pipeline()).

    def __init__(self, *args, **kwargs):
        super(CKANLoader, self).__init__(*args, **kwargs)
        self.ckan_url = kwargs.get('ckan_root_url').rstrip('/') + '/api/3/'
        self.dump_url = kwargs.get('ckan_root_url').rstrip('/') + '/datastore/dump/'
        self.key = kwargs.get('ckan_api_key')
        self.package_id = kwargs.get('package_id')
        self.resource_name = kwargs.get('resource_name')
        self.resource_id = kwargs.get('resource_id',
                                      self.get_resource_id(self.package_id, self.resource_name))

    def get_resource_id(self, package_id, resource_name):
        """Search for resource within a CKAN dataset and returns its ID

        Params:
            package_id: ID of the resource's parent dataset
            resource_name: name of the resource

        Returns:
            The resource ID if the resource is found within the package;
            ``None`` otherwise
        """
        response = requests.post(
            self.ckan_url + 'action/package_show',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': package_id
            })
        )
        # todo: handle bad request
        response_json = response.json()
        resource_id = next((i['id'] for i in response_json['result']['resources'] if 'name' in i and resource_name == i['name']), None)
        # Note that 'name' can be missing from a resource description if it is created without a name.
        return resource_id


    def resource_exists(self, package_id, resource_name):
        """Search for the existence of a resource on CKAN instance

        Params:
            package_id: ID of resource's parent dataset
            resource_name: name of the resource

        Returns:
            ``True`` if the resource is found within the package,
            ``False`` otherwise
        """
        resource_id = self.get_resource_id(package_id, resource_name)
        return (resource_id is not None)

    def create_resource(self, package_id, resource_name):
        '''Create a new resource on the CKAN instance

        Params:
            package_id: dataset under which the new resource should be added
            resource_name: name of the new resource

        Returns:
            ID of the newly created resource if successful,
            ``None`` otherwise
        '''

        # Make api call
        response = requests.post(
            self.ckan_url + 'action/resource_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'package_id': package_id,
                'url': '#',
                'name': resource_name,
                'url_type': 'datapusher',
                'format': 'CSV'
            })
        )

        response_json = response.json()

        if not response_json.get('success', False):
            raise CKANException('An error occured: {}'.format(response_json['error']['__type'][0]))

        return response_json['result']['id']

    def create_datastore(self, resource_id, fields):
        """Create new datastore for specified resource

        Params:
            resource_id: resource ID for which the new datastore is being made
            fields: header fields for the CSV file

        Returns:
            resource_id for the new datastore if successful

        Raises:
            CKANException if resource creation is unsuccessful
        """

        # Make API call
        create_datastore = requests.post(
            self.ckan_url + 'action/datastore_create',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True,
                'fields': fields,
                'primary_key': self.key_fields if hasattr(self, 'key_fields') else None,
                'indexes': self.indexes if hasattr(self, 'indexes') else None
            })
        )
        # Note that
        #   https://github.com/ckan/ckan/blob/7fd6ca6439e3a7db60787283148652f895b02920/ckanext/datastore/tests/test_create.py
        # shows this as an example value for the indexes field:
        #  'indexes': [['boo%k', 'author'], 'author'],
        # This appears to demonstrate how to make 'author' and and also
        # the combination of 'author' and 'boo%k' things that are indexed.

        # https://github.com/ckan/ckan/blob/b6298333453650cd9dbb3f5d3566da719804ecca/ckanext/datastore/backend/postgres.py
        # contains these checks:
            # if indexes is not None:...
            # if primary_key is not None:...
        # This suggests that passing these values as None should be fine.
        create_datastore = create_datastore.json()

        if not create_datastore.get('success', False):
            if 'name' in create_datastore['error'] and type(create_datastore['error']['name']) == list:
                error_message = create_datastore['error']['name'][0]
            else:
                error_message = create_datastore['error']
            raise CKANException('An error occured: {}'.format(error_message))

        return create_datastore['result']['resource_id']


    def generate_datastore(self, fields, clear, first, wipe_data):
        if wipe_data and first:
            # Delete all the records in the datastore, preserving the schema.
            ckan = ckanapi.RemoteCKAN(site, apikey=self.key)
            response = ckan.action.datastore_delete(id=self.resource_id, filters={}, force=True)
            # Deleting the records in the datastore also has the side effect of deactivating the
            # datastore, so we need to reactivate it.
            response2 = ckan.action.resource_patch(id=self.resource_id, datastore_active=True)
        elif clear and first:
            delete_status = self.delete_datastore(self.resource_id)
            if str(delete_status)[0] in ['4', '5']:
                if str(delete_status) == '404':
                    print("The datastore currently doesn't exist, so let's create it!")
                else:
                    raise RuntimeError('Delete failed with status code {}.'.format(str(delete_status)))
            self.create_datastore(self.resource_id, fields)

        elif self.resource_id is None:
            self.resource_id = self.create_resource(self.package_id, self.resource_name)
            self.create_datastore(self.resource_id, fields)

        return self.resource_id


    def delete_datastore(self, resource_id):
        """Deletes datastore table for resource

        Params:
            resource: resource_id to remove table from

        Returns:
            Status code from the request
        """
        delete = requests.post(
            self.ckan_url + 'action/datastore_delete',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'force': True
            })
        )
        return delete.status_code

    def upsert(self, resource_id, data, method='upsert'):
        """Upsert data into datastore

        Params:
            resource_id: resource_id to which data will be inserted
            data: data to be upserted

        Returns:
            request status
        """
        upsert = requests.post(
            self.ckan_url + 'action/datastore_upsert',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'resource_id': resource_id,
                'method': method,
                'force': True,
                'records': data
            })
        )
        if upsert.status_code != 200:
            print(f"Attempted upsert returned with status code {upsert.status_code}, reason '{upsert.reason}', and also this explanation:\n{upsert.text}\n")
        return upsert.status_code

    def update_metadata(self, resource_id, just_last_modified=False):
        """Update a resource's metadata

        TODO: Make this versatile

        Params:
            resource_id: ID of the resource for which the metadata will be modified
            just_last_modified: if True, this function should only change the
            last_modified metadata field (to avoid changing the URL from whatever
            link has been deliberately put there [like a downstream link] to
            the default dump URL (as shown below))

        Returns:
            request status
        """
        kwparameters = {
                'id': resource_id,
                'last_modified': datetime.datetime.now().isoformat(),
            }
        if not just_last_modified:
            kwparameters['url'] = self.dump_url + str(resource_id)
            kwparameters['url_type'] = 'datapusher'

        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps(kwparameters)
        )
        return update.status_code

class CKANDatastoreLoader(CKANLoader):
    '''Store data in CKAN using an upsert strategy
    '''

    def __init__(self, *args, **kwargs):
        '''Constructor for new CKANDatastoreLoader

        Arguments:
            config: location of a configuration file

        Keyword Arguments:
            fields: List of CKAN fields. CKAN fields must be
                formatted as a list of dictionaries with
                ``id`` and ``type`` keys.
            key_fields: Primary key field
            indexes: Optional list of fields to index (but
                not make primary keys)
            method: Must be one of ``upsert`` or ``insert``.
                Defaults to ``upsert``. See
                :~pipeline.loaders.CKANLoader.upsert:
            clear_first: True when the entire datastore should
                be deleted before loading new data. (Useful
                when the schema or primary key changes.)
            wipe_data: True when the records in the datastore
                should be deleted but the Fields (and possibly
                the integrated data dictionary) should be kept.
                (Implicitly wipe_data == True implies
                clear_first == False.)

        Raises:
            RuntimeError if fields is not specified or method is
            ``upsert`` and no ``key_fields`` are passed.
        '''
        super(CKANDatastoreLoader, self).__init__(*args, **kwargs)
        self.fields = kwargs.get('fields', None)
        self.key_fields = kwargs.get('key_fields', None)
        self.indexes = kwargs.get('indexes', None)
        self.method = kwargs.get('method', 'upsert')
        self.header_fix = kwargs.get('header_fix', None)
        self.clear_first = kwargs.get('clear_first', False)
        self.wipe_data = kwargs.get('wipe_data', False)
        self.first_pass = True

        if self.fields is None:
            raise RuntimeError('Fields must be specified.')
        if self.method == 'upsert' and self.key_fields is None:
            raise RuntimeError('Upsert method requires primary key(s).')
        if self.clear_first and not self.resource_id:
            raise RuntimeError('Resource must already exist in order to be cleared.')
        if self.wipe_data and not self.resource_id:
            raise RuntimeError('Resource must already exist in order to wipe its records.')
        if self.wipe_data and self.clear_first:
            raise RuntimeError('wipe_data and clear_first can not both be True at once.')

    def load(self, data):
        '''Load data to CKAN using an upsert strategy

        Arguments:
            data: a list of records to be inserted into or upserted
                to the configured CKAN instance

        Raises:
            RuntimeError if the upsert or update metadata
                calls are unsuccessful

        Returns:
            A two-tuple of the status codes for the upsert
            and metadata update calls
        '''
        self.generate_datastore(self.fields, self.clear_first, self.first_pass, self.wipe_data)
        self.first_pass = False
        upsert_status = self.upsert(self.resource_id, data, self.method)
        update_status = self.update_metadata(self.resource_id)

        if upsert_status == 409:
            print("dir(self) = {}".format(dir(self)))
            pprint(self.fields)
            print("key_fields = {}".format(self.key_fields))
            if hasattr(self, 'indexes') and self.indexes is not None:
                print("indexes = {}".format(self.indexes))
            raise RuntimeError('Upsert failed with status code {}. This may be because of a conflict between datastore fields/keys and specified primary keys. Or maybe you are trying to insert a row into a resource with an existing row with the same primary key or keys. But check the more informative explanation above.'.format(str(upsert_status)))

        if str(upsert_status)[0] in ['4', '5']:
            raise RuntimeError('Upsert failed with status code {}.'.format(str(upsert_status)))
        elif str(update_status)[0] in ['4', '5']:
            time.sleep(1)
            update_status = self.update_metadata(self.resource_id) # Try again.
            if str(update_status)[0] in ['4', '5']:
                raise RuntimeError('Metadata update failed (twice) with status code {}'.format(str(update_status)))
        else:
            return upsert_status, update_status

class FileLoader(Loader):
    """Write data to a local file (mostly for testing purposes)."""

    def __init__(self, *args, **kwargs):
        super(FileLoader, self).__init__(*args, **kwargs)
        self.filepath = kwargs.get('filepath')
        self.file_format = kwargs.get('file_format').lower()
        self.fields = kwargs.get('fields', None)
        self.key_fields = kwargs.get('key_fields', None)
        self.method = kwargs.get('method', 'upsert')
        self.clear_first = kwargs.get('clear_first', False)
        self.wipe_data = kwargs.get('wipe_data', False)
        self.first_pass = True

        if self.fields is None:
            raise RuntimeError('Fields must be specified.')
        if self.method == 'upsert' and self.key_fields is None:
            raise RuntimeError('The upsert method requires primary key(s).')

    def check_format(self, filepath, file_format):
        '''Create a new local file

        Params:
            filepath: path to file where data should be saved
            file_format: format of the file

        Returns:
            filepath of the newly created resource if successful,
            ``None`` otherwise
        '''

        assert file_format == 'csv'
        if filepath[-3:].lower() != 'csv':
            raise ValueError("Why does the end of the filename not have the same extension as the file_format?")

        # How should the situation when the file already exists be handled?
        # For a CSV file, creating the file could just require outputing the header line.
        #if not os.path.exists(filepath):
        #    raise RuntimeError("{} was not created.".format(filepath))

        return filepath

    def write_or_append_to_csv(self, filename, list_of_dicts, keys):
        if not os.path.isfile(filename):
            with open(filename, 'w') as output_file:
                dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
                dict_writer.writeheader()
        with open(filename, 'a') as output_file:
            # When appending, verify that all keys are in the existing file.
            extant_keys = check_keys_in_extant_file(keys, filename)
            # Use extant_keys so that the new values go into the correct columns of the existing file.
            dict_writer = csv.DictWriter(output_file, extant_keys, extrasaction='ignore', lineterminator='\n')
            dict_writer.writerows(list_of_dicts)

    def delete_file(self, filepath):
        """Delete the file."""
        if os.path.exists(filepath):
            os.remove(filepath)

    def clear_file(self, fields, clear, first, wipe_data):
        if clear and first:
            self.delete_file(self.filepath)
        elif wipe_data: # Strictly speaking, maybe this option should delete all but the first line of a
            self.delete_file(self.filepath) # CSV file, but for implemented purposes, this is probably fine.
            print("As implemented, wipe_data is just deleting the file, rather than retaining the schema.")

    def insert(self, filepath, data, method='insert'):
        """Insert data into the file

        Params:
            filepath: path to file into which data will be inserted
            data: data to be inserted

        Returns:
            request status
        """
        assert method == 'insert' # Upserts will have to be handled if FileLoader
        # is ever modified to support SQLite output.
        ordered_list_of_fields = [f['id'] for f in self.fields] # Convert
        # CKAN-formatted field list (really a schema) to list of field names.
        self.write_or_append_to_csv(filepath, data, ordered_list_of_fields)

    def load(self, data):
        '''Load data into a local file

        Arguments:
            data: a list of records to be inserted into or upserted
                to the configured local file

        Raises:
            RuntimeError if the upsert or update metadata
                calls are unsuccessful

        Returns:
            A two-tuple of the status codes for the upsert
            and metadata update calls
        '''

        self.clear_file(self.fields, self.clear_first, self.first_pass, self.wipe_data)
        self.check_format(self.filepath, self.file_format)
        self.insert(self.filepath, data, self.method)
        self.first_pass = False
        return self.filepath
