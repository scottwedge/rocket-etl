import requests
import json
import datetime

from engine.wprdc_etl.pipeline.exceptions import CKANException

from pprint import pprint

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
        resource_id = next((i['id'] for i in response_json['result']['resources'] if resource_name == i['name']), None)
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
            raise CKANException('An error occured: {}'.format(create_datastore['error']['name'][0]))

        return create_datastore['result']['resource_id']


    def generate_datastore(self, fields, clear, first):
        if clear and first:
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
        return upsert.status_code

    def update_metadata(self, resource_id):
        """Update a resource's metadata

        TODO: Make this versatile

        Params:
            resource_id: ID of the resource for which the metadata will be modified

        Returns:
            request status
        """
        update = requests.post(
            self.ckan_url + 'action/resource_patch',
            headers={
                'content-type': 'application/json',
                'authorization': self.key
            },
            data=json.dumps({
                'id': resource_id,
                'url': self.dump_url + str(resource_id),
                'url_type': 'datapusher',
                'last_modified': datetime.datetime.now().isoformat(),
            })
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

        Raises:
            RuntimeError if fields is not specified or method is
            ``upsert`` and no ``key_fields`` are passed.
        '''
        super(CKANDatastoreLoader, self).__init__(*args, **kwargs)
        self.fields = kwargs.get('fields', None)
        self.key_fields = kwargs.get('key_fields', None)
        self.indexes = kwargs.get('indexes', False)
        self.method = kwargs.get('method', 'upsert')
        self.header_fix = kwargs.get('header_fix', None)
        self.clear_first = kwargs.get('clear_first', False)
        self.first_pass = True

        if self.fields is None:
            raise RuntimeError('Fields must be specified.')
        if self.method == 'upsert' and self.key_fields is None:
            raise RuntimeError('Upsert method requires primary key(s).')
        if self.clear_first and not self.resource_id:
            raise RuntimeError('Resource must already exist in order to be cleared.')

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
        self.generate_datastore(self.fields, self.clear_first, self.first_pass)
        self.first_pass = False
        upsert_status = self.upsert(self.resource_id, data, self.method)
        update_status = self.update_metadata(self.resource_id)

        if upsert_status == 409:
            print("dir(self) = {}".format(dir(self)))
            pprint(self.fields)
            print("key_fields = {}".format(self.key_fields))
            if indexes is not None:
                print("indexes = {}".format(indexes))
            raise RuntimeError('Upsert failed with status code {}. This may be because of a conflict between datastore fields/keys and specified primary keys. Or maybe you are trying to insert a row into a resource with an existing row with the same primary key or keys.'.format(str(upsert_status)))

        if str(upsert_status)[0] in ['4', '5']:
            raise RuntimeError('Upsert failed with status code {}.'.format(str(upsert_status)))
        elif str(update_status)[0] in ['4', '5']:
            raise RuntimeError('Metadata update failed with status code {}'.format(str(update_status)))
        else:
            return upsert_status, update_status
