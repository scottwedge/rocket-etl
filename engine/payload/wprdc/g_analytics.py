from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2, requests
from collections import OrderedDict, defaultdict
from datetime import date, timedelta, datetime
import time

from engine.parameters.google_api_credentials import profile
#from google_api_credentials import profile

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.etl_util import Job

from pprint import pprint
from icecream import ic

tracking_resource_id = None # There was originally a resource ID value
# here, but I toggled it to None to switch off stuff that I decided to
# stop doing in get_IDs. That stuff could probably be deleted at this
# point.

class WebStatsSchema(pl.BaseSchema):
    year_month = fields.String(load_from='Year+month'.lower(), dump_to='Year+month', allow_none=False)
    users = fields.Integer(load_from='Users', dump_to='Users', allow_none=False)
    sessions = fields.Integer(load_from='Sessions', dump_to='Sessions', allow_none=False)
    pageviews= fields.Integer(load_from='Pageviews', dump_to='Pageviews', allow_none=False)
    views_per_session = fields.Float(load_from='Pageviews_per_session'.lower(), dump_to='Pageviews per session', allow_none=False)
    average_session_duration = fields.Float(load_from='Average_session_duration_(seconds)'.lower(), dump_to='Average session duration (seconds)', allow_none=False)

    class Meta:
        ordered = True

class MonthlyPageviewsSchema(pl.BaseSchema):
    year_month = fields.String(load_from='Year+month'.lower(), dump_to='Year+month', allow_none=False)
    package = fields.String(load_from='Package'.lower(), dump_to='Package', allow_none=False)
    resource = fields.String(load_from='Resource'.lower(), dump_to='Resource', allow_none=False)
    publisher = fields.String(load_from='Publisher'.lower(), dump_to='Publisher', allow_none=False)
    groups = fields.String(load_from='Groups'.lower(), dump_to='Groups', allow_none=False)
    package_id = fields.String(load_from='Package_ID'.lower(), dump_to='Package ID', allow_none=False)
    resource_id = fields.String(load_from='Resource_ID'.lower(), dump_to='Resource ID', allow_none=False)
    pageviews = fields.Integer(load_from='Pageviews', dump_to='Pageviews', allow_none=False)

    class Meta:
        ordered = True

#    @pre_load
#    def check(self, data):
#        ic(data)

def stringify_groups(p):
    groups_string = ''
    if 'groups' in p:
        groups = p['groups']
        groups_string = '|'.join(set([g['title'] for g in groups]))
    return groups_string


# Begin CKAN functions #
def get_IDs():
    # This function originally just got resource IDs (and other parameters) from the
    # current_package_list_with_resources API endpoint. However, this ignores
    # resources that existed but have been deleted (or turned private again). To
    # track the statistics of these as well. We are now merging in historical
    # resource IDs produced by dataset-tracker.
    from engine.credentials import site
    ckan_max_results = 9999999
    resources, packages, = [], []
    lookup_by_id = defaultdict(lambda: defaultdict(str))
    url = "{}/api/3/action/current_package_list_with_resources?limit={}".format(site, ckan_max_results)
    r = requests.get(url)
    # Traverse package list to get resource IDs.
    package_list = r.json()['result']
    for p in package_list:
        r_list = p['resources']
        if len(r_list) > 0:
            packages.append(r_list[0]['package_id'])
            for k,resource in enumerate(r_list):
                if 'id' in resource:
                    resources.append(resource['id'])
                    lookup_by_id[resource['id']]['package id'] = r_list[0]['package_id']
                    lookup_by_id[resource['id']]['package name'] = p['title']
                    lookup_by_id[resource['id']]['publisher'] = p['organization']['title']
                    lookup_by_id[resource['id']]['groups'] = stringify_groups(p)
                if 'name' in resource:
                    lookup_by_id[resource['id']]['name'] = resource['name']
#                else:
#                    lookup_by_id[resource['id']]['name'] = 'Unnamed resource'

    if tracking_resource_id is not None:
        tracks = load_resource(site, tracking_resource_id, None)
        for r in tracks:
            r_id = r['resource_id']
            if r_id not in resources:
                if 'name' in resource:
                    lookup_by_id[resource['id']]['name'] = resource['name']
                    print("Adding resource ID {} ({})".format(r_id,r['resource_name']))
                else:
                    print("Adding resource ID {} (Unnamed resource)".format(r_id))
                resources.append(r_id)
                if r['package_id'] not in packages:
                    packages.append(r['package_id'])
                lookup_by_id[r_id]['package_id'] = r['package_id']
                lookup_by_id[r_id]['package name'] = r['package_name']
                lookup_by_id[r_id]['publisher'] = r['organization']
                lookup_by_id[r_id]['groups'] = stringify_groups(p)

    return resources, packages, lookup_by_id

# End CKAN functions #

def insert_zeros(rows, extra_columns, metrics_count, yearmonth='201510'):
    # To make the data easier to work with, ensure that every dataset
    # has the same number of month entries, even if a bunch of them
    # are zero-downloads entries.
    # yearmonth is the first year/month in the dataset.
    first_year = int(yearmonth[0:4])
    first_month = int(yearmonth[4:6])
    today = date.today()
    last_year = today.year
    last_month = today.month

    new_rows = []
    year, month = first_year, first_month
    while year <= last_year:
        if year < last_year or month <= last_month:
            current_ym = '{:d}{:02d}'.format(year,month)
            yms = [row[0] for row in rows]
            if current_ym in yms:
                current_index = yms.index(current_ym)
                new_rows.append(rows[current_index])
            else:
                row_of_zeros = [current_ym] + extra_columns
                base_length = len(row_of_zeros)
                for mtrc in range(0,metrics_count):
                    row_of_zeros.append('0')
                new_rows.append(row_of_zeros)
        month += 1
        if month == 13:
            year += 1
            month = 1
        if month == last_month and year == last_year and today.day == 1:
            year += 1 # Abort loop since Google Analytics has no data on
            # the first of the month. This is a bit of a kluge, but it works.
            # I am leaving this in to make the monthly-downloads sparklines
            # consistent with the behavior of the Users plot on the front
            # page of the dashboard.
    return new_rows


#https://developers.google.com/analytics/devguides/reporting/core/v4/migration#v4_14
def initialize_ga_api():
    #Create service credentials
    #Rename your JSON key to client_secrets.json and save it to your working folder
    credentials = ServiceAccountCredentials.from_json_keyfile_name('analytics_credentials.json', ['https://www.googleapis.com/auth/analytics.readonly'])

    #Create a service object
    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http, discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
    return service

def get_metrics(service, profile_id, metrics, start_date='30daysAgo', end_date='today', dimensions=[], sort_by=[], filters=[], max_results=10000, start_index=1):
  # Use the Analytics Service Object to query the Core Reporting API
  # for the specified metrics over the given date range.
  if sort_by == []:
        requests = service.reports().batchGet(
                body={
                    'reportRequests': [
                        {
                            'viewId': profile,
                            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                            'metrics': metrics,
                            'dimensions': dimensions,
                            'orderBys': sort_by,
                            'pageSize': max_results,
                            'pageToken': start_index-1,
                        }]
                }
                ).execute()
        return requests
  else:
        requests = service.reports().batchGet(
                body={
                    'reportRequests': [
                        {
                            'viewId': profile,
                            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                            'metrics': metrics,
                            #'dimensions': dimensions,
                            #'orderBys': sort_by,
                            'filtersExpression': filters,
                            'pageSize': max_results,
                            'pageToken': start_index-1,
                        }]
                }
                ).execute()
        return requests

def get_history_by_month(service,profile,metrics,resource_id=None,event=False):
    # If the user doesn't specify the resource ID (or the event flag is
    # set to False), download monthly stats. Otherwise, treat it as one
    # of those special "eventLabel/eventCategory" things.
    if resource_id is None:
        try:
            requests = service.reports().batchGet(
                    body={
                        'reportRequests': [
                            {
                                'viewId': profile,
                                'dateRanges': [{'startDate': '2015-10-15', 'endDate': 'today'}],
                                'metrics': metrics,
                                'dimensions': [{"name": "ga:yearMonth"}],
                                'orderBys': [{"fieldName": "ga:yearMonth", "sortOrder": "ASCENDING"}],
                                'pageSize': 10000
                            }]
                    }
                    ).execute()
        except:
            requests = None
    elif not event:
        # This plucks out the entire history for a parameter like pageviews
        # for a particular resource ID.
        try:
            requests = service.reports().batchGet(
                    body={
                        'reportRequests': [
                            {
                                'viewId': profile,
                                'dateRanges': [{'startDate': '2015-10-15', 'endDate': 'today'}],
                                'metrics': metrics,
                                'filtersExpression': "ga:pagePath=~^/dataset/.*/resource/" + resource_id + "$;ga:pagePath!~^/dataset/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                                # This complex filter has been confirmed for one dataset
                                # to get the exact number of pageviews as generated by the
                                # R dashboard code.

                                # To AND filters together, separate them with a semicolon.
                                # To OR filters together, separate them with a comma.
                                #filters="ga:pagePath=~/dataset/.*/resource/40776043-ad00-40f5-9dc8-1fde865ff571$",#works
                                # But also catches things like
                                # /terms-of-use?came_from=/dataset/city-facilities-centroids/resource/9a5a52fd-fbe5-45b3-b6b2-c3bdaf6a2e04
                                #filters="ga:pagePath=~^/dataset/.*/resource/9a5a52fd-fbe5-45b3-b6b2-c3bdaf6a2e04$", # actually works, although some weird isolated
                                # hits like these get through:
                                # /dataset/a8f7a1c2-7d4d-4daa-bc30-b866855f0419/resource/40776043-ad00-40f5-9dc8-1fde865ff571
                                'dimensions': [{"name": "ga:yearMonth"}],
                                'orderBys': [{"fieldName": "ga:yearMonth", "sortOrder": "ASCENDING"}],
                                'pageSize': 10000
                            }]
                    }
                    ).execute()
        except:
            requests = None
    else:
        # This is the special case for getting stats on something that
        # has an eventCategory saying it is a download stat and
        # an eventLabel identifying a particular resource by ID.
        try:
            requests = get_metrics(service, profile, metrics, start_date='2015-10-15', end_date='yesterday',
                    dimensions=[{"name": "ga:yearMonth"}, {"name": "ga:eventLabel"}, {"name": "ga:eventCategory"}],
                    sort_by=[{"fieldName": "ga:yearMonth", "sortOrder": "ASCENDING"}],
                    filters=f"ga:eventLabel=={resource_id}")
            #filters="ga:eventCategory==CKAN%20Resource%20Download%20Request;ga:eventLabel=="+resource_id)
                # The double filter just doesn't work for some reason when the two filters are ANDed together.
                # Therefore I have devised the kluge in the else
                # statement below.
            # Using the following form seems to make no difference
            # (confirmed in GA Query Explorer):
            #filters="ga:eventCategory==CKAN%20Resource%20Download%20Request;ga:eventLabel=~^"+resource_id+"$")
        except:
            requests = None
        else:
            chs = requests['columnHeaders']['dimensions']
            #xi = ['ga:eventCategory' in c.values() for c in chs].index(True)
            xi = chs.index('ga:eventCategory')

            #if 'data' in requests and 'rows' in requests['data']:
            #    requests['data']['rows'] = [r[:xi]+r[xi+1:] for r in requests['data']['rows'] if 'CKAN API Request' not in r]
            if 'data' in requests and 'rows' in requests['data']:
                for r in requests['data']['rows']:
                    if 'CKAN API Request' not in r:
                        v = requests['data']['rows']['metrics'][0]['values']
                        requests['data']['rows']['metrics'][0]['values'] = v[:xi] + v[xi+1:]

           #'rows': [{'dimensions': ['201510'],
           #          'metrics': [{'values': ['1515',
           #                                  '2160',
           #                                  '21222',
           #                                  '9.825',
           #                                  '306.36296296296297']}]},
           #         {'dimensions': ['201511'],
           #          'metrics': [{'values': ['815',
           #                                  '1358',
           #                                  '13884',
           #                                  '10.223858615611192',
           #                                  '333.00662739322536']}]},

#[{'columnHeader': {'dimensions': ['ga:yearMonth'],
#'metricHeader': {'metricHeaderEntries': [{'name': 'ga:users',
#             'type': 'INTEGER'},
#            {'name': 'ga:sessions',
#             'type': 'INTEGER'},
#            {'name': 'ga:pageviews',
#             'type': 'INTEGER'},
#            {'name': 'ga:pageviewsPerSession',
#             'type': 'FLOAT'},
#            {'name': 'ga:avgSessionDuration',
#             'type': 'TIME'}]}},

    return requests


def fetch_and_store_metric(dmbm_file, metric, metrics_name, event, first_yearmonth, limit=0):
    # target_resource_id is the resource ID of the dataset that the
    # fetched information should be sent to (not to be confused with
    # the resource IDs of the data files about which metrics are being
    # obtained).
    service = initialize_ga_api()
    metrics = [{'expression': m} for m in metrics_name.keys()]

    resources, packages, lookup_by_id = get_IDs()

    #Write the field names as the first line of the file:
    fcsv = open(dmbm_file, 'w')
    extra_fields = ['Year+month']
    extra_fields += ['Package', 'Resource', 'Publisher', 'Groups', 'Package ID', 'Resource ID']
        # This is the first place to add extra fields.
    #if metric == 'downloads':
    #    extra_fields.remove("Resource ID") # This causes an error.
    csv_row = ','.join(extra_fields + list(metrics_name.values()))
    fcsv.write(csv_row+'\n')

    all_rows = []
    if limit > 0:
        resources = resources[:limit]
    for k, r_id in enumerate(resources):
        metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 1. ")
            metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 2. ")
            time.sleep(1)
            metric_by_month = get_history_by_month(service, profile, metrics, r_id, event)
        if metric_by_month is None:
            print("Strike 3. ")
            raise Exception(f"Unable to get metric_by_month data for resource ID {r_id} after trying thrice.")


        if 'reports' in metric_by_month and len(metric_by_month['reports']) > 0 and 'rows' in metric_by_month['reports'][0]['data']:
            metric_rows = metric_by_month['reports'][0]['data']['rows']
            # Unfortunately, Google Analytics does not provide the resource
            # ID as a queryable parameter for pageviews or other metrics
            # the way it does for downloads (since the resource ID has been
            # inserted as the eventLabel).
            # Therefore, I need to manually insert the resource ID in these
            # cases (and now other parameters).

            lbid = lookup_by_id[r_id]
            new_metric_rows = []
            for row in metric_rows:
                if metric == 'downloads':
                    row.remove(unicode(r_id))
                new_metric_rows.append([row['dimensions'][0], lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id] + row['metrics'][0]['values'])
                # This is the second place to add (and order) extra fields.
            metric_rows = new_metric_rows

            metric_rows = insert_zeros(metric_rows,
                [lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id], len(metrics_name), first_yearmonth)
                # This is the third place to add extra fields.

            for row in metric_rows:
                csv_row = ','.join(row)
                fcsv = open(dmbm_file, 'a')
                fcsv.write(csv_row+'\n')
                fcsv.close()

            all_rows += metric_rows
        else:
            print(f"No rows found in the response for the dataset with resource ID {r_id}")
        time.sleep(1)

# Create an update to the dataset-downloads dataset by just looking at this month and last month and upserting the results.
#    if modify_datastore:
#        # The fourth and final place to add extra fields is field_mapper,
#        # which is now defined in pull_web_stats_from_ga, but you can also
#        # just extend it here with a command like
#        #       field_mapper['Beeblebrox'] = "dict"
#        keys = ['Year+month', 'Resource ID']
#        push_dataset_to_ckan(all_rows, metrics_name, server, target_resource_id, field_mapper, keys, extra_fields) #This pushes everything in metric_rows

    fields = extra_fields + list(metrics_name.values())
    return all_rows, fields

    # [ ] Modify push_dataset_to_ckan to only initialize the datastore when necessary.
        # This script could have two modes:
        #   1) Download all data and overwrite the old stuff.
        #   2) Download only this month and last month and upsert into
        #   the existing repository.



#    get_full_history(resource_id="40776043-ad00-40f5-9dc8-1fde865ff571")
# Pull down daily downloads/unique downloads/pageviews and then monthly
# stats, and then use the data.json file to filter down to the things
# we want to track (maybe).

# If we dump the output
# u'rows': [[u'40776043-ad00-40f5-9dc8-1fde865ff571', u'668', u'260'],
#       [u'7a417847-37bb-4a16-a25e-477f2a71661d', u'493', u'82'],
#       [u'c0fcc09a-7ddc-4f79-a4c1-9542301ef9dd', u'139', u'78'],
#
# and prepend some kind of date information, that could essentially be the
# stuff inserted into the dataset (once the types have been properly taken care of)


# These two functions just illustrate the migration from v3 to v4:
def download_monthly_stats_v3():
    requests = service.data().ga().get(
    ids='ga:' + profile,
    start_date='2015-10-15',
    end_date='yesterday',
    dimensions="ga:yearMonth",
    sort="ga:yearMonth",
    metrics=metrics).execute()

    return requests

def download_monthly_stats_v4(service, profile, metrics, resource_id=None, event=False):
    requests = service.reports().batchGet( body={
                'reportRequests': [
                    {
                        'viewId': profile,
                        'dateRanges': [{'startDate': '2015-10-15', 'endDate': 'today'}],
                        'metrics': metrics,
                        'dimensions': [{"name": "ga:yearMonth"}],
        #                "filtersExpression":f"ga:pagePath={regex}",
                        'orderBys': [{"fieldName": "ga:yearMonth", "sortOrder": "DESCENDING"}],
                        'pageSize': 10000
                    }]
            }
            ).execute()

    return requests
# END functions for accessing Google Analytics #

def write_to_csv(filename, list_of_dicts, keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def pull_web_stats_from_ga(jobject, **kwparameters):
    if not kwparameters['use_local_files']:
        service = initialize_ga_api()

        metrics_name = OrderedDict([("ga:users",'Users'),
                        ("ga:sessions",'Sessions'),
                        ("ga:pageviews",'Pageviews'),
                        ("ga:pageviewsPerSession",'Pageviews per session'),
                        ("ga:avgSessionDuration",'Average session duration (seconds)')
                        ])
        metrics = [{'expression': m} for m in metrics_name.keys()]

        stats_by_month = get_history_by_month(service, profile, metrics)
        if stats_by_month is None:
            stats_by_month = get_history_by_month(service, profile, metrics)
        if stats_by_month is None:
            raise Exception("Unable to get stats_by_month data after trying twice")
        stats_rows = stats_by_month['reports'][0]['data']['rows']

        site_stats_file = jobject.local_cache_filepath
        #Write the field names as the first line of the file:
        fcsv = open(site_stats_file, 'w')
        csv_row = ','.join(['Year+month'] + list(metrics_name.values()))
        fcsv.write(csv_row + '\n')

        for row in stats_rows:
            csv_row = ','.join(row['dimensions'] + row['metrics'][0]['values'])
            fcsv = open(site_stats_file, 'a')
            fcsv.write(csv_row + '\n')
            fcsv.close()
        #time.sleep(0.5)

def pull_monthly_pageviews_from_ga(jobject, **kwparameters):
    metric = 'pageviews'
    first_yearmonth = '201510'
    event = False
    metrics_name = OrderedDict([("ga:pageviews",'Pageviews')])

    limit = 0
    fetch_and_store_metric(jobject.local_cache_filepath, metric, metrics_name, event, first_yearmonth, limit)

if __name__ == '__main__':
    pull_web_stats_from_ga(Job())

analytics_package_id = 'b114c5e3-6e59-448a-b160-ed71f55b94d1' # Production version of WPRDC statistics data package

job_dicts = [
    {
        'job_code': 'web_stats',
        'source_type': 'local',
        'source_dir': '',
        'source_file': f'site_stats_by_month.csv',
        'custom_processing': pull_web_stats_from_ga,
        'encoding': 'utf-8-sig',
        'schema': WebStatsSchema,
        'primary_key_fields': ['Year+month'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'web_stats.csv', # purposes.
        'package': analytics_package_id,
        'resource_name': f'WPRDC Site Statistics by Month'
    },
    {
        'job_code': 'monthly_pageviews',
        'source_type': 'local',
        'source_dir': '',
        'source_file': f'dataset_pageviews_by_month.csv',
        'custom_processing': pull_monthly_pageviews_from_ga,
        'encoding': 'utf-8-sig',
        'schema': MonthlyPageviewsSchema,
        'primary_key_fields': ['Year+month', 'Resource ID'],
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'dataset_pageviews_by_month.csv', # purposes.
        'package': analytics_package_id,
        'resource_name': f'WPRDC Resource Pageviews by Month'
    },
]
