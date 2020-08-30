from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from apiclient.discovery import build
import httplib2, requests
from collections import OrderedDict, defaultdict
from datetime import date, timedelta, datetime
import time, re
import pandas as pd

from engine.parameters.google_api_credentials import profile
#from google_api_credentials import profile

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.etl_util import Job, write_or_append_to_csv

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
    groups = fields.String(load_from='Groups'.lower(), dump_to='Groups', allow_none=True)
    package_id = fields.String(load_from='Package_ID'.lower(), dump_to='Package ID', allow_none=False)
    resource_id = fields.String(load_from='Resource_ID'.lower(), dump_to='Resource ID', allow_none=False)
    pageviews = fields.Integer(load_from='Pageviews', dump_to='Pageviews', allow_none=False)

    class Meta:
        ordered = True

class MonthlyResourceDownloadsSchema(pl.BaseSchema):
    year_month = fields.String(load_from='Year+month'.lower(), dump_to='Year+month', allow_none=False)
    package = fields.String(load_from='Package'.lower(), dump_to='Package', allow_none=False)
    resource = fields.String(load_from='Resource'.lower(), dump_to='Resource', allow_none=False)
    publisher = fields.String(load_from='Publisher'.lower(), dump_to='Publisher', allow_none=False)
    groups = fields.String(load_from='Groups'.lower(), dump_to='Groups', allow_none=True)
    package_id = fields.String(load_from='Package_ID'.lower(), dump_to='Package ID', allow_none=False)
    resource_id = fields.String(load_from='Resource_ID'.lower(), dump_to='Resource ID', allow_none=False)
    downloads = fields.Integer(load_from='Downloads', dump_to='Downloads', allow_none=False)
    unique_downloads = fields.Integer(load_from='Unique downloads', dump_to='Unique downloads', allow_none=False)

    class Meta:
        ordered = True

class MonthlyPackageDownloadsSchema(pl.BaseSchema):
    year_month = fields.String(load_from='Year+month'.lower(), dump_to='Year+month', allow_none=False)
    package = fields.String(load_from='Package'.lower(), dump_to='Package', allow_none=False)
    publisher = fields.String(load_from='Publisher'.lower(), dump_to='Publisher', allow_none=False)
    groups = fields.String(load_from='Groups'.lower(), dump_to='Groups', allow_none=True)
    package_id = fields.String(load_from='Package_ID'.lower(), dump_to='Package ID', allow_none=False)
    downloads = fields.Integer(load_from='Downloads', dump_to='Downloads', allow_none=False)
    unique_downloads = fields.Integer(load_from='Unique downloads', dump_to='Unique downloads', allow_none=False)

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
    resources, packages = [], []
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

# Begin data maniuplation functions #
def group_by_1_sum_2_ax_3(all_rows, common_fields, fields_to_sum, eliminate, metrics_name):
    # Use pandas to group by common_fields (all fields that should be in
    # common and preserved), sum the elements in the sum field, and
    # eliminate the fields in eliminate, using metrics_name as a guideline.
    df = pd.DataFrame(all_rows, columns=list(metrics_name))
    df[fields_to_sum] = df[fields_to_sum].apply(pd.to_numeric)
    grouped = df.groupby(common_fields, as_index=False)[fields_to_sum].sum()
    for e in eliminate:
        if e in list(df):
            grouped.drop(e, axis=1, inplace=True)
    return grouped

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
                for mtrc in range(0, metrics_count):
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

def insert_zeros_dicts(list_of_dicts, metric_dict_template, metrics_names, yearmonth='201510'):
    # To make the data easier to work with, ensure that every dataset/resource
    # has the same number of month entries, even if a bunch of them
    # are zero-downloads entries.
    # yearmonth is the first year/month in the dataset.
    first_year = int(yearmonth[0:4])
    first_month = int(yearmonth[4:6])
    today = date.today()
    last_year = today.year
    last_month = today.month

    new_list_of_dicts = []
    year, month = first_year, first_month
    while year <= last_year:
        if year < last_year or month <= last_month:
            current_ym = '{:d}{:02d}'.format(year,month)
            yms = [row['Year+month'] for row in list_of_dicts]
            if current_ym in yms:
                current_index = yms.index(current_ym)
                new_list_of_dicts.append(list_of_dicts[current_index])
            else:
                dict_with_zeros = dict(metric_dict_template)
                dict_with_zeros['Year+month'] = current_ym
                for metric_name in metrics_names:
                    dict_with_zeros[metric_name] = '0'
                new_list_of_dicts.append(dict_with_zeros)
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
    return new_list_of_dicts
# End data maniuplation functions #

#https://developers.google.com/analytics/devguides/reporting/core/v4/migration#v4_14
def initialize_ga_api():
    #Create service credentials
    #Rename your JSON key to client_secrets.json and save it to your working folder
    credentials = ServiceAccountCredentials.from_json_keyfile_name('analytics_credentials.json', ['https://www.googleapis.com/auth/analytics.readonly'])

    #Create a service object
    http = credentials.authorize(httplib2.Http())
    service = build('analytics', 'v4', http=http, discoveryServiceUrl=('https://analyticsreporting.googleapis.com/$discovery/rest'))
    return service

def get_metrics(service, profile, metrics, start_date='30daysAgo', end_date='today', dimensions=[], sort_by=[], filters='', page_size=100000, page_token=None):

    # start_index is no longer supported.

    # Now we have to switch to pageToken

    # Use the Analytics Service Object to query the Core Reporting API
    # for the specified metrics over the given date range.
    args_dict = { 'viewId': profile,
                'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                'metrics': metrics,
                'dimensions': dimensions,
                'orderBys': sort_by,
                'pageSize': page_size,
                }
    if page_token is not None:
        args_dict['pageToken'] = page_token

#    if sort_by != []: # This seemed like the way the original function was written, but it also seems bogus.
#        args_dict['filtersExpression'] = filters
    args_dict['filtersExpression'] = filters
    try:
        response = service.reports().batchGet(
                    body={
                        'reportRequests': [ args_dict ]
                    }
                    ).execute()
    except HttpError:
        # Retry after failure
        time.sleep(5)
        response = service.reports().batchGet(
                    body={
                        'reportRequests': [ args_dict ]
                    }
                    ).execute()

    new_rows = response.get("reports")[0].get('data', {}).get('rows', [])
#    ic| response.get("reports"): [{'columnHeader': {'dimensions': ['ga:yearMonth',
#                                                               'ga:eventLabel',
#                                                               'ga:eventCategory'],
#                                                'metricHeader': {'metricHeaderEntries': [{'name': 'ga:totalEvents',
#                                                                                          'type': 'INTEGER'},
#                                                                                         {'name': 'ga:uniqueEvents',
#                                                                                          'type': 'INTEGER'}]}},
#                               'data': {'totals': [{'values': ['0', '0']}]}}]
    #ic(new_rows)
    return response, new_rows

def page_through_get_metrics(service, profile, metrics, start_date='30daysAgo', end_date='today', dimensions=[], sort_by=[], filters='', page_size=100000):
    # Function for paging through a response and pulling out rows, but this doesn't fit how the response
    # form of get_history_by_month and also the responses are never more than 100,000 rows long.
    page_token = None
    response, rows = get_metrics(service, profile, metrics, start_date='30daysAgo', end_date='today', dimensions=dimensions, sort_by=sort_by, filters=filters, page_size=page_size, page_token=page_token)
    while response['reports'][0].get('nextPageToken', None) != None:
        next_page_token = response['reports'][0]['nextPageToken']
        ic(next_page_token)
        response, new_rows = get_metrics(service, profile, metrics, start_date='30daysAgo', end_date='today', dimensions=[], sort_by=[], filters=[], page_size=page_size, page_token=next_page_token)
        rows += new_rows
        if new_rows != []:
            assert 0 == 4
        time.sleep(0.1)

    return rows

def get_history_by_month(service, profile, metrics, resource_id=None, event=False):
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
        except HttpError:
            # retry
            time.sleep(5)
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
            # Maybe handle second failure by returning requests = None
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
        except HttpError:
            # retry
            time.sleep(5)
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
        #    requests = None
    else:
        # This is the special case for getting stats on something that
        # has an eventCategory saying it is a download stat and
        # an eventLabel identifying a particular resource by ID.
        requests, _ = get_metrics(service, profile, metrics, start_date='2015-10-15', end_date='yesterday',
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
        rows = requests.get("reports")[0].get('data', {}).get('rows', [])
        column_headers = requests.get("reports")[0].get('columnHeader', {}).get('dimensions', [])

        download_rows = [r for r in rows if 'CKAN Resource Download Request' in r.get('dimensions', [])]
        requests['reports'][0]['data']['rows'] = download_rows
#ic| requests: {'reports': [{'columnHeader': {'dimensions': ['ga:yearMonth',
#                                                            'ga:eventLabel',
#                                                            'ga:eventCategory'],
#                                             'metricHeader': {'metricHeaderEntries': [{'name': 'ga:totalEvents',
#                                                                                       'type': 'INTEGER'},
#                                                                                      {'name': 'ga:uniqueEvents',
#                                                                                       'type': 'INTEGER'}]}},
#                            'data': {'maximums': [{'values': ['1573', '653']}],
#                                     'minimums': [{'values': ['2', '1']}],
#                                     'rowCount': 42,
#                                     'rows': [{'dimensions': ['201712',
#                                                              '76fda9d0-69be-4dd5-8108-0de7907fc5a4',
#                                                              'CKAN API Request'],
#                                               'metrics': [{'values': ['549', '255']}]},
#                                              {'dimensions': ['201712',
#                                                              '76fda9d0-69be-4dd5-8108-0de7907fc5a4',
#                                                              'CKAN Resource Download '
#                                                              'Request'],

        #xi = column_headers.index('ga:eventCategory')
        #if 'data' in requests and 'rows' in requests['data']:
        #    for r in requests['data']['rows']:
        #        if 'CKAN API Request' not in r:
        #            v = requests['data']['rows']['metrics'][0]['values']
        #            requests['data']['rows']['metrics'][0]['values'] = v[:xi] + v[xi+1:]

    return requests


def fetch_and_store_metric(dmbm_file, metric, metrics_name, event, first_yearmonth, limit=0):
    # In the change from the v3 API to the v4 API, this function was switched from optionally
    # saving the data to a CKAN dataset to saving it to a local file.
    # HOWEVER, the downloads metric goes through more aggregation before being saved.
    # So, if dmbm_file is None, no file-saving is done in this function.

    # Setting a non-zero limit can be useful for testing (since polling all resources is 
    # time-consuming).

    # target_resource_id is the resource ID of the dataset that the
    # fetched information should be sent to (not to be confused with
    # the resource IDs of the data files about which metrics are being
    # obtained).
    service = initialize_ga_api()
    metrics = [{'expression': m} for m in metrics_name.keys()]

    resources, packages, lookup_by_id = get_IDs()
    extra_fields = ['Year+month']
    extra_fields += ['Package', 'Resource', 'Publisher', 'Groups', 'Package ID', 'Resource ID']

    all_rows = []
    all_dicts = []
    if limit > 0:
        resources = resources[:limit]

    for k, r_id in enumerate(resources):
        if k % 10 == 0:
            print(f"Working on resource {k}/{len(resources)}: {r_id}.")
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
            list_of_metric_dicts = []
            metric_dict_template = { 'Package': lbid['package name'],
                    'Resource': lbid['name'], 
                    'Publisher': lbid['publisher'], 
                    'Groups': lbid['groups'], 
                    'Package ID': lbid['package id'], 
                    'Resource ID': r_id}
            for row in metric_rows:
                #if metric == 'downloads':
                #    ic(row)
                #    row.remove(r_id)
                new_metric_rows.append([row['dimensions'][0], lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id] + row['metrics'][0]['values'])
                # This is the second place to add (and order) extra fields.
                new_metric_dict = dict(metric_dict_template)
                new_metric_dict['Year+month'] = row['dimensions'][0]
                for metric_name, metric in zip(metrics_name.values(), row['metrics'][0]['values']):
                    new_metric_dict[metric_name] = metric
                list_of_metric_dicts.append(new_metric_dict)
            metric_rows = new_metric_rows

            list_of_metric_dicts = insert_zeros_dicts(list_of_metric_dicts, metric_dict_template, metrics_name.values(), first_yearmonth)

            metric_rows = insert_zeros(metric_rows,
                [lbid['package name'], lbid['name'], lbid['publisher'], lbid['groups'], lbid['package id'], r_id], len(metrics_name), first_yearmonth)
                # This is the third place to add extra fields.

            csv_keys = extra_fields + list(metrics_name.values())
            write_or_append_to_csv(dmbm_file, list_of_metric_dicts, csv_keys)

            all_rows += metric_rows # Just a list of lists of values. Equivalent to [d.values for d in list_of_metric_dicts]
            all_dicts += list_of_metric_dicts # List of dicts
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
    return all_dicts, all_rows, fields

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

def pull_monthly_downloads_from_ga(jobject, **kwparameters):
    # Create entire downloads dataset by looking at every month.
    # For every resource_id in the data.json file, run metric_by_month and upsert the results to the monthly-downloads datastore.

    # One issue in trying to merge downloads and pageviews datasets is that there are a lot more resources that come up with non-zero pageviews than resources that come up with non-zero downloads (like 799 vs. like maybe 400-450).
    metric = 'downloads'
    first_yearmonth = '201603'
    event = True
    metrics_name = OrderedDict([("ga:totalEvents", 'Downloads'),
                    ("ga:uniqueEvents", 'Unique downloads')
                    ])

    limit = 0
    resource_stats_file = jobject.local_cache_filepath
    all_dicts, all_rows, fields = fetch_and_store_metric(resource_stats_file, metric, metrics_name, event, first_yearmonth, limit)
    ic(all_rows[0])
    ic(all_dicts[0])
    # Aggregate rows by package to get package stats.

    # Probably all_rows should be pushed to WPRDC Resource Downloads by Month and
    # the package-level aggregation below populates a separate table:

    # Write to both files: package_downloads + resource_downloads


    # This could all be a custom_processing function for the monthly_package_downloads job.
    # AND that version could check the freshness of the resource_downloads CSV file and redownload 
    # if necessary.

    package_stats_file = re.sub('resource', 'package', resource_stats_file)
    assert resource_stats_file != package_stats_file

    common_fields = ['Year+month', 'Package', 'Publisher', 'Groups', 'Package ID']
    fields_to_sum = ['Downloads', 'Unique downloads']
    # It would probably not be hard to rewrite this to avoid using pandas.

    df = group_by_1_sum_2_ax_3(all_rows, common_fields, fields_to_sum, [], fields)
    df = df.sort_values(by=['Package ID', 'Year+month'], ascending=[True, True])
    df = df.reset_index(drop=True) # Eliminate row numbers.
    keys = ['Package ID', 'Year+month']
    all_fields = common_fields + fields_to_sum
    ##resource_id = 'd72725b1-f163-4378-9771-14ce9dad3002' # This is just
    ### a temporary resource ID.
    ##push_df_to_ckan(df, "Live", resource_id, field_mapper, all_fields, keys)

    df.to_csv(package_stats_file, sep=',', line_terminator='\n', encoding='utf-8', index=False)

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
    {
        'job_code': 'monthly_downloads',
        'source_type': 'local',
        'source_dir': '',
        'source_file': f'resource_downloads_by_month.csv',
        'custom_processing': pull_monthly_downloads_from_ga,
        'encoding': 'utf-8-sig',
        'schema': MonthlyResourceDownloadsSchema,
        'primary_key_fields': ['Year+month', 'Resource ID'], # Why isn't this by Resource ID?
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'resource_downloads_by_month.csv', # purposes.
        'package': analytics_package_id, # What is the correct package ID for dataset downloads (instead of resource downloads)?
        'resource_name': f'WPRDC Resource Downloads by Month' # Where does this actually come from?
    },
    {
        'job_code': 'monthly_package_downloads',
        'source_type': 'local',
        'source_dir': '',
        'source_file': f'package_downloads_by_month.csv',
        'encoding': 'utf-8-sig',
        'schema': MonthlyPackageDownloadsSchema,
        'primary_key_fields': ['Year+month', 'Package ID'], # Why isn't this by Resource ID?
        'always_wipe_data': False,
        'upload_method': 'upsert',
        'destinations': ['file'], # These lines are just for testing
        'destination_file': f'package_downloads_by_month.csv', # purposes.
        'package': '2bdef6b1-bf31-4d20-93e7-1aa3920d2c52',
        'resource_name': f'WPRDC Package Downloads by Month'
    },
]
