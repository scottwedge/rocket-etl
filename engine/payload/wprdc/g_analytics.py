from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import httplib2
from collections import OrderedDict

from engine.parameters.google_api_credentials import profile
#from google_api_credentials import profile

from marshmallow import fields, pre_load, post_load
from engine.wprdc_etl import pipeline as pl
from engine.notify import send_to_slack
from engine.etl_util import Job

from pprint import pprint
from icecream import ic

class WebStatsSchema(pl.BaseSchema):
    year_month = fields.String(load_from='Year+month'.lower(), dump_to='Year+month', allow_none=False)
    users = fields.Integer(load_from='Users', dump_to='Users', allow_none=False)
    sessions = fields.Integer(load_from='Sessions', dump_to='Sessions', allow_none=False)
    pageviews= fields.Integer(load_from='Pageviews', dump_to='Pageviews', allow_none=False)
    views_per_session = fields.Float(load_from='Pageviews_per_session'.lower(), dump_to='Pageviews per session', allow_none=False)
    average_session_duration = fields.Float(load_from='Average_session_duration_(seconds)'.lower(), dump_to='Average session duration (seconds)', allow_none=False)

    class Meta:
        ordered = True

    @pre_load
    def check(self, data):
        ic(data)

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

def write_to_csv(filename,list_of_dicts,keys):
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
        ic(stats_by_month)
        if stats_by_month is None:
            stats_by_month = get_history_by_month(service, profile, metrics)
        if stats_by_month is None:
            raise Exception("Unable to get stats_by_month data after trying twice")
        print(stats_by_month['reports'][0].keys())
        stats_rows = stats_by_month['reports'][0]['data']['rows']
        pprint(stats_by_month['reports'][0]['data']['rows'])

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
]
