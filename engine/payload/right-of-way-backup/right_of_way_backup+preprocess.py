from marshmallow import fields, pre_load, post_load

import requests
from engine.wprdc_etl import pipeline as pl
from engine.credentials import site, API_key
from engine.etl_util import fetch_city_file, query_resource, Job
from engine.notify import send_to_slack


from datetime import datetime
from icecream import ic

#import psycopg2

GEO_FIELDS = ['neighborhood', 'council_district', 'ward', 'tract', 'public_works_division',
              # 'pli_division', 'police_zone', 'fire_zone',
              'latitude', 'longitude']

CHECK_QUERY = "select name from allegheny_county_municipal_boundaries " \
              "where ST_Intersects(geom, ST_SetSRID(ST_Point({lon}, {lat}), 4326));"

# Configure geocoder
#conn = psycopg2.connect(dbname="streets", user="steven", password="root")
#cur = conn.cursor()


def rev_geocode(lon, lat):
    if lat and lon:
        r = requests.get(
            'http://tools.wprdc.org/geo/reverse_geocode/', params={'lat': lat, 'lng': lon})
        if r.status_code == 200:
            j = r.json()

            return j['results']
        else:
            return None


class DataDictSchema(pl.BaseSchema):
    id = fields.String()
    type = fields.String()

    class Meta:
        ordered = True


class PghGeoSchema(pl.BaseSchema):
    neighborhood = fields.String(allow_none=True)
    council_district = fields.String(allow_none=True)
    ward = fields.String(allow_none=True)
    tract = fields.String(allow_none=True)
    public_works_division = fields.String(allow_none=True)
    # pli_division = fields.String(allow_none=True)
    # police_zone = fields.String(allow_none=True)
    # fire_zone = fields.String(allow_none=True)

    latitude = fields.Float(dump_to='address_lat', allow_none=True)
    longitude = fields.Float(dump_to='address_lon', allow_none=True)

    @pre_load
    def reverse_geocode(self, data):
        if 'NA' in (data['longitude'], data['latitude']):
            data['longitude'], data['latitude'] = None, None

        geo_data = rev_geocode(data['longitude'], data['latitude'])
        if geo_data:
            if 'pittsburgh_neighborhood' in geo_data:
                data['neighborhood'] = geo_data['pittsburgh_neighborhood']['name']

            if 'pittsburgh_city_council' in geo_data:
                data['council_district'] = geo_data['pittsburgh_city_council']['name']

            if 'pittsburgh_ward' in geo_data:
                data['ward'] = geo_data['pittsburgh_ward']['name']

            if 'us_census_tract' in geo_data:
                data['tract'] = geo_data['us_census_tract']['name']

            if 'pittsburgh_dpw_division' in geo_data:
                data['public_works_division'] = geo_data['pittsburgh_dpw_division']['name']

            if 'pittsburgh_ward' in geo_data:
                data['pli_division'] = geo_data['pittsburgh_ward']['name']

            if 'pittsburgh_police_zone' in geo_data:
                data['police_zone'] = geo_data['pittsburgh_police_zone']['name']

            if 'pittsburgh_fire_zone' in geo_data:
                data['fire_zone'] = geo_data['pittsburgh_fire_zone']['name']


class RightOfWaySchema(PghGeoSchema):
    display = fields.String(load_from='display', dump_to='id')
    sequence = fields.Integer()
    type = fields.String(allow_none=True)
    open_date = fields.Date(allow_none=True)
    from_date = fields.Date(allow_none=True)
    to_date = fields.Date(allow_none=True)
    restoration_date = fields.Date(allow_none=True)
    description = fields.String(allow_none=True)
    full_address = fields.String(dump_to="address", allow_none=True)
    location = fields.String(dump_to="street_or_location", allow_none=True)
    from_street = fields.String(allow_none=True)
    to_street = fields.String(allow_none=True)
    business_name = fields.String(allow_none=True)
    license_type = fields.String(allow_none=True)
    from_lat = fields.Float(allow_none=True)
    from_lon = fields.Float(allow_none=True)
    to_lat = fields.Float(allow_none=True)
    to_lon = fields.Float(allow_none=True)

    class Meta:
        ordered = True
        fields = ['display', 'sequence', 'type', 'open_date', 'from_date', 'to_date', 'restoration_date', 'description',
                  'full_address', 'location', 'from_street', 'to_street', 'business_name', 'license_type'] \
            + GEO_FIELDS \
            + ['from_lat', 'from_lon', 'to_lat', 'to_lon']

    @pre_load
    def fix_dates(self, data):
        for field in ['open_date', 'from_date', 'to_date', 'restoration_date']:
            if not data[field] or 'NA' in data[field]:
                data[field] = None
            else:
                data[field] = datetime.strptime(
                    data[field], '%Y-%m-%d').date().isoformat()

    @pre_load
    def remove_bad_coords(self, data):
        for lon, lat in [('from_lon', 'from_lat'), ('to_lon', 'to_lat'), ('longitude', 'latitude')]:
            if data[lon] and data[lat]:
                if data[lon] == 'NA' or data[lat] == 'NA':
                    data[lon], data[lat] = None, None
                    continue
                #cur.execute(CHECK_QUERY.format(lon=data[lon], lat=data[lat])) # Removing this since the database is not set up on this particular machine.
                # [ ] However, one question is what will be the effect of not checking these geocoordinates, as this affects the base latitude/longitude 
                # data as well.
                #result = cur.fetchone()
                #if not result or result[0] != 'PITTSBURGH':
                #    print('outta the city')
                #    data[lon], data[lat] = None, None


package_id = "23482953-50cc-4370-858c-eb0c034b8157"
#package_id = "812527ad-befc-4214-a4d3-e621d8230563" # Test package on data.wprdc.org

if package_id == "812527ad-befc-4214-a4d3-e621d8230563":
    print("Using the test package.")

#resource_name = "Right of Way Permits"

#target = 'row.csv'
#target = '/home/daw165/rocket-etl/output_files/right_of_way_backup/backup_right_of_way.csv'

#row_pipeline = pl.Pipeline('row_pipeline', 'Right of Way Permits Pipeline',
#                           log_status=False, chunk_size=2000) \
#    .connect(pl.FileConnector, target, encoding='latin-1') \
#    .extract(pl.CSVExtractor, firstline_headers=True) \
#    .schema(RightOfWaySchema) \
#    .load(pl.CKANDatastoreLoader, 'production',
#          fields=RightOfWaySchema().serialize_to_ckan_fields(capitalize=False),
#          package_id=package_id,
#          key_fields=['id', 'sequence', 'type'],
#          resource_name=resource_name,
#          method='upsert').run()

def conditionally_get_city_files(job, **kwparameters):
    if not kwparameters['use_local_files']:
        fetch_city_file(job)

def preprocess(job, **kwparameters):
    import datetime, usaddress, re, csv, copy

    get_file_job = Job({
        'source_type': 'sftp', # This parameter is currently unneeded by fetch_city_file.
        'source_dir': '',
        'source_file': 'right_of_way_permits.csv',
        'job_directory': 'right_of_way_backup', # This parameter is usually auto-added
        # in launchpad.py.
    })
    fetch_city_file(get_file_job) # This is the first time a little joblet was created
    # to handle the file-fetching in the initial custom-processing script.
    # Eventually the manner for doing this stuff should be standardized...
    # Other scripts have broken such a mutli-step process into two processes at the
    # job_dict level, which is another option and is perhaps a little easier for 
    # the reader to follow. It would also route around the INPUT_FILE/OUTPUT_FILE stuff
    # below. Really this alternate approach is a result of taking two existing scripts
    # and combining them into one rocket-etl-style script in a way that minimized 
    # code changes and possible unanticipated bugs.

    print("Let the preprocessing begin...")
    INPUT_FILE = '/home/daw165/rocket-etl/source_files/right_of_way_backup/right_of_way_permits.csv'
    OUTPUT_FILE = '/home/daw165/rocket-etl/source_files/right_of_way_backup/right_of_way_transmuted_backup.csv'
    INPUT_HEADING = [
        "id", 'display', "type.display", "openDate", "Date.From", "Date.To", "asis.Restoration.cDate", "description",
        "loc.address_full", "asis.From", "asis.From.c2", "asis.From.c3", "asis.From.c4", "asis.From.c5",
        "asis.From.c1", "asis.To", "asis.To.c2", "asis.To.c3", "asis.To.c4", "asis.To.c5", "asis.To.c1",
        "asis.Location", "asis.Location.c2", "asis.Location.c3", "asis.Location.c4", "asis.Location.c5",
        "asis.Location.c1", "prof.businessName", "prof.licenseType.value", "geo.x", "geo.y"
    ]

    OUTPUT_HEADING = [
        'display', "sequence", "type", "open_date", "from_date", "to_date", "restoration_date", "description",
        "full_address",
        "location", "from_street", "to_street",
        "business_name", "license_type", "latitude", "longitude", "from_lat", "from_lon", "to_lat", "to_lon"
    ]

    ROW_MAPPING = {
        'display': 'display',
        'type': 'type.display',
        'open_date': 'openDate',
        'from_date': 'Date.From',
        'to_date': 'Date.To',
        'restoration_date': 'asis.Restoration.cDate',
        'description': 'description',
        'full_address': 'loc.address_full',
        # 'location': '',
        # 'from': '',
        # 'to': '',
        'business_name': 'prof.businessName',
        'license_type': 'prof.licenseType.value',
        'latitude': 'geo.y',
        'longitude': 'geo.x'
    }

    def clean_street(addr):
        try:
            parts = usaddress.tag(addr)[0]
            pre = parts.get('StreetNamePreDirectional', '')
            name = parts.get('StreetName', '')
            post = parts.get('StreetNamePostType', '')
            return re.sub(r'\s+', ' ', '{} {} {}'.format(pre, name, post)).lstrip().rstrip()
        except:
            print(addr)

    input_f = open(INPUT_FILE)
    output_f = open(OUTPUT_FILE, 'w')

    reader = csv.DictReader(input_f)
    writer = csv.DictWriter(output_f, OUTPUT_HEADING)

    writer.writeheader()

    resource_id = "cc17ee69-b4c8-4b0c-8059-23af341c9214" # Production version of Right-of-Way Permits table
    published_ids = [x['id'] for x in query_resource(site, 'SELECT DISTINCT id FROM "{}"'.format(resource_id), API_key)]
    print(f"Found {len(published_ids)} extant, published 'id' values.")

    written = 0
    open_dates = []
    for row in reader:
        if datetime.datetime.strptime(row['openDate'], '%Y-%m-%d') >= datetime.datetime.now() - datetime.timedelta(
                days=6): # This condition has been reversed with respect to the original preprocess.py script
            # to catch the uncaught rows.
            print('too new', row['openDate'])
            continue

        # If the 'id' field value is already in the published data, skip this row.
        if row['display'] in published_ids:
            continue
        else:
            written += 1

        new_row_base = {}

        for new_heading, old_heading in ROW_MAPPING.items():
            new_row_base[new_heading] = row[old_heading]

        for i in range(6):
            if not i:
                suffix = ''  # no appending number for 0th item
            else:
                suffix = '.c' + str(i)

            location = row['asis.Location{}'.format(suffix)]
            to_street = row['asis.To{}'.format(suffix)]
            from_street = row['asis.From{}'.format(suffix)]

            new_row = copy.deepcopy(new_row_base)
            new_row['sequence'] = str(i - 1) if i else 0

            open_dates.append(row['openDate'])

            if location != 'NA':
                new_row['location'] = location
                new_row['from_street'] = from_street
                new_row['to_street'] = to_street
                new_row['from_lat'], new_row['from_lon'] = '', ''
                new_row['to_lat'], new_row['to_lon'] = '', ''
                location = clean_street(location)
                if from_street != 'NA':
                    x, y = '', '' ##cross_geocode(location, from_street)
                    new_row['from_lat'], new_row['from_lon'] = y, x
                    #streets['{}--{}'.format(location, from_street)] = (x, y)
                    #streets['{}--{}'.format(from_street, location)] = (x, y)

                if to_street != 'NA':
                    x, y = '', '' ##cross_geocode(location, to_street)
                    new_row['to_lat'], new_row['to_lon'] = y, x
                    #streets['{}--{}'.format(location, to_street)] = (x, y)
                    #streets['{}--{}'.format(to_street, location)] = (x, y)

                writer.writerow(new_row)

            elif not (i):  # there is no location on the first attempt - must use address
                new_row['location'] = ''
                new_row['from_street'] = ''
                new_row['to_street'] = ''
                writer.writerow(new_row)

    msg = f"Found {written} rows which are overdue for writing to the Right-of-Way Permits table."
    if written > 0 and len(open_dates) > 0:
        msg += f" open_date values range from {min(open_dates)} to {max(open_dates)}. Preparing to push them to the CKAN table."
    print(msg)
    print(f"Wrote {written} rows to {OUTPUT_FILE}.")
    if not kwparameters['test_mode']:
        channel = "@david" #if (test_mode or not PRODUCTION) else "#etl-hell"
        send_to_slack(msg, username='Right-of-Way Permits Backup preprocessor', channel=channel, icon=':illuminati:')


job_dicts = [
    {
        'source_type': 'local',
        'source_dir': '',
        'source_file': 'right_of_way_transmuted_backup.csv',
        'encoding': 'utf-8-sig',
        'custom_processing': preprocess,
        'schema': RightOfWaySchema,
        'primary_key_fields': ['id', 'sequence', 'type'],
        'upload_method': 'upsert',
        'package': package_id,
        'resource_name': 'Right of Way Permits',
    }
]
