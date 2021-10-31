import urllib.request
import json
import csv
import random
import geojson
from geojson import Point, Feature, FeatureCollection
from datetime import datetime
import time
from dateutil import parser


def get_data_url():
    with urllib.request.urlopen('https://status.vatsim.net/status.json') as url:
        content = json.loads(url.read().decode())
        data_url = random.choice(content['data']['v3'])

    return data_url


def get_vatsim_data():
    global global_timestamp, request_cnt, update_cnt
    while True:
        with urllib.request.urlopen(get_data_url()) as url:
            data = json.loads(url.read().decode())

            data_time = parser.isoparse(data['general']['update_timestamp'])
            request_cnt += 1

            if global_timestamp == data_time:
                print('equal timestamp, waiting 5s...\n')
                time.sleep(5)
            else:
                global_timestamp = data_time
                update_cnt += 1
                return data


def create_feature(flight, slots=None):
    global flight_count, booked_cid_cnt, cid_cs_mismatch, not_booked_cross, not_booked_event
    flight_count += 1

    flight_data = flight.copy()
    for item in ['latitude', 'longitude', 'flight_plan']:
        flight_data.pop(item)

    if flight['flight_plan']:
        flight_plan = {}
        for item in flight['flight_plan'].items():
            item = list(item)
            item[0] = 'fp_{}'.format(item[0])
            flight_plan[item[0]] = item[1]
        flight_data.update(flight_plan)

        # slot analysis (event mode setting)
        if slots:
            # search for match on CID
            search_1 = list(filter(lambda pos: pos['CID'] == str(flight['cid']), slots))
            if search_1:
                flight_data.update({
                    'cid_bkd': '1'
                })
                booked_cid_cnt += 1
                search_2 = list(filter(lambda pos: pos['C/S'] == str(flight['callsign']), slots))
                if search_2:
                    flight_data.update({
                        'cs_match': '1'
                    })
                else:
                    flight_data.update({
                        'cs_match': '0'
                    })
                    cid_cs_mismatch += 1
            else:
                # CID does not hold a booking
                flight_data.update({
                    'cid_bkd': '0',
                    'cs_match': '0'
                })
                if flight['flight_plan']['departure'] and flight['flight_plan']['arrival']:
                    if flight['flight_plan']['departure'][0] in ['K', 'C']:
                        if flight['flight_plan']['arrival'][0] in ['E', 'L']:
                            not_booked_cross += 1
                            flight_data.update({
                                'cross_a_no_booking': '1'
                            })
                            if flight['flight_plan']['departure'] in ['KBOS', 'KORD', 'KATL', 'KJFK', 'KMIA', 'CYYZ', 'KIAD']:
                                if flight['flight_plan']['arrival'] in ['EHAM', 'EDDB', 'EIDW', 'EFHK', 'EGLL', 'LFPG', 'LOWW', 'LEBL']:
                                    not_booked_event += 1
                                    flight_data.update({
                                        'event_apts_no_booking': '1'
                                    })

    point_object = Point((flight['longitude'], flight['latitude']))
    flight_feature = Feature(geometry=point_object, properties=flight_data)
    return flight_feature


def load_slots():
    slot_list = []
    with open('data/bookings.csv', 'r') as slots:
        for line in csv.DictReader(slots):
            slot_list.append(line)

    return slot_list


def log(data):
    filename = 'logs/log {}.csv'.format(str(datetime.utcnow().strftime('%d%m%Y')))
    with open(filename, 'a') as log_file:
        writer = csv.writer(log_file)
        writer.writerow(data)


if __name__ == '__main__':
    # Settings
    event = False
    test = False

    global_timestamp = datetime.utcnow()
    request_cnt = 0
    update_cnt = 0
    flight_count_persistent = 0
    while True:
        # counts
        flight_count = 0
        booked_cid_cnt = 0
        cid_cs_mismatch = 0
        not_booked_cross = 0
        not_booked_event = 0

        # start script
        feature_list = []
        slots = load_slots()
        vatsim_data = get_vatsim_data()['pilots']
        # create stats point:
        vatsim_data.append(
            {'cid': 1, 'name': 'Data', 'callsign': 'DATA_INFO', 'server': 'UK', 'pilot_rating': '', 'latitude': 58.340,
             'longitude': -19.402, 'altitude': flight_count_persistent, 'groundspeed': '', 'transponder': '', 'heading': '', 'qnh_i_hg': '',
             'qnh_mb': '', 'flight_plan': [], 'logon_time': '',
             'last_updated': str(global_timestamp.strftime('%H:%M:%S'))}
        )
        for flight in vatsim_data:
            if event:
                feature_list.append(
                    create_feature(flight, slots)
                )
            else:
                feature_list.append(
                    create_feature(flight)
                )

        collection = FeatureCollection(feature_list, crs={
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }})
        with open('output.geojson', 'w') as out_file:
            dump = geojson.dump(collection, out_file, indent=4)

        print('{} - Flights connected: {}'.format(
                global_timestamp.strftime('%H:%M:%S'),
                flight_count,
                ))
        if event:
            print(
                'Booked: {}, CID/CS mismatch: {}\n'
                'Not Booked: Atlantic Crossing: {}, Between Event Airports: {}'.format(
                    booked_cid_cnt,
                    cid_cs_mismatch,
                    not_booked_cross,
                    not_booked_event
                )
            )
        print('fetch/request ratio: {:.2%}'.format(request_cnt/update_cnt))
        print('\n')

        log([
            global_timestamp.strftime('%H:%M:%S'),
            flight_count,
            booked_cid_cnt,
            cid_cs_mismatch,
            not_booked_cross,
            not_booked_event
        ])
        flight_count_persistent = flight_count
        if test:
            time.sleep(2)
        else:
            time.sleep(14.9)
