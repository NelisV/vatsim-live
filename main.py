# -----------------------------------------------------------
# VATSIM Live
#
# Script generate live geojson files to display traffic.
#
#
#   TODO: match FIR boundary to polygon
#
# (C) 2015 Niels
# -----------------------------------------------------------


import urllib.request
import json
import csv
import random
import geojson
from geojson import Point, Feature, FeatureCollection
from datetime import datetime, timezone
import time
from dateutil import parser
import folium


def get_data_url():
    with urllib.request.urlopen('https://status.vatsim.net/status.json') as url:
        content = json.loads(url.read().decode())
        data_url = random.choice(content['data']['v3'])

    return data_url


def get_vatsim_data():
    global global_timestamp, request_cnt, update_cnt, iter_start_time
    while True:
        with urllib.request.urlopen(get_data_url()) as url:
            data = json.loads(url.read().decode())

            data_time = parser.isoparse(data['general']['update_timestamp'])
            request_cnt += 1

            if global_timestamp == data_time:
                print('equal timestamp, waiting 5s...\n')
                log([
                    global_timestamp.strftime('%H:%M:%S'),
                    datetime.utcnow().strftime('%H:%M:%S'),
                    'skipped duplicate data'
                     ])
                time.sleep(5)
                iter_start_time = datetime.now()
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
                    if flight['flight_plan']['departure'][0] in ['G', 'E', 'L']:
                        if flight['flight_plan']['arrival'][0] in ['K', 'C', 'T', 'M']:
                            not_booked_cross += 1
                            flight_data.update({
                                'cross_a_no_booking': '1'
                            })
                            if flight['flight_plan']['departure'] in ['ENGM', 'LOWW', 'EDDM', 'EHAM', 'EGKK', 'EIDW', 'LFPG', 'LTFM', 'LPPT', 'GCTS']:
                                if flight['flight_plan']['arrival'] in ['KATL', 'CYWG', 'CYVR', 'KIAH', 'CYUL', 'KBOS', 'KEWR', 'KPHL', 'KTPA', 'TFFR', 'TFFF', 'TBPB']:
                                    not_booked_event += 1
                                    flight_data.update({
                                        'event_apts_no_booking': '1'
                                    })

    point_object = Point((flight['longitude'], flight['latitude']))
    flight_feature = Feature(geometry=point_object, properties=flight_data)
    return flight_feature


def load_slots():
    slot_list = []
    with open('data/bookings_ctpw22.csv', 'r') as slots:
        for line in csv.DictReader(slots, delimiter=';'):
            slot_list.append(line)

    return slot_list


def log(data):
    filename = 'logs/log {}.csv'.format(str(datetime.utcnow().strftime('%d%m%Y')))
    with open(filename, 'a', newline='') as log_file:
        writer = csv.writer(log_file)
        writer.writerow(data)


def load_map_data():
    cycle = '2202'

    fn = 'AIRAC/{}/Boundaries.geojson'.format(cycle)
    with open(fn, 'r') as f:
        boundaries = json.load(f)
        print(type(boundaries))

    fn = 'AIRAC/{}/Countries.json'.format(cycle)
    with open(fn, 'r') as f:
        countries = json.load(f)
        print(type(countries))

    fn = 'AIRAC/{}/FIRs.json'.format(cycle)
    with open(fn, 'r') as f:
        firs = json.load(f)
        print(type(firs))

    fn = 'AIRAC/{}/UIRs.json'.format(cycle)
    with open(fn, 'r') as f:
        uirs = json.load(f)
        print(type(uirs))

    data_dict = {
        'boundaries': boundaries,
        'countries': countries,
        'firs': firs,
        'uirs': uirs
    }

    print(data_dict.keys())
    return data_dict


if __name__ == '__main__':
    # Settings
    event = False
    test = False

    # Global params
    global_timestamp = datetime.utcnow()
    request_cnt = 0
    update_cnt = 0
    flight_count_persistent = 0

    # load airac data
    airac_data = load_map_data()

    # Start loop
    while True:
        iter_start_time = datetime.now()

        # counts
        flight_count = 0
        booked_cid_cnt = 0
        cid_cs_mismatch = 0
        not_booked_cross = 0
        not_booked_event = 0

        # start script
        feature_list = []
        if event:
            slots = load_slots()
        vatsim_data = get_vatsim_data()
        vatsim_pilot_data = vatsim_data['pilots']
        vatsim_controller_data = vatsim_data['controllers']

        # pilot data
        # create stats point:
        vatsim_pilot_data.append(
            {'cid': 1, 'name': 'Data', 'callsign': 'DATA_INFO', 'server': 'UK', 'pilot_rating': '', 'latitude': 58.340,
             'longitude': -19.402, 'altitude': flight_count_persistent, 'groundspeed': '', 'transponder': '', 'heading': '', 'qnh_i_hg': '',
             'qnh_mb': '', 'flight_plan': [], 'logon_time': '',
             'last_updated': str(global_timestamp.strftime('%H:%M:%S'))}
        )
        for flight in vatsim_pilot_data:
            if event:
                feature_list.append(
                    create_feature(flight, slots)
                )
            else:
                feature_list.append(
                    create_feature(flight)
                )

        collection = FeatureCollection(feature_list)
        with open('live_output/live_flights.geojson', 'w') as out_file:
            dump = geojson.dump(collection, out_file, indent=4)

        # Temporary turned off for event
        #
        #
        # # controller data
        # for station in vatsim_controller_data:
        #
        #     # filter radar controllers
        #     if station['facility'] > 5:
        #         # print(station)
        #         print('\n{} {} {}'.format(station['callsign'], station['frequency'], station['facility']))
        #
        #         # separate station code from callsign
        #         callsign = station['callsign'].split('_')[0]
        #         print(callsign)
        #
        #         # look for entry in firs data
        #         entry_match = None
        #         for entry in airac_data['firs']:
        #             if callsign == entry['ICAO']:
        #                 print('found')
        #                 print(entry)
        #                 entry_match = entry
        #             elif entry['callsign_prefix']:
        #                 if callsign == entry['callsign_prefix']:
        #                     print('found alias')
        #                     print(entry)
        #                     entry_match = entry
        #
        #         if not entry_match:
        #             print('NOTHING FOUND')

        # information
        print('{} - Flights connected: {}'.format(
                global_timestamp.strftime('%H:%M:%S'),
                flight_count,
                ))
        if event:
            print(
                'Booked: {}, CID/CS mismatch: {}, Not Booked: Atlantic Crossing: {}, Between Event Airports: {}'.format(
                    booked_cid_cnt,
                    cid_cs_mismatch,
                    not_booked_cross,
                    not_booked_event
                )
            )
        print('fetch/request ratio: {:.1%}'.format(request_cnt/update_cnt))
        delay = (datetime.now(timezone.utc)-global_timestamp).seconds
        print('delay {} secs'.format(delay))
        print('\r')

        log([
            global_timestamp.strftime('%H:%M:%S'),
            datetime.utcnow().strftime('%H:%M:%S'),
            delay,
            flight_count,
            booked_cid_cnt,
            cid_cs_mismatch,
            not_booked_cross,
            not_booked_event
        ])

        # timer
        flight_count_persistent = flight_count
        if test:
            time.sleep(2)
        else:
            time.sleep(15-(datetime.now()-iter_start_time).seconds)
