import urllib.request
import json
import csv
import random
import geojson
from geojson import Point, Feature, FeatureCollection
from datetime import datetime
import time


def get_data_url():
    with urllib.request.urlopen('https://status.vatsim.net/status.json') as url:
        content = json.loads(url.read().decode())
        data_url = random.choice(content['data']['v3'])

    return data_url


def get_vatsim_data():
    with urllib.request.urlopen(get_data_url()) as url:
        data = json.loads(url.read().decode())

        return data


def create_feature(flight, slots=None):
    global flight_count, booked_cid_cnt, cid_cs_mismatch, not_booked_cross, not_booked_event
    flight_count += 1
    flight_data = {
        'callsign': flight['callsign'],
        'altitude': flight['altitude'],
        'heading': flight['heading'],
        'cid': flight['cid']
    }
    if flight['flight_plan']:
        flight_data.update({
            'adep': flight['flight_plan']['departure'],
            'ades': flight['flight_plan']['arrival'],
            'ac_type': flight['flight_plan']['aircraft_short']
        })
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
            if flight['flight_plan']:
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
    global log
    header = ['time', 'flights', 'booked', 'cs mismatch', 'not booked crossing', 'not booked between event apts']
    filename = 'logs/log {}.csv'.format(str(datetime.utcnow()))
    with open(filename, 'w') as log_file:
        writer = csv.writer(log_file)
        if not log:
            writer.writerow(header)
        writer.writerow(data)


if __name__ == '__main__':
    log = []
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
        for flight in get_vatsim_data()['pilots']:
            feature_list.append(
                create_feature(flight, slots)
            )

        collection = FeatureCollection(feature_list, crs={
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:OGC:1.3:CRS84"
            }})
        with open('output.geojson', 'w') as out_file:
            dump = geojson.dump(collection, out_file)

        print('Data updated {}. \n'
              'Flights: {}, \n'
              'Booked: {}, \n'
              'CID/CS mismatch: {}, \n'
              'not booked crossing: {}, \n'
              'not booked event apts: {}\n'.format(
                datetime.utcnow(),
                flight_count,
                booked_cid_cnt,
                cid_cs_mismatch,
                not_booked_cross,
                not_booked_event
                ))
        # log(str(
        #     datetime.utcnow(),
        #     flight_count,
        #     booked_cid_cnt,
        #     cid_cs_mismatch,
        #     not_booked_cross,
        #     not_booked_event
        # )
        time.sleep(35)
