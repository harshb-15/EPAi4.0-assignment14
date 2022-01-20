import csv
from collections import namedtuple
from datetime import datetime


def str_to_date(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def get_headers(file_name):
    with open(file_name, 'r') as f:
        csv_reader = csv.reader(f)
        return next(csv_reader)


def employment_itr():
    with open("employment.csv", 'r') as f:
        csv_reader = csv.reader(f)
        dta = namedtuple('dta', next(csv_reader))
        for row in csv_reader:
            yield dta(row[0], row[1], row[2], row[3])


def personal_info_itr():
    with open("personal_info.csv", 'r') as f:
        csv_reader = csv.reader(f)
        dta = namedtuple('dta', next(csv_reader))
        for row in csv_reader:
            yield dta(row[0], row[1], row[2], row[3], row[4])


def update_status_itr():
    with open("update_status.csv", 'r') as f:
        csv_reader = csv.reader(f)
        dta = namedtuple('dta', next(csv_reader))

        for row in csv_reader:
            yield dta(row[0], datetime.strptime(row[1], "%Y-%m-%dT%H:%M:%SZ"), str_to_date(row[2]))


def vehicles_itr():
    with open("vehicles.csv", 'r') as f:
        csv_reader = csv.reader(f)
        dta = namedtuple('dta', next(csv_reader))
        for row in csv_reader:
            yield dta(row[0], row[1], row[2], int(row[3]))


class DataIterable:
    def __init__(self, n=1000):
        self.n = n

    def __iter__(self):
        return self.DataIterator(self.n)

    class DataIterator:
        def __init__(self, n):
            self.n = n
            all_headers = list()
            all_headers.append(get_headers("employment.csv")[-1])
            all_headers += get_headers("employment.csv")[:3]
            all_headers += get_headers("personal_info.csv")[1:]
            all_headers += get_headers("update_status.csv")[1:]
            all_headers += get_headers("vehicles.csv")[1:]
            self.dta = namedtuple('dta', all_headers)
            self.a = employment_itr()
            self.b = personal_info_itr()
            self.c = update_status_itr()
            self.d = vehicles_itr()

        def __next__(self):

            if self.n == 0:
                raise StopIteration
            else:
                self.n -= 1
                row1 = next(self.a)
                row2 = next(self.b)
                row3 = next(self.c)
                row4 = next(self.d)
                return self.dta(row1[-1], row1[0], row1[1], row1[2], row2[1], row2[2], row2[3], row2[4], row3[1],
                                row3[2], row4[1], row4[2], row4[3])


def get_non_stale_rec():
    for dt in DataIterable():
        if dt.last_updated > datetime(2017, 3, 1):
            yield dt


f = dict()
m = dict()


def car_makes():
    for dt in DataIterable():
        if dt.gender == 'Male':
            if dt.vehicle_make in m.keys():
                m[dt.vehicle_make] += 1
            else:
                m[dt.vehicle_make] = 1
        if dt.gender == 'Female':
            if dt.vehicle_make in f.keys():
                f[dt.vehicle_make] += 1
            else:
                f[dt.vehicle_make] = 1
    mx = -1
    lst1 = list()
    for j in m:
        if m[j] >= mx:
            lst1.clear()
            lst1.append(j)
            mx = m[j]
    mx = -1
    lst2 = list()
    for j in f:
        if f[j] >= mx:
            lst2.clear()
            lst2.append(j)
            mx = f[j]
    return lst1, lst2
