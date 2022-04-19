from abc import abstractmethod

import numpy as np


class Climate:
    latitude: float
    longitude: float
    keys: []

    def __init__(self, latitude, longitude, kv_dict):
        self.latitude = latitude
        self.longitude = longitude
        self.kv_dict = kv_dict

    def convert_to_numpy(self, arr):
        k = arr["key"]
        v = arr["value"]["values"]
        if k in self.kv_dict:
            self.kv_dict[k].append(list(v.values()))
        else:
            self.kv_dict[k] = [list(v.values())]

    @abstractmethod
    def bind_keys(self, keys):
        pass

    def get_average_array(self, kv_dict):
        return np.mean(kv_dict, axis=0)

    def square(self, poly):
        rounded = []
        for latlong in poly:
            rounded.append([round(num) for num in latlong])
        xs, ys = zip(*rounded)
        minx, maxx = min(xs), max(xs)
        miny, maxy = min(ys), max(ys)

        newPoly = [(int(x - minx), int(y - miny)) for (x, y) in rounded]

        X = maxx - minx + 1
        Y = maxy - miny + 1

        grid = np.zeros((X, Y), dtype=np.int8)
        mahotas.polygon.fill_polygon(newPoly, grid)

        return [(x + minx, y + miny) for (x, y) in zip(*np.nonzero(grid))]
