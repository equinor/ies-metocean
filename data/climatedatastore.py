import h5py
import numpy as np


def get_average_array(numpy_array):
    return np.mean(numpy_array, axis=0)


def check_coordinate_object(coordinates):
    if len(coordinates) == 2:
        coordinate = coordinates[1]
        return coordinate
    elif len(coordinates) == 1:
        coordinate = coordinates[0]
        return coordinate
    else:
        raise ValueError(f'coordinates object is not'
                         f' formatted correctly. Object: {coordinates}')


class ClimateDatastore:
    coordinates: []
    from_date: str
    to_date: str
    headers: {}
    kv_dict: {}

    def __init__(self, results):
        self.coordinates = []
        self.kv_dict = {}
        for result in results:
            key = list(result["data"].keys())
            coordinate = check_coordinate_object(key)
            self.coordinates.append(coordinate.split("_"))
            for value in result["data"][coordinate]:
                key = value["key"]
                values = value["value"]["values"]
                if key in self.kv_dict:
                    self.kv_dict[key].append(list(values.values()))
                else:
                    self.kv_dict[key] = [list(values.values())]

    def create_datasets(self, args, file):
        with h5py.File(file, 'a') as f:
            if "coordinates" not in f:
                f.create_dataset(name="coordinates", data=np.asarray(self.coordinates).astype(float))
            for key in self.kv_dict.keys():
                if key not in f:
                    if args.mode == "avg":
                        f.create_dataset(name=key, data=get_average_array(np.asarray(self.kv_dict[key]).astype(float)))
                    elif args.mode == "ind":
                        f.create_dataset(name=key, data=np.asarray(self.kv_dict[key]).astype(float))
                    print(f'created dataset: {key}')
                else:
                    raise ValueError(f"{key} already exists in {file}. append mode is not developed yet.")

    def calculate_dhi(self):
        self.kv_dict["dhi"] = np.divide(self.kv_dict["ssrd"], 3600) - np.divide(self.kv_dict["fdir"], 3600)

    def calculate_ghi(self):
        self.kv_dict["ghi"] = np.divide(self.kv_dict["ssrd"], 3600)
