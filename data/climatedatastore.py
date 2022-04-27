import h5py
import numpy as np


def get_average_array(numpy_array):
    """Returns the average of all values in an array."""
    return np.mean(numpy_array, axis=0)


def check_coordinate_object(coordinates):
    """Validates the formatting of the coordinate-object returned from API."""
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

    def create_datasets(self, file):
        """Takes in a file name, and creates hdf5 datasets using the datastore's kv_dict and coordinates attributes. """
        with h5py.File(file, 'a') as f:
            if "coordinates" not in f:
                f.create_dataset(name="coordinates", data=np.asarray(self.coordinates).astype(float))
            for key in self.kv_dict.keys():
                if key not in f:
                    f.create_dataset(name=key, data=np.asarray(self.kv_dict[key]).astype(float))
                    print(f'created dataset: {key}')
                else:
                    raise ValueError(f"{key} already exists in {file}. append mode is not developed yet.")

    def calculate_dhi(self):
        """
        The function calculates the diffuse horizontal irradiance (dhi) by subtracting the direct normal irradiance (dni)
        from the global horizontal irradiance (ghi)
        """
        self.kv_dict["dhi"] = np.divide(self.kv_dict["ssrd"], 3600) - np.divide(self.kv_dict["fdir"], 3600)

    def calculate_ghi(self):
        """
        The function `calculate_ghi` takes the `ssrd` key-value pair from the `kv_dict` dictionary and divides it by 3600
        to get the `ghi` key-value pair
        """
        self.kv_dict["ghi"] = np.divide(self.kv_dict["ssrd"], 3600)
