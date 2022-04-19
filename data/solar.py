from data.climate import Climate


class Solar(Climate):

    def __init__(self, latitude, longitude, kv_dict={}):
        super().__init__(latitude, longitude, kv_dict)

    def bind_keys(self, keys):
        for k in keys:
            print(k)
        print(keys)


if __name__ == '__main__':
    sun = Solar(52.50, 51.50)
    sun.bind_keys(["t2m", "w10", "d10", "ssrd", "fdir"])
