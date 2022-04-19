import os
import asyncio
import timer
import argparse
from data.client import Client
from data.climatedatastore import ClimateDatastore


def setup_parser():
    my_parser = argparse.ArgumentParser(description="Retrieve ERA5/Nora10 data for wind or solar")
    my_parser.add_argument('--type',
                           '-t',
                           action='store',
                           help='wind or solar',
                           required=True)

    my_parser.add_argument('--config',
                           '-c',
                           action='store',
                           help='path to config file',
                           required=True)

    my_parser.add_argument('--mode',
                           '-m',
                           action='store',
                           help='avg: average all coordinate value. ind: get all values from each coordinate, square: '
                                'get all rounded coordinates within one area defined by four edge coordinates',
                           required=True)

    my_parser.add_argument('--square',
                           '-s',
                           default=False,
                           action="store_true",
                           help='get all rounded coordinates within one area defined by four edge coordinates')
    return my_parser.parse_args()


async def main():
    args = setup_parser()
    req_timer = timer.Timer()
    req_timer.start()
    headers = {
        'Authorization': 'Bearer ' + os.getenv("token"),
        'Api-Version': 'v1',
        'Ocp-Apim-Subscription-Key': os.getenv("ocp")
    }
    req = Client(headers, "params.json", args.type)
    results = await req.get_climate_data(args)

    datastore = ClimateDatastore(results)
    if args.type == "solar":
        datastore.calculate_dhi()
        datastore.calculate_ghi()
    datastore.create_datasets(args, "test.h5")

asyncio.run(main())
