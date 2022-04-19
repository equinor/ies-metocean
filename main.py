import io
import os
import asyncio
import json
from typing import Dict, Any, List, Tuple

import mahotas
from aiohttp import ClientSession
import h5py
import numpy as np
import timer
import argparse


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def http_get_with_aiohttp(session: ClientSession, url: str, headers=None, proxy: str = None,
                                timeout: int = 10) -> (int, Dict[str, Any], bytes):
    if headers is None:
        headers = {}
    response = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)

    response_json = None
    try:
        response_json = await response.json(content_type=None)
    except json.decoder.JSONDecodeError as e:
        pass

    response_content = None
    try:
        response_content = await response.read()

    except:
        print(response_content)
        pass

    return response.status, response_json, response_content


async def http_get_with_aiohttp_parallel(session: ClientSession, list_of_urls: List[str], headers=None,
                                         proxy: str = None, timeout: int = 10) -> (
        List[Tuple[int, Dict[str, Any], bytes]], float):
    if headers is None:
        headers = {}

    lists = chunks(list_of_urls, 5)

    results = []
    for li in lists:
        results.append(await asyncio.gather(
            *[http_get_with_aiohttp(session, url, headers, proxy, timeout) for url in li]))
        await asyncio.sleep(15)
    return np.concatenate(results)


def get_era_5_urls(keys, from_date, to_date, input_coordinates):
    urls = []
    for input_coordinate in input_coordinates:
        url = (f'https://api.gateway.equinor.com/metocean/era/5/data?from={from_date}&'
               f'to={to_date}&longitude={input_coordinate[1]}&latitude={input_coordinate[0]}&keys={keys}')
        urls.append(url)
        print(url)
    return urls


def get_params(file_path):
    with open(file_path, 'r') as file:
        data = file.read()
    obj = json.loads(data)
    return obj


def check_type(arg, params):
    if arg == "wind":
        return params["wind_keys"]
    elif arg == "solar":
        return params["solar_keys"]
    else:
        raise ValueError("You must supply either wind or solar as an argument")


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


def create_dataset(file, dataset, data):
    with h5py.File(file, 'a') as f:
        if dataset not in f:
            f.create_dataset(name=dataset, data=data)
            print(f'created dataset: {dataset}')


def get_average_array(numpy_array):
    return np.mean(numpy_array, axis=0)


def square(poly):
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
                           help='avg: average all coordinate value. ind: get all values from each coordinate',
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

    params = get_params(args.config)
    if args.square:
        coords = params['coordinates']
        if len(coords) != 4:
            raise ValueError("square option requires four coordinates")
        else:
            coordinates = square(params['coordinates'])
    else:
        coordinates = params['coordinates']

    keys = check_type(args.type, params)
    urls = get_era_5_urls("&keys=".join(keys), params["from"], params["to"], coordinates)
    session = ClientSession()
    results = await http_get_with_aiohttp_parallel(session, urls, headers, timeout=1000)
    req_timer.stop("request_timer")
    await session.close()

    json_dict = {}
    coordinates = []
    for result in results:
        if result[0] != 200:
            print("not 200: ", result)
            continue
        print("200: ", result[0])

        key = list(result[1]["data"].keys())
        coordinate = check_coordinate_object(key)
        coordinates.append(coordinate.split("_"))

        for value in result[1]["data"][coordinate]:
            k = value["key"]
            v = value["value"]["values"]

            if k in json_dict:
                json_dict[k].append(list(v.values()))
            else:
                json_dict[k] = [list(v.values())]

    # Does not work
    if args.mode == "avg":
        for arr in json_dict:
            values = get_average_array(np.asarray(json_dict[arr]).astype(float))
            create_dataset("data.h5", arr + "-avg", values)
    elif args.mode == "ind":
        for arr in json_dict:
            numpy_matches = np.asarray(json_dict[arr]).astype(float)
            create_dataset("data.h5", arr, numpy_matches)

    numpy_coords = np.asarray(coordinates).astype(float)
    create_dataset('data.h5', "coordinates", numpy_coords)


asyncio.run(main())
