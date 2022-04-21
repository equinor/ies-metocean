import json
from typing import Dict, Any
import mahotas
import numpy as np

import asyncio

from aiohttp import ClientSession, ClientResponseError


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


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


class Client:
    keys: []
    coordinates: []
    from_date: str
    to_date: str
    headers: {}

    def __init__(self, headers, params_file, arg):
        self.headers = headers
        with open(params_file, 'r') as file:
            data = file.read()
        obj = json.loads(data)

        self.from_date = obj["from"]
        self.to_date = obj["to"]
        self.coordinates = obj["coordinates"]
        if arg == "wind":
            self.keys = obj["wind_keys"]
        elif arg == "solar":
            self.keys = obj["solar_keys"]
        else:
            raise ValueError("You must supply either wind or solar as an argument")

    def get_urls(self, arg):
        keys_str = '&keys='.join(self.keys)
        urls = []
        self.coordinates = self.check_square(arg)

        for input_coordinate in self.coordinates:
            url = (f'https://api.gateway.equinor.com/metocean/{arg.hindcast}/data?from={self.from_date}&'
                   f'to={self.to_date}&longitude={input_coordinate[1]}&latitude={input_coordinate[0]}&keys={keys_str}')
            urls.append(url)
            print(url)
        return urls

    async def http_get_with_aiohttp(self, session, url, timeout=10):
        try:
            async with session.get(url=url, headers=self.headers, timeout=timeout, raise_for_status=True) as resp:
                response = None
                if resp.status != 200:
                    response = {"error": f"server returned {resp.status}"}
                else:
                    response = await resp.json()

        except ClientResponseError as e:
            if e.status == 429:
                print(e)
                print(response)
        except Exception as ex:
            print(ex)
        except asyncio.TimeoutError:
            response = {"results": f"timeout error on {url}"}

        return response

    async def get_climate_data(self, arg, interval=15):
        async with ClientSession() as session:
            urls = self.get_urls(arg)
            counter = len(urls)
            lists = chunks(urls, 5)
            results = []
            for li in lists:
                counter = counter - len(li)
                results.append(await asyncio.gather(
                    *[self.http_get_with_aiohttp(session=session, url=url, timeout=10000) for url in li]))
                await asyncio.sleep(interval)
                print('\rremaining requests: ' + str(counter) + '/' + str(len(urls)), end='')
            print('\nall requests have been completed')
            return np.concatenate(results)

    def check_square(self, arg):
        if arg.square:
            if len(self.coordinates) != 4:
                raise ValueError('square option requires four coordinates')
            else:
                return square(self.coordinates)
        else:
            return self.coordinates
