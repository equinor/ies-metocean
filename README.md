# ies-Metocean
Uses the metocean API to retrieve ERA5 and NORA10 climate data, and converts this data into the HDF5 format.

### Usage
This program requires to environment variables, an OCP token, and an authentication token. The OCP token is retrieved
through equinors internal service for subscribing to the metocean API. The authentication token has to be refreshed on
an hourly basis by visiting the metocean API site, and clicking the get token button. 