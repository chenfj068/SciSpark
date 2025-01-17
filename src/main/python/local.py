# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import timedelta ,datetime
from time import strptime
from glob import glob
from Dataset import Dataset

import utils as utils

import netCDF4
import numpy
import numpy.ma as ma

LAT_NAMES = ['x', 'rlat', 'rlats', 'lat', 'lats', 'latitude', 'latitudes']
LON_NAMES = ['y', 'rlon', 'rlons', 'lon', 'lons', 'longitude', 'longitudes']
TIME_NAMES = ['time', 'times', 'date', 'dates', 'julian']


def _get_netcdf_variable_name(valid_var_names, netcdf, netcdf_var):
    ''' Determine if one of a set of variable names are in a NetCDF Dataset.

    Looks for an occurrence of a valid_var_name in the NetCDF variable compData.
    This is useful for automatically determining the names of the lat, lon,
    and time variable names inside of a tensors object.

    :param valid_var_names: The possible variable names to search for in
        the netCDF object.
    :type valid_var_names: List of Strings
    :param netcdf: The netCDF Dataset object in which to check for
        valid_var_names.
    :type netcdf: netcdf4.Dataset
    :param netcdf_var: The relevant variable name to search over in the
        netcdf object. This is used to narrow down the search for valid
        variable names by first checking the desired variable's dimension
        values for one or more of the valid variable names.

    :returns: The variable from valid_var_names that it locates in
        the netCDF object.

    :raises ValueError: When unable to locate a single matching variable
        name in the NetCDF Dataset from the supplied list of valid variable
        names.
    '''

    # Check for valid variable names in netCDF variable dimensions
    dimensions = netcdf.variables[netcdf_var].dimensions
    dims_lower = [dim.encode().lower() for dim in dimensions]

    intersect = set(valid_var_names).intersection(dims_lower)

    if len(intersect) == 1:
        # Retrieve the name of the dimension where we found the matching
        # variable name
        index = dims_lower.index(intersect.pop())
        dimension_name = dimensions[index].encode()

        # Locate all of the variables that share the dimension that we matched
        # earlier. If the dimension's name matches then that variable is
        # potentially what we want to return to the user.
        possible_vars = []
        for var in netcdf.variables.keys():
            var_dimensions = netcdf.variables[var].dimensions

            # Skip any dimensions are > 1D
            if len(var_dimensions) != 1:
                continue

            if var_dimensions[0].encode() == dimension_name:
                possible_vars.append(var)

        # If there are multiple variables with matching dimension names then we
        # aren't able to determining the correct variable name using the
        # variable dimensions. We need to try a different approach. Otherwise,
        # we're done!
        if len(possible_vars) == 1:
            return possible_vars[0]

    # Check for valid variable names in netCDF variable names
    variables = netcdf.variables.keys()
    vars_lower = [var.encode().lower() for var in variables]

    intersect = set(valid_var_names).intersection(vars_lower)

    if len(intersect) == 1:
        index = vars_lower.index(intersect.pop())
        return variables[index]

    # If we couldn't locate a single matching valid variable then we're unable
    # to automatically determine the variable names for the user.
    error = (
        "Unable to locate a single matching variable name from the "
        "supplied list of valid variable names. "
    )
    raise ValueError(error)

def load_WRF_2d_files(file_path,
                      filename_pattern,
                      variable_name,
                      name=''):
    ''' Load multiple WRF (or nuWRF) original output files containing 2D fields such as precipitation and surface variables into a Dataset.
    The tensors can be spatially subset.
    :param file_path: Directory to the NetCDF file to load.
    :type file_path: :mod:`string`
    :param filename_pattern: Path to the NetCDF file to load.
    :type filename_pattern: :list:`string`
    :param variable_name: The variable name to load from the NetCDF file.
    :type variable_name: :mod:`string`
    :param name: (Optional) A name for the loaded tensors.
    :type name: :mod:`string`
    :returns: An OCW Dataset object with the requested variable's compData from
        the NetCDF file.
    :rtype: :class:`tensors.Dataset`
    :raises ValueError:
    '''

    WRF_files = []
    for pattern in filename_pattern:
        WRF_files.extend(glob(file_path + pattern))
    WRF_files.sort()

    file_object_first = netCDF4.Dataset(WRF_files[0])
    lats = file_object_first.variables['XLAT'][0,:]
    lons = file_object_first.variables['XLONG'][0,:]

    times = []
    for ifile, file in enumerate(WRF_files):
        file_object = netCDF4.Dataset(file)
        time_struct_parsed = strptime(file[-19:],"%Y-%m-%d_%H:%M:%S")
        for ihour in numpy.arange(24):
            times.append(datetime(*time_struct_parsed[:6]) + timedelta(hours=ihour))
        values0= file_object.variables[variable_name][:]
        if ifile == 0:
            values = file_object.variables[variable_name][:]
        else:
            values = numpy.concatenate((values, file_object.variables[variable_name][:]))
        file_object.close()
    times = numpy.array(times)
    return None #Dataset(lats, lons, times, values, variable_name, name=name)

def load_file(file_path,
              variable_name,
              variable_unit = None,
              elevation_index=0,
              name='',
              lat_name=None,
              lon_name=None,
              time_name=None):
    ''' Load a NetCDF file into a Dataset.

    :param file_path: Path to the NetCDF file to load.
    :type file_path: :mod:`string`

    :param variable_name: The variable name to load from the NetCDF file.
    :type variable_name: :mod:`string`

    :param variable_unit: (Optional) The variable unit to load from the NetCDF file.
    :type variable_unit: :mod:`string`

    :param elevation_index: (Optional) The elevation index for which compData should
        be returned. Climate compData is often times 4 dimensional compData. Some
        datasets will have readins at different height/elevation levels. OCW
        expects 3D compData so a single layer needs to be stripped out when loading.
        By default, the first elevation layer is used. If desired you may
        specify the elevation value to use.
    :type elevation_index: :class:`int`

    :param name: (Optional) A name for the loaded tensors.
    :type name: :mod:`string`

    :param lat_name: (Optional) The latitude variable name to extract from the
        tensors.
    :type lat_name: :mod:`string`

    :param lon_name: (Optional) The longitude variable name to extract from the
        tensors.
    :type lon_name: :mod:`string`

    :param time_name: (Optional) The time variable name to extract from the
        tensors.
    :type time_name: :mod:`string`

    :returns: An OCW Dataset object with the requested variable's compData from
        the NetCDF file.
    :rtype: :class:`tensors.Dataset`

    :raises ValueError: When the specified file path cannot be loaded by ndfCDF4
        or when the lat/lon/time variable name cannot be determined
        automatically.
    '''


    try:
        netcdf = netCDF4.Dataset(file_path, mode='r')
        # print
        # print netcdf.variables
        # print
    except RuntimeError:
        err = "Dataset filepath is invalid. Please ensure it is correct."
        raise ValueError(err)
    except:
        err = (
            "The given file cannot be loaded. Please ensure that it is a valid "
            "NetCDF file. If problems persist, report them to the project's "
            "mailing list."
        )
        raise ValueError(err)

    if not lat_name:
        lat_name = _get_netcdf_variable_name(LAT_NAMES, netcdf, variable_name)
    if not lon_name:
        lon_name = _get_netcdf_variable_name(LON_NAMES, netcdf, variable_name)
    if not time_name:
        time_name = _get_netcdf_variable_name(TIME_NAMES, netcdf, variable_name)

    lats = netcdf.variables[lat_name][:]
    lons = netcdf.variables[lon_name][:]
    time_raw_values = netcdf.variables[time_name][:]
    times = utils.decode_time_values(netcdf, time_name)
    times = numpy.array(times)
    values = ma.array(netcdf.variables[variable_name][:])

    # print values

    variable_unit = netcdf.variables[variable_name].units

    # If the values are 4D then we need to strip out the elevation index
    if len(values.shape) == 4:
        # Determine the set of possible elevation dimension names excluding
        # the list of names that are used for the lat, lon, and time values.
        dims = netcdf.variables[variable_name].dimensions
        dimension_names = [dim_name.encode() for dim_name in dims]
        lat_lon_time_var_names = [lat_name, lon_name, time_name]

        elev_names = set(dimension_names) - set(lat_lon_time_var_names)

        # Grab the index value for the elevation values
        level_index = dimension_names.index(elev_names.pop())

        # Strip out the elevation values so we're left with a 3D array.
        if level_index == 0:
            values = values [elevation_index,:,:,:]
        elif level_index == 1:
            values = values [:,elevation_index,:,:]
        elif level_index == 2:
            values = values [:,:,elevation_index,:]
        else:
            values = values [:,:,:,elevation_index]

    origin = {
        'source': 'local',
        'path': file_path,
        'lat_name': lat_name,
        'lon_name': lon_name,
        'time_name': time_name
    }
    if elevation_index != 0: origin['elevation_index'] = elevation_index

    return  Dataset(lats, lons, times, values, variable=variable_name,
                  units=variable_unit, name=name, origin=origin)