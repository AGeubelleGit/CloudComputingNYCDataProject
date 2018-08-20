# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import warnings

import numpy as np
import pandas as pd
import scipy.ndimage as ndi
from matplotlib import cm
from matplotlib import pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon
from matplotlib.patches import Rectangle
import matplotlib.patches as mpatches
from scipy.spatial import ConvexHull

from .google_static_maps_api import GoogleStaticMapsAPI
from .google_static_maps_api import MAPTYPE
from .google_static_maps_api import MAX_SIZE
from .google_static_maps_api import SCALE


BLANK_THRESH = 2 * 1e-3     # Value below which point in a heatmap should be blank


def register_api_key(api_key):
    """Register a Google Static Maps API key to enable queries to Google.
    Create your own Google Static Maps API key on https://console.developers.google.com.

    :param str api_key: the API key

    :return: None
    """
    GoogleStaticMapsAPI.register_api_key(api_key)


def background_and_pixels(latitudes, longitudes, size, maptype):
    """Queries the proper background map and translate geo coordinated into pixel locations on this map.

    :param pandas.Series latitudes: series of sample latitudes
    :param pandas.Series longitudes: series of sample longitudes
    :param int size: target size of the map, in pixels
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: map and pixels
    :rtype: (PIL.Image, pandas.DataFrame)
    """
    # From lat/long to pixels, zoom and position in the tile
    center_lat = (latitudes.max() + latitudes.min()) / 2
    center_long = (longitudes.max() + longitudes.min()) / 2
    zoom = GoogleStaticMapsAPI.get_zoom(latitudes, longitudes, size, SCALE)
    pixels = GoogleStaticMapsAPI.to_tile_coordinates(latitudes, longitudes, center_lat, center_long, zoom, size, SCALE)
    # Google Map
    img = GoogleStaticMapsAPI.map(
        center=(center_lat, center_long),
        zoom=zoom,
        scale=SCALE,
        size=(size, size),
        maptype=maptype,
    )
    return img, pixels


def scatter(latitudes, longitudes, size_const=40, colors=None, alpha_val=0.5, maptype=MAPTYPE, center=None, save_file=None, legend=None):
    """Scatter plot over a map. Can be used to visualize clusters by providing the marker colors.

    :param pandas.Series latitudes: series of sample latitudes
    :param pandas.Series longitudes: series of sample longitudes
    :param pandas.Series colors: marker colors, as integers
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: None
    """
    width = SCALE * MAX_SIZE
    colors = pd.Series(0, index=latitudes.index) if colors is None else colors
    if center is not None:
        latitudes = latitudes.append(pd.Series(center[1]))
        longitudes = longitudes.append(pd.Series(center[0]))
        if colors is not None:
            colors.append(pd.Series([0,0,0]))
    img, pixels = background_and_pixels(latitudes, longitudes, MAX_SIZE, maptype)
    plt.figure(figsize=(10, 10))
    plt.imshow(np.array(img))                                               # Background map
    plt.scatter(                                                            # Scatter plot
        pixels['x_pixel'],
        pixels['y_pixel'],
        c=colors,
        s=width / size_const,
        linewidth=0,
        alpha=alpha_val,
    )

    if center is not None:
        x = pixels['x_pixel'].get_values()
        x = x[len(x)-1]
        y = pixels['y_pixel'].get_values()
        y = y[len(y)-1]
        center_x = list()
        center_x.append(x)
        center_y = list()
        center_y.append(y)

        plt.scatter(                                                            # Scatter plot
            center_x,
            center_y,
            c='k',
            s=width / 2,
            linewidth=0,
            alpha=1.0,
            marker='*'
        )

    plt.gca().invert_yaxis()                                                # Origin of map is upper left
    plt.axis([0, width, width, 0])                                          # Remove margin
    plt.axis('off')
    plt.tight_layout()

    if legend is not None:
        plt.legend(handles=legend)

    if save_file is not None:
        fig = plt.gcf()
        fig.set_size_inches(18.5, 10.5)
        fig.savefig(save_file, dpi=300)

    plt.show()


def plot_markers(markers, maptype=MAPTYPE):
    """Plot markers on a map.

    :param pandas.DataFrame markers: DataFrame with at least 'latitude' and 'longitude' columns, and optionally
        * 'color' column, see GoogleStaticMapsAPI docs for more info
        * 'label' column, see GoogleStaticMapsAPI docs for more info
        * 'size' column, see GoogleStaticMapsAPI docs for more info
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: None
    """
    # Checking input columns
    fields = markers.columns.intersection(['latitude', 'longitude', 'color', 'label', 'size'])
    if len(fields) == 0 or 'latitude' not in fields or 'longitude' not in fields:
        msg = 'Input dataframe should contain at least colums \'latitude\' and \'longitude\' '
        msg += '(and columns \'color\', \'label\', \'size\' optionally).'
        raise KeyError(msg)
    # Checking NaN input
    nans = (markers.latitude.isnull() | markers.longitude.isnull())
    if nans.sum() > 0:
        warnings.warn('Ignoring {} example(s) containing NaN latitude or longitude.'.format(nans.sum()))
    # Querying map
    img = GoogleStaticMapsAPI.map(
        scale=SCALE,
        markers=markers[fields].loc[~nans].T.to_dict().values(),
        maptype=maptype,
    )
    plt.figure(figsize=(10, 10))
    plt.imshow(np.array(img))
    plt.tight_layout()
    plt.axis('off')
    plt.show()


def heatmap(latitudes, longitudes, values, resolution=None, maptype=MAPTYPE, alpha_val=0.15, save_file=None, title=None):
    """Plot a geographical heatmap of the given metric.

    :param pandas.Series latitudes: series of sample latitudes
    :param pandas.Series longitudes: series of sample longitudes
    :param pandas.Series values: series of sample values
    :param int resolution: resolution (in pixels) for the heatmap
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: None
    """
    img, pixels = background_and_pixels(latitudes, longitudes, MAX_SIZE, maptype)
    # Smooth metric
    z = grid_density_gaussian_filter(
        zip(pixels['x_pixel'], pixels['y_pixel'], values),
        MAX_SIZE * SCALE,
        resolution=resolution if resolution else MAX_SIZE * SCALE,          # Heuristic for pretty plots
    )
    # Plot
    width = SCALE * MAX_SIZE

    plt.figure(figsize=(10, 10))
    plt.imshow(np.array(img))                                               # Background map
    plt.imshow(z, origin='lower', extent=[0, width, 0, width], alpha=alpha_val)  # Foreground, transparent heatmap
    #plt.scatter(pixels['x_pixel'], pixels['y_pixel'], s=1)                  # Markers of all points
    plt.gca().invert_yaxis()                                                # Origin of map is upper left
    plt.axis([0, width, width, 0])                                          # Remove margin
    plt.axis('off')
    plt.tight_layout()

    if save_file is not None:
        fig = plt.gcf()
        fig.set_size_inches(18.5, 10.5)
        fig.savefig(save_file, dpi=300)

    plt.show()


def density_plot(latitudes, longitudes, alpha_val=0.15, save_file=None, resolution=None, maptype=MAPTYPE, title=None):
    """Given a set of geo coordinates, draw a density plot on a map.

    :param pandas.Series latitudes: series of sample latitudes
    :param pandas.Series longitudes: series of sample longitudes
    :param int resolution: resolution (in pixels) for the heatmap
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: None
    """
    heatmap(latitudes, longitudes, np.ones(latitudes.shape[0]), alpha_val=alpha_val, save_file=save_file, resolution=resolution, maptype=maptype, title=title)


def grid_density_gaussian_filter(data, size, resolution=None, smoothing_window=None):
    """Smoothing grid values with a Gaussian filter.

    :param [(float, float, float)] data: list of 3-dimensional grid coordinates
    :param int size: grid size
    :param int resolution: desired grid resolution
    :param int smoothing_window: size of the gaussian kernels for smoothing

    :return: smoothed grid values
    :rtype: numpy.ndarray
    """
    resolution = resolution if resolution else size
    k = (resolution - 1) / size
    w = smoothing_window if smoothing_window else int(0.01 * resolution)    # Heuristic
    imgw = (resolution + 2 * w)
    img = np.zeros((imgw, imgw))
    for x, y, z in data:
        ix = int(x * k) + w
        iy = int(y * k) + w
        if 0 <= ix < imgw and 0 <= iy < imgw:
            img[iy][ix] += z
    z = ndi.gaussian_filter(img, (w, w))                                    # Gaussian convolution
    z[z <= BLANK_THRESH] = np.nan                                           # Making low values blank
    return z[w:-w, w:-w]


def lines_overlay(all_xs, all_ys, maptype=MAPTYPE, save_file=None):
    width = SCALE * MAX_SIZE

    group = list()
    counter = 0
    lats = list()
    lons = list()
    for i in range(len(all_xs)):
        for j in range(len(all_xs[i])):
            lats.append(all_xs[i][j])
            lons.append(all_ys[i][j])
            group.append(counter)
        counter += 1

    print(len(group))
    print(len(lats))

    img, pixels = background_and_pixels(pd.Series(lats), pd.Series(lons), MAX_SIZE, maptype)
    x = pixels['x_pixel']
    y = pixels['y_pixel']

    print(len(x))
    print(len(y))

    index = 0
    last = None
    lines_x = list()
    lines_y = list()
    curr_line_set_x = list()
    curr_line_set_y = list()
    while index < len(x):
        if last is None or last != group[index]:
            last = group[index]
            lines_x.append(curr_line_set_x)
            lines_y.append(curr_line_set_y)
            curr_line_set_x = list()
            curr_line_set_y = list()
        curr_line_set_x.append(x[index])
        curr_line_set_y.append(y[index])
        last = group[index]
        index += 1

    plt.figure(figsize=(10, 10))
    for i in range(len(lines_x)):
        plt.plot(lines_x[i], lines_y[i], '-', c='b')
       
    plt.imshow(np.array(img))
    plt.gca().invert_yaxis()                                                # Origin of map is upper left
    plt.axis([0, width, width, 0])                                          # Remove margin
    plt.axis('off')
    plt.tight_layout()
    if save_file is not None:
        fig = plt.gcf()
        fig.set_size_inches(18.5, 10.5)
        fig.savefig(save_file, dpi=300)
    plt.show()


def polygons(latitudes, longitudes, clusters, maptype=MAPTYPE):
    """Plot clusters of points on map, including them in a polygon defining their convex hull.

    :param pandas.Series latitudes: series of sample latitudes
    :param pandas.Series longitudes: series of sample longitudes
    :param pandas.Series clusters: marker clusters
    :param string maptype: type of maps, see GoogleStaticMapsAPI docs for more info

    :return: None
    """
    width = SCALE * MAX_SIZE
    img, pixels = background_and_pixels(latitudes, longitudes, MAX_SIZE, maptype)

    # Building collection of polygons
    polygon_list = []
    unique_clusters = clusters.unique()
    cmap = pd.Series(np.arange(unique_clusters.shape[0] - 1, -1, -1), index=unique_clusters)
    for c in unique_clusters:
        in_polygon = clusters == c
        if in_polygon.sum() < 3:
            print('[WARN] Cannot draw polygon for cluster {} - only {} samples.'.format(c, in_polygon.sum()))
            continue
        cluster_pixels = pixels.loc[clusters == c]
        polygon_list.append(Polygon(cluster_pixels.iloc[ConvexHull(cluster_pixels).vertices], closed=True))

    # Background map
    plt.figure(figsize=(10, 10))
    ax = plt.subplot(111)
    plt.imshow(np.array(img))
    # Collection of polygons
    p = PatchCollection(polygon_list, cmap='jet', alpha=0.25)
    p.set_array(cmap.values)
    ax.add_collection(p)
    # Scatter plot
    plt.scatter(
        pixels['x_pixel'],
        pixels['y_pixel'],
        c=cmap.loc[clusters],
        cmap='jet',
        s=width / 40,
        linewidth=0,
        alpha=0.25,
    )
    # Axis options
    plt.gca().invert_yaxis()                                                # Origin of map is upper left
    plt.axis([0, width, width, 0])                                          # Remove margin
    plt.axis('off')
    plt.tight_layout()
    # Building legend box
    jet_cmap = cm.get_cmap('jet')
    plt.legend(
        [Rectangle((0, 0), 1, 1, fc=jet_cmap(i / (cmap.shape[0] - 1)), alpha=0.25) for i in cmap.values],
        cmap.index,
        loc=4,
        bbox_to_anchor=(1.1, 0),
    )
    plt.show()
