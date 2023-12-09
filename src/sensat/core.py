import os
import re
import zipfile
import argparse
import numpy as np
import pandas as pd
from cdsetool.query import query_features
from cdsetool.download import download_feature
from cdsetool.credentials import Credentials
from datetime import date, datetime





class SenSat:



    def __init__(self, username, password, tiles, level='1C', start='20150523', end=datetime.today().strftime('%Y%m%d'),
            maxcloud=100, minsize=25., output_dir=os.getcwd(), remove=False) -> None:
        """
        Download Sentinel-2 data from the Copernicus Open Access Hub, specifying a particular tile, date ranges and degrees
        of cloud cover. This is the function that is initiated from the command line.

        Args:
            username: cdse username. Sign up at https://identity.dataspace.copernicus.eu/auth.
            password: cdse password.
            tiles: A string containing the name of the tile to to download, or a list of tiles.
            level: Download level '1C' (default) or '2A' data.
            start: Start date for search in format YYYYMMDD. Defaults to '20150523'.
            end: End date for search in format YYYYMMDD. Defaults to today's date.
            maxcloud: An integer of maximum percentage of cloud cover to download. Defaults to 100 %% (download all images, regardless of cloud cover).
            minsize: A float with the minimum filesize to download in MB. Defaults to 25 MB.  Be aware, file sizes smaller than this can result sen2three crashing.
            output_dir: Optionally specify an output directory. Defaults to the present working directory.
            remove: Boolean value, which when set to True deletes level 1C .zip files after decompression is complete. Defaults to False.
        """

        # Connect to API
        self._connectToAPI(username, password)

        # Allow download of single tile
        if type(tiles) == str: tiles = [tiles]

        for tile in tiles:

            # Search for files, return a data frame containing details of matching Sentinel-2 images
            products = self._search(tile, level=level, start=start, end=end, maxcloud=maxcloud, minsize=minsize)

            # Where no data
            if len(products) == 0: continue

            # If folder doesn't exist, then create it.
            if not os.path.exists('%s/%s' % (output_dir, tile)):
                os.makedirs('%s/%s' % (output_dir, tile))

            # Download products
            zip_files = self._download(products, output_dir='%s/%s' % (output_dir, tile))

            # Decompress data
            self._decompress(zip_files, output_dir='%s/%s' % (output_dir, tile), remove=remove)
        
        return None


    def _removeZip(self, zip_file):
        """
        Deletes Level 1C .zip file from disk.

        Args:
            #A Sentinel-2 level 1C .zip file from Copernicus Open Access Data Hub.
        """

        assert '_MSIL1C_' in zip_file, "removeZip function should only be used to delete Sentinel-2 level 1C compressed .SAFE files"
        assert zip_file.split('/')[-1][
            -4:] == '.zip', "removeL1C function should only be used to delete Sentinel-2 level 1C compressed .SAFE files"

        os.remove(zip_file)


    def _validateTile(self, tile):
        '''
        Validate the name structure of a Sentinel-2 tile. This tests whether the input tile format is correct.

        Args:
            tile: A string containing the name of the tile to to download.
        '''

        # Tests whether string is in format ##XXX
        name_test = re.match("[0-9]{2}[A-Z]{3}$", tile)

        return bool(name_test)


    def _connectToAPI(self, username, password):
        '''
        Connect to the cdse API with sentinelsat.

        Args:
            username: cdse username. Sign up at https://identity.dataspace.copernicus.eu/auth.
            password: cdse password.
        '''

        # Let API be accessed by other functions
        # The Copernicus Data Space Ecosystem OpenSearch API
        global cdse_api

        # Connect to Sentinel API
        cdse_api = Credentials(username, password)
    

    def _format_query_date(self, in_date):
        r"""
        Format a date, datetime or a YYYYMMDD string input as YYYY-MM-DDThh:mm:ssZ
        or validate a date string as suitable for the full text search interface and return it.

        `None` will be converted to '\*', meaning an unlimited date bound in date ranges.

        Parameters
        ----------
        in_date : str or datetime or date or None
            Date to be formatted

        Returns
        -------
        str
            Formatted string

        Raises
        ------
        ValueError
            If the input date type is incorrect or passed date string is invalid
        """
        if in_date is None:
            return "*"
        if isinstance(in_date, (datetime, date)):
            return in_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        elif not isinstance(in_date, str):
            raise ValueError("Expected a string or a datetime object. Received {}.".format(in_date))

        in_date = in_date.strip()
        if in_date == "*":
            # '*' can be used for one-sided range queries e.g. ingestiondate:[* TO NOW-1YEAR]
            return in_date

        # Reference: https://cwiki.apache.org/confluence/display/solr/Working+with+Dates

        # ISO-8601 date or NOW
        valid_date_pattern = r"^(?:\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z|NOW)"
        # date arithmetic suffix is allowed
        units = r"(?:YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)"
        valid_date_pattern += r"(?:[-+]\d+{}S?)*".format(units)
        # dates can be rounded to a unit of time
        # e.g. "NOW/DAY" for dates since 00:00 today
        valid_date_pattern += r"(?:/{}S?)*$".format(units)
        in_date = in_date.strip()
        if re.match(valid_date_pattern, in_date):
            return in_date

        try:
            return datetime.strptime(in_date, "%Y%m%d").strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError("Unsupported date value {}".format(in_date))


    def _get_filesize(self, products_df):
        """
        Extracts file size in MB from a Sentinel products pandas dataframe.

        Args:
            products_df: A pandas dataframe from search().
        Returns:
            A numpy array with file sizes in MB.
        """

        size = [int(float(str(i).split(' ')[0])) for i in products_df['properties.services.download.size'].values]
        # suffix = [str(i).split(' ')[1].lower() for i in products_df['properties.services.download.size'].values]
        suffix = [
            str(i).split(' ')[1].lower() if len(str(i).split(' ')) > 1 else 'mb' 
            for i in products_df['properties.services.download.size'].values
        ]

        size_mb = []

        for this_size, this_suffix in zip(size, suffix):
            if this_suffix == 'kb' or this_suffix == 'kib':
                size_mb.append(this_size * 0.001)
            elif this_suffix == 'mb' or this_suffix == 'mib':
                size_mb.append(this_size * 1.)
            elif this_suffix == 'gb' or this_suffix == 'gib':
                size_mb.append(this_size * 1000.)
            else:
                size_mb.append(this_size * 0.000001)

        return np.array(size_mb)


    def _search(self, tile, level='1C', start='20150523', end=datetime.today().strftime('%Y%m%d'), maxcloud=100,
            minsize=25.):
        """search(tile, start = '20161206', end = datetime.today().strftime('%Y%m%d'),  maxcloud = 100, minsize_mb = 25.)

        Searches for images from a single Sentinel-2 Granule that meet conditions of date range and cloud cover.

        Args:
            tile: A string containing the name of the tile to to download.
            level: Download level '1C' (default) or '2A' data.
            start: Start date for search in format YYYYMMDD. Defaults to 20150523.
            end: End date for search in format YYYYMMDD. Defaults to today's date.
            maxcloud: An integer of maximum percentage of cloud cover to download. Defaults to 100 %% (download all images, regardless of cloud cover).
            minsize: A float with the minimum filesize to download in MB. Defaults to 25 MB.  Be aware, file sizes smaller than this can result sen2three crashing.

        Returns:
            A pandas dataframe with details of scenes matching conditions.
        """

        # Test that we're connected to the
        assert 'cdse_api' in globals(), "The global variable cdse_api doesn't exist. You should run connectToAPI(username, password) before searching the data archive."

        # Validate tile input format for search
        assert self._validateTile(tile), "The tile name input (%s) does not match the format ##XXX (e.g. 36KWA)." % tile

        assert level in ['1C', '2A'], "Level must be '1C' or '2A'."

        # Set up start and end dates
        startdate = self._format_query_date(start)
        enddate = self._format_query_date(end)

        products = query_features('Sentinel2', {
            'tileId': tile, 
            'startDate': startdate, 
            'completionDate': enddate,
            'productType': 'S2MSI%s' % level,
            'cloudCover': f'[0,{maxcloud}]'
        })
        # convert to Pandas DataFrame, which can be searched modified before commiting to download()
        # Flatten the nested JSON object
        flat_json = pd.json_normalize(products)
        products_df = pd.DataFrame(flat_json)

        # Where no results for tile
        if len(products_df) == 0: return products_df

        products_df['filesize_mb'] = self._get_filesize(products_df)

        products_df = products_df[products_df['filesize_mb'] >= float(minsize)]

        print('Found %s matching images for tile: %s' % (str(len(products_df)), tile))

        return products_df
    

    def _convert_to_nested_dict(self, flat_dict):
        nested_dict = {}
        for key, value in flat_dict.items():
            keys = key.split('.')
            current_dict = nested_dict

            for k in keys[:-1]:
                current_dict = current_dict.setdefault(k, {})
            current_dict[keys[-1]] = value

        return nested_dict


    def _download(self, products_df, output_dir=os.getcwd()):
        ''' download(products_df, output_dir = os.getcwd())

        Downloads all images from a dataframe produced by sentinelsat.

        Args:
            products_df: Pandas dataframe from search() function.
            output_dir: Optionally specify an output directory. Defaults to the present working directory.
        '''

        assert os.path.isdir(output_dir), "Output directory doesn't exist."

        if products_df.empty == True:
            print ('WARNING: No products found to download. Check your search terms.')
            raise

        else:

            downloaded_files = []

            # for uuid, filename in products_df['filename']:
            for index, feature in products_df.iterrows():
                filename = feature.get("properties.title")

                if os.path.exists('%s/%s' % (output_dir, filename[:-5] + '.zip')):
                    print ('Skipping file %s, as it has already been downloaded in the directory %s. If you want to re-download it, delete it and run again.' % (
                    filename, output_dir))

                    downloaded_files.append(('%s/%s' % (output_dir.rstrip('/'), filename)).replace('.SAFE', '.zip'))

                elif os.path.exists('%s/%s' % (output_dir, filename)):
                    print ('Skipping file %s, as it has already been downloaded and extracted in the directory %s. If you want to re-download it, delete it and run again.' % (
                    filename, output_dir))

                else:

                    try:
                        # Download selected product
                        print ('Downloading %s...' % filename)
                        idx = download_feature(self._convert_to_nested_dict(feature.to_dict()), output_dir, {"credentials": cdse_api})

                        downloaded_files.append(('%s/%s' % (output_dir.rstrip('/'), filename)).replace('.SAFE', '.zip'))
                    except:
                        continue

        return downloaded_files


    def _decompress(self, zip_files, output_dir=os.getcwd(), remove=False):
        '''decompress(zip_files, output_dir = os.getcwd(), remove = False

        Decompresses .zip files downloaded from cdse, and optionally removes original .zip file.

        Args:
            zip_files: A list of .zip files to decompress.
            output_dir: Optionally specify an output directory. Defaults to the present working directory.
            remove: Boolean value, which when set to True deletes level 1C .zip files after decompression is complete. Defaults to False.
        '''

        if type(zip_files) == str: zip_files = [zip_files]

        for zip_file in zip_files:
            assert zip_file[-4:] == '.zip', "Files to decompress must be .zip format."

        # Decompress each zip file
        for zip_file in zip_files:

            # Skip those files that have already been extracted
            if os.path.exists('%s/%s' % (output_dir, zip_file.split('/')[-1].replace('.zip', '.SAFE'))):
                print ('Skipping extraction of %s, as it has already been extracted in directory %s. If you want to re-extract it, delete the .SAFE file.' % (
                zip_file, output_dir))

            else:
                print ('Extracting %s' % zip_file)
                if zipfile.is_zipfile(zip_file):
                    with zipfile.ZipFile(zip_file) as obj:
                        obj.extractall(output_dir)
                else:
                    print('********** Could not extract the zip file: %s' % zip_file)
                    print('********** Try to remove bad the zip file: %s' % zip_file)
                    self._removeZip(zip_file)

                # Delete zip file
                if remove: self._removeZip(zip_file)


def main():
    s2_tiles = ['39STD', '39STC', '39STB', '38SQJ', '38SQH', 
             '38SQG', '39SUC', '39SUB', '39SUA', '39SWA', 
             '39SWV', '39SVB', '39SVA', '39SXA', '39SXV', 
             '39SYB', '39SYA', '39SYV', '40SBG', '40SBF', 
             '40SCH', '40SCG', '40SCF', '40SDH', '40SDG']
    parser = argparse.ArgumentParser(description="Download Sentinel-2 data from the Copernicus Open Access Hub")

    parser.add_argument("-u", "--username", type=str, help="cdse username. Sign up at https://identity.dataspace.copernicus.eu/auth")
    parser.add_argument("-p", "--password", type=str, help="cdse password")
    parser.add_argument("-t", "--tiles", nargs='+', help="Tile name(s) to download", default=s2_tiles)
    parser.add_argument("-l", "--level", type=str, default="1C", help="Download level '1C' (default) or '2A' data")
    parser.add_argument("-s", "--start", type=str, default="20150523", help="Start date for search in format YYYYMMDD. Defaults to '20150523'")
    parser.add_argument("-e", "--end", type=str, default=datetime.today().strftime('%Y%m%d'),
                        help="End date for search in format YYYYMMDD. Defaults to today's date")
    parser.add_argument("-c", "--maxcloud", type=int, default=100, help="Maximum percentage of cloud cover to download. Defaults to 100")
    parser.add_argument("-m", "--minsize", type=float, default=25.0, help="Minimum filesize to download in MB. Defaults to 25.0 MB")
    parser.add_argument("-d", "--output-dir", type=str, default=os.getcwd(), help="Output directory. Defaults to the present working directory")
    parser.add_argument("-r", "--remove", action="store_true", help="Delete level 1C .zip files after decompression is complete")

    args = parser.parse_args()

    obj = SenSat(args.username, args.password, args.tiles, args.level, args.start, args.end, 
        args.maxcloud, args.minsize, args.output_dir, args.remove)