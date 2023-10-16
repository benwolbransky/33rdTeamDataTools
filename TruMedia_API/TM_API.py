import pandas as pd
import requests as r
import json
import os

from config import EMAIL, TOKEN, DEFAULT_SEASON

class TMrequest:    
    def __init__(self):
        
       # Imports and set up for TruMedia API.
        self.cache = {} # Cache to speed up existing requests
        
        # Get session token 
        headers = {'Content-Type':'application/json'}
        data = '{{"username":"{email}", "sitename":"33rdteam", "token":"{token}"}}'.format(email = EMAIL, token = TOKEN)

        res = r.post('https://api.trumedianetworks.com/v1/siteadmin/api/createTempPBToken', data = data, headers = headers)
        self.tok = json.loads(res.content)['pbTempToken'] #session api token
        
        # base url for all API requests
        self.base_url = "https://api.trumedianetworks.com/v1/nflapi/customQuery/{query}.csv?token={tok}"
    def clear_cache(self):
        """
        Clear cache in the event of pulling live data
        """
        self.cache = {}

    def check_cache_request(self, url):
        '''
        Retreive cached API calls
        :param url: A URL to check. If previous cached, return the cache, otherwise, request from the API

        :return: API request
        '''
        if url in self.cache: #check if URL is in Cache
            return self.cache[url]
        else: # if not, fetch the URL request
            req = pd.read_csv(url)
            self.cache[url] = req
            return req

    def _list_to_cols(self, list_of_cols = None):
        """
        Convert a list of columns to a usable string for the TM API
        :param list_of_cols: A list of Columns in the format ['Col1','Col2,'Col3']

        :return: a string of columns  "[Col1],[Col2],[Col3]"
        """
        
        api_column_list = ["[" + i + "]" for i in list_of_cols] # List coprehension for propper formatting
        api_columns = ','.join(api_column_list) # comma spliced
        return api_columns

    
    def team_games(self, seasons = [DEFAULT_SEASON], seasonTypes = ['REG'], cols = None, team=None, **kwargs):
        """
        Make a request from the Team Games endpoint. For each TEAM, return stats aggregated at a GAME level, as per the filters.

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param seasons: list or a single season
        :param seasonTypes: list or a single season type. One or multiple of ['REG','PLY','PRE']
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652

        :return: pandas dataframe from TMAPI
        """
        # check for named cols
        if cols is None:
            raise ValueError('Please provide columns')
        elif isinstance(cols, list):
            cols = self._list_to_cols(cols)

        # assign team plays base url
        base_url = self.base_url.format(query = 'TeamGames', tok = self.tok)
        
        # Check provided seasons to ensure proper formatting
        if hasattr(seasons, "__iter__"):
            try:
                seasons = [int(i) for i in seasons]
                logic = [len(str(i))==4 for i in seasons]
                if all(logic):
                    pass
            except:
                raise ValueError('Seasons provided are not in "YYYY" format')
        elif (len(str(seasons)) == 4) & (str(seasons).isdigit()):
            seasons = [int(seasons)]
        else:
            raise ValueError('Please provide a list of seasons or a single season')

        # check seasonType to ensure proper formatting
        valid_types = {'PLY','REG','PRE'}
        
        if not isinstance(seasonTypes, list):
            seasonTypes = [seasonTypes]
        
        if hasattr(seasonTypes, "__iter__"):
            #convert to set
            type_set = set(seasonTypes)
        else:
            raise ValueError('Season Type not any of ["REG","PLY","PRE"]')
        
        if not type_set <= valid_types:
            raise ValueError(f'Season Type {type_set} not exclusive of ["REG","PLY","PRE"]')

        seasonTypes = list(type_set)

        urls = []
        for season in seasons:
            for seasonType in seasonTypes:
                urls.append(
                    base_url + ''.join(f"&seasonYear={season}" + f"&seasonType={seasonType}" + f"&columns={cols}")
                )

        df = pd.DataFrame()

        for url in urls:
            req = self.check_cache_request(url)
            df = pd.concat([df,req])
        return df

    def player_games(self, seasons = [DEFAULT_SEASON], seasonTypes = ['REG'], cols = None, team=None, **kwargs):
        """
        Make a request from the Player Games endpoint. For each PLAYER, return stats aggregated at a GAME level, per the filters.

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param seasons: list or a single season
        :param seasonTypes: list or a single season type. One or multiple of ['REG','PLY','PRE']
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652

        :return: pandas dataframe from TMAPI
        """
        # check for named cols
        if cols is None:
            raise ValueError('Please provide columns')
        elif isinstance(cols, list):
            cols = cols + ['Position','GameStatus']
            cols = self._list_to_cols(cols)

        # assign team plays base url
        base_url = self.base_url.format(query = 'PlayerGames', tok = self.tok)
        
        # Check provided seasons to ensure proper formatting
        if hasattr(seasons, "__iter__"):
            try:
                seasons = [int(i) for i in seasons]
                logic = [len(str(i))==4 for i in seasons]
                if all(logic):
                    pass
            except:
                raise ValueError('Seasons provided are not in "YYYY" format')
        elif (len(str(seasons)) == 4) & (str(seasons).isdigit()):
            seasons = [int(seasons)]
        else:
            raise ValueError('Please provide a list of seasons or a single season')

        # check seasonType to ensure proper formatting
        valid_types = {'PLY','REG','PRE'}
        
        if not isinstance(seasonTypes, list):
            seasonTypes = [seasonTypes]
        
        if hasattr(seasonTypes, "__iter__"):
            #convert to set
            type_set = set(seasonTypes)
        else:
            raise ValueError('Season Type not any of ["REG","PLY","PRE"]')
        
        if not type_set <= valid_types:
            raise ValueError(f'Season Type {type_set} not exclusive of ["REG","PLY","PRE"]')

        seasonTypes = list(type_set)

        urls = []
        for season in seasons:
            for seasonType in seasonTypes:
                urls.append(
                    base_url + ''.join(f"&seasonYear={season}" + f"&seasonType={seasonType}" + f"&columns={cols}")
                )

        df = pd.DataFrame()

        for url in urls:
            req = self.check_cache_request(url)
            df = pd.concat([df,req])
        return df

    def team_seasons(self, seasons = [DEFAULT_SEASON], seasonType = None, cols = None, team=None, **kwargs):
        """
        Make a request from the Team Seasons endpoint. For each TEAM, return stats aggregated at a SEASON level, as per the filters.

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param seasons: list or a single season
        :param seasonType: a single season type. Report returns 
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652

        :return: pandas dataframe from TMAPI
        """
        # check for named cols
        if cols is None:
            raise ValueError('Please provide columns')
        elif isinstance(cols, list):
            cols = self._list_to_cols(cols)

        # assign team plays base url
        base_url = self.base_url.format(query = 'TeamSeasons', tok = self.tok)

        # Check provided seasons to ensure proper formatting
        if hasattr(seasons, "__iter__"):
            try:
                seasons = [int(i) for i in seasons]
                logic = [len(str(i))==4 for i in seasons]
                if all(logic):
                    pass
            except:
                raise ValueError('Seasons provided are not in "YYYY" format')
        elif (len(str(seasons)) == 4) & (str(seasons).isdigit()):
            seasons = [int(seasons)]
        else:
            raise ValueError('Please provide a list of seasons or a single season')

        # check seasonType to ensure proper formatting
        valid_types = {'PLY','REG','PRE', None}
        
        if seasonType is None:
            pass
        elif not isinstance(seasonType, list):
            seasonType = [seasonType]
        
        if hasattr(seasonType, "__iter__"):
            #convert to set
            type_set = set(seasonType)
        elif seasonType is None:
            pass
        else:
            raise ValueError('Season Type not any of ["REG","PLY","PRE"]')
        
        if seasonType is None:
            pass
        elif not type_set <= valid_types:
            raise ValueError(f'Season Type {type_set} not exclusive of ["REG","PLY","PRE"]')

        urls = []
        df = pd.DataFrame()
        
        if seasonType is None:
            for season in seasons:
                urls.append(
                    base_url + ''.join(f"&seasonYear={season}" + f"&columns={cols}")
                )
        else:
            seasonTypes = list(type_set)

            for season in seasons:
                for seasonType in seasonTypes:
                    urls.append(
                        base_url + ''.join(f"&seasonYear={season}" + f"&seasonType={seasonType}" + f"&columns={cols}")
                    )

        for url in urls:
            req = self.check_cache_request(url)
            df = pd.concat([df,req])
        return df


    def player_seasons(self, seasons = [DEFAULT_SEASON], seasonType = None, cols = None, team=None, **kwargs):
        """
        Make a request from the Player Seasons endpoint. For each PLAYER, return stats aggregated at a SEASON level, as per the filters.

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param seasons: list or a single season
        :param seasonType: a single season type. Report returns 
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652

        :return: pandas dataframe from TMAPI
        """
        # check for named cols
        if cols is None:
            raise ValueError('Please provide columns')
        elif isinstance(cols, list):
            cols = cols + ['Position']
            cols = self._list_to_cols(cols)

        # assign team plays base url
        base_url = self.base_url.format(query = 'PlayerSeasons', tok = self.tok)

        # Check provided seasons to ensure proper formatting
        if hasattr(seasons, "__iter__"):
            try:
                seasons = [int(i) for i in seasons]
                logic = [len(str(i))==4 for i in seasons]
                if all(logic):
                    pass
            except:
                raise ValueError('Seasons provided are not in "YYYY" format')
        elif (len(str(seasons)) == 4) & (str(seasons).isdigit()):
            seasons = [int(seasons)]
        else:
            raise ValueError('Please provide a list of seasons or a single season')

        # check seasonType to ensure proper formatting
        valid_types = {'PLY','REG','PRE', None}
        
        if seasonType is None:
            pass
        elif not isinstance(seasonType, list):
            seasonType = [seasonType]
        
        if hasattr(seasonType, "__iter__"):
            #convert to set
            type_set = set(seasonType)
        elif seasonType is None:
            pass
        else:
            raise ValueError('Season Type not any of ["REG","PLY","PRE"]')
        
        if seasonType is None:
            pass
        elif not type_set <= valid_types:
            raise ValueError(f'Season Type {type_set} not exclusive of ["REG","PLY","PRE"]')

        urls = []
        df = pd.DataFrame()
        
        if seasonType is None:
            for season in seasons:
                urls.append(
                    base_url + ''.join(f"&seasonYear={season}" + f"&columns={cols}")
                )
        else:
            seasonTypes = list(type_set)

            for season in seasons:
                for seasonType in seasonTypes:
                    urls.append(
                        base_url + ''.join(f"&seasonYear={season}" + f"&seasonType={seasonType}" + f"&columns={cols}")
                    )
        for url in urls:
            req = self.check_cache_request(url)
            df = pd.concat([df,req])
        return df

    def _custom(self, endpoint = None, **kwargs):
        """
        Function I used for debugging

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param seasons: list or a single season
        :param seasonType: a single season type. Report returns 
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652

        :return: pandas dataframe from TMAPI
        """
        valid_endpoints = {'PlayerGames','PlayerPlays','PlayerSeasons','TeamGames','TeamPlays','TeamSeasons'}

        if set([endpoint]) <= valid_endpoints:
            pass
        else:
            raise ValueError(f'{endpoint} is not a valid endpoint')

        base_url = self.base_url.format(query = endpoint, tok = self.tok)

        print(base_url)

    def player_plays(self, seasons = [DEFAULT_SEASON], seasonType = None, cols = None, team=None, statEvent="Snaps", print_urls = False, **kwargs):
        """
        Make a request from the Player plays endpoint. For each PLAYER, return stats from each PLAY, one line per statEvent

        :param cols: list of columns (['Att','Comp','PsYds']) or a python string in proper format ("[Att],[Comp],[PsYds]")
        :param statEvent: string containing the stat event to get data for. Valid events include "Snap","Rec","Att",'Tar','Rush' etc. 
        :param seasons: list or a single season
        :param seasonType: a single season type. Report returns all by default.
        :param kwargs: any additional filters. For example gsisGameKey=2022082652 will return only data from game 2022082652
        :param print_urls: default False. Prints the URL for debugging

        :return: pandas dataframe from TMAPI
        """
        # check for named cols
        if cols is None:
            raise ValueError('Please provide columns')
        elif isinstance(cols, list):
            cols = cols + ['Position']
            cols = self._list_to_cols(cols)

        # assign team plays base url
        base_url = self.base_url.format(query = 'PlayerPlays', tok = self.tok)

        # Check provided seasons to ensure proper formatting
        if hasattr(seasons, "__iter__"):
            try:
                seasons = [int(i) for i in seasons]
                logic = [len(str(i))==4 for i in seasons]
                if all(logic):
                    pass
            except:
                raise ValueError('Seasons provided are not in "YYYY" format')
                
        elif (len(str(seasons)) == 4) & (str(seasons).isdigit()):
            seasons = [int(seasons)]
        else:
            raise ValueError('Please provide a list of seasons or a single season')

        # check seasonType to ensure proper formatting
        valid_types = {'PLY','REG','PRE', None}
        
        if seasonType is None:
            pass
        elif not isinstance(seasonType, list):
            seasonType = [seasonType]
        
        if hasattr(seasonType, "__iter__"):
            #convert to set
            type_set = set(seasonType)
        elif seasonType is None:
            pass
        else:
            raise ValueError('Season Type not any of ["REG","PLY","PRE"]')
        
        if seasonType is None:
            pass
        elif not type_set <= valid_types:
            raise ValueError(f'Season Type {type_set} not exclusive of ["REG","PLY","PRE"]')

        urls = []
        df = pd.DataFrame()
        
        if seasonType is None:
            for season in seasons:
                urls.append(
                    base_url + ''.join(f"&seasonYear={season}" + f"&columns={cols}" + f"&statEvent=[{statEvent}]")
                )
        else:
            seasonTypes = list(type_set)

            for season in seasons:
                for seasonType in seasonTypes:
                    urls.append(
                        base_url + ''.join(f"&seasonYear={season}" + f"&seasonType={seasonType}" + f"&columns={cols}" + f"&statEvent=[{statEvent}]")
                    )
        if print_urls == True:
            print(urls)
        else:
            for url in urls:
                req = self.check_cache_request(url)
                df = pd.concat([df,req])
            return df

    
        