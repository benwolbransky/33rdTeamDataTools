import pandas as pd
import requests
import json
import os

class SISrequest():    
    def __init__(self, client_id = None, client_secret = None):
        '''client_id is your client id, client secret is the API key from SIS'''
        
        c_id = os.environ.get('T33C_ID')
        if (client_secret is None):
            if (os.environ.get('T33API_KEY') is None):
                raise Exception("No API Key provided")
            else:
                api_key = api_key = os.environ.get('T33API_KEY')
        else:
            api_key = client_secret

        if (client_id is None):
            if (c_id is None):
                raise Exception("No API Key provided")
            c_id = c_id
        else:
            c_id = client_id

        AUTH_URL = 'https://auth.sportsinfosolutions.com/connect/token'
        self.cache = {}
        post_obj = {'client_id': str(c_id), #put your email address here
        'client_secret': str(api_key), #put your API secret key here
        'grant_type': 'client_credentials', #dont change
        'scope': 'sisapi'} #dont change

        x = requests.post(AUTH_URL, data = post_obj)

        print("Connected" if x.status_code == 200 else "Did not connect")
        token = 'Bearer '+json.loads(x.text)['access_token']

        self.header = {'Authorization':token}
        self.base_url = 'https://api.sportsinfosolutions.com'

    def cache_check(self, endpoint):
        
        if endpoint not in self.cache:
            data = self.endpoint(endpoint)
            self.cache[endpoint] = data
        else:
            data = self.cache[endpoint]
        return data

    def clear_cache(self):
        self.cache = {}
        
    def endpoint(self, endpoint):
        
        url = self.base_url + endpoint
        if url in self.cache:
            return self.cache[url]
        else:
            r = requests.get(url, headers = self.header) 
            if r.status_code == 200:
                df = pd.read_json(r.text)
                if len(df) == 0:
                    print(f"Endpoint {endpoint} yielded no results")
                    return None
                if (len(df.columns) == 1) & (pd.read_json(r.text).columns.values[0] == 'data'):
                    self.cache[url] = df['data'].apply(pd.Series)
                    return self.cache[url]
                else:
                    self.cache[url] = df
                    return df
            else:
                print(f"Access to Endpoint {endpoint} Failed")
                return None

    def get_Data(self,seasons = range(2016,2022)):
        self.EventTypes = pd.DataFrame()
        self.Teams = pd.DataFrame()
        self.Desc = pd.DataFrame()
        self.Passing = pd.DataFrame()
        self.Receiving = pd.DataFrame()
        self.Rushing = pd.DataFrame()
        self.PBP = pd.DataFrame()
        self.Players = pd.DataFrame()
        
        self.EventTypes = self.cache_check("/api/v1/nfl/EventTypes")
        self.Desc = self.cache_check(f"/api/v1/nfl/game/playdescription")

        for season in seasons:
            PBP = self.cache_check(f'/api/v1/nfl/standard/events/{season}/')
            Teams = self.cache_check(f"/api/v1/nfl/seasons/{season}/teams")
            Passing = self.cache_check(f"/api/v1/nfl/standard/passing/{season}/")
            Rushing = self.cache_check(f"/api/v1/nfl/standard/rushing/{season}")
            Receiving = self.cache_check(f'/api/v1/nfl/standard/receiving/{season}')
            Players = self.cache_check(f"/api/v1/nfl/seasons/{season}/players")
            Players['season'] = season
            
            self.Teams = pd.concat([self.Teams,Teams])
            self.Passing = pd.concat([self.Passing,Passing])
            self.Rushing = pd.concat([self.Rushing,Rushing])
            self.PBP = pd.concat([self.PBP,PBP])
            self.Receiving = pd.concat([self.Receiving,Receiving])
            self.Players = pd.concat([self.Players,Players])

    def endpoint_cat(self, endpoint, seasons = range(2016,2022), weeks = range(1,23)):
        if 'week' in endpoint:
            weekly = True
        else: 
            weekly = False

        if 'season' in endpoint:
            seasonly = True
        else:
            seasonly = False

        df = pd.DataFrame()

        if (not seasonly) and (not weekly): 
            print("no season or weeks to run through")

        if seasonly and weekly:
            for season in seasons:
                for week in weeks:
                    req = self.cache_check(endpoint.format(season=season, week = week))

            
        for season in seasons:
            req = self.cache_check(endpoint.format(season=season))
            
            df = pd.concat([df,req])

        return df 
        
    def get_pbp(self):
        if not hasattr(self,'Players'):
            print("GETTING ATTRS")
            self.get_Data()
        
        df = self.PBP[['season','week','gameId','eventId','eventType','offensiveTeamId','defensiveTeamId','homeTeamId','quarter','timeLeft','down','toGo',
        'startYard','homeTeamScore','awayTeamScore','scoreGap', 'firstDown', 'fieldGoal',
        'touchdown', 'extraPoint', 'twoPtConversion', 'twoPtReturn', 'safety', 'turnover', 'yardageNegated']]

        df = df.merge(self.EventTypes, left_on = 'eventType', right_on = 'eventType', how = 'left').drop('eventType', axis = 1).rename(columns ={'descr':"eventType"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','offensiveTeamId'], right_on = ['season','teamId'], how = 'left').drop(['teamId','offensiveTeamId'], axis = 1).rename(columns ={'abbr':"offTeamAbbr"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','defensiveTeamId'], right_on = ['season','teamId'], how = 'left').drop(['defensiveTeamId','teamId'], axis = 1).rename(columns ={'abbr':"defTeamAbbr"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','homeTeamId'], right_on = ['season','teamId'], how = 'left').drop(['homeTeamId','teamId'], axis = 1).rename(columns ={'abbr':"homeTeamAbbr"})
        df = df.merge(self.Desc, on = ['gameId','eventId'], how = 'left').rename(columns ={'extraNote':"playDesc"})
        df = df.merge(self.Passing[['gameId','eventId','playerId','completed','intercepted','yards','throwDepth']], on = ['gameId','eventId'], how = 'left').rename(columns ={'yards':"passYards", "playerId":"qbId"})
        df = df.merge(self.Rushing[['gameId','eventId','rpo','yards','playerId']], on = ['gameId','eventId'], how = 'left').rename(columns ={'yards':"rushYards",'playerId':'rusherId'})
        df = df.merge(self.Receiving[['gameId','eventId','playerId']], on = ['gameId','eventId'], how = 'left').rename(columns ={'playerId':'receiverId'})
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','qbId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"qbName"}).drop(['playerId'], axis = 1, errors = 'ignore')
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','receiverId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"receiverName"}).drop(['playerId'], axis = 1, errors = 'ignore')
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','rusherId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"rusherName"}).drop(['playerId'], axis = 1, errors = 'ignore')
        return df

    def get_pbp_advanced(self):
        if not hasattr(self,'Players'):
            self.get_Data()
            
        df = self.PBP
        pbpcols = set(self.PBP.columns)
        passcols = set(self.Passing.columns)
        rushcols = set(self.Rushing.columns)
        reccols = set(self.Receiving.columns)
        universal_cols = ['eventId','gameId','gsisGameId','gsisPlayId','lastUpdate','nflGameId','scoreType','season','week']

        dfs = [self.PBP,self.Passing,self.Rushing,self.Receiving]

        for df1 in dfs:
            df1.drop(['scoreType','lastUpdate'], errors = 'ignore', inplace=True)

        df = df.merge(self.EventTypes, left_on = 'eventType', right_on = 'eventType', how = 'left').drop('eventType', axis = 1).rename(columns ={'descr':"eventType"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','offensiveTeamId'], right_on = ['season','teamId'], how = 'left').drop(['teamId','offensiveTeamId'], axis = 1).rename(columns ={'abbr':"offTeamAbbr"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','defensiveTeamId'], right_on = ['season','teamId'], how = 'left').drop(['defensiveTeamId','teamId'], axis = 1).rename(columns ={'abbr':"defTeamAbbr"})
        df = df.merge(self.Teams[['season','teamId','abbr']], left_on = ['season','homeTeamId'], right_on = ['season','teamId'], how = 'left').drop(['homeTeamId','teamId'], axis = 1).rename(columns ={'abbr':"homeTeamAbbr"})
        df = df.merge(self.Desc, on = ['gameId','eventId'], how = 'left').rename(columns ={'extraNote':"playDesc"})
        df = df.merge(self.Passing, on = ['season','week','gameId','eventId'], how = 'left', suffixes=('','Passer')).rename(columns ={'yards':"passYards", "playerId":"qbId"})
        df = df.merge(self.Rushing, on = ['season','week','gameId','eventId'], how = 'left', suffixes=('','Rusher')).rename(columns ={'yards':"rushYards",'playerId':'rusherId'})
        df = df.merge(self.Receiving, on = ['season','week','gameId','eventId'], how = 'left', suffixes=('', 'Receiver')).rename(columns ={'playerId':'receiverId'})
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','qbId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"qbName"}).drop(['playerId','qbId'], axis = 1, errors = 'ignore')
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','receiverId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"receiverName"}).drop(['playerId','receiverId'], axis = 1, errors = 'ignore')
        df = df.merge(self.Players[['season','playerId','fullName']], left_on=['season','rusherId'], right_on=['season','playerId'], how = 'left').rename(columns ={'fullName':"rusherName"}).drop(['playerId','rusherId'], axis = 1, errors = 'ignore')
        return df
