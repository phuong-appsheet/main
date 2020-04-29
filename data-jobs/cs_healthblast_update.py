#import libraries
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
import requests
from requests.auth import HTTPBasicAuth
from mixpanel import Mixpanel
import time
import json 
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from apiclient.discovery import build
import base64
import urllib
import gspread  
import os
from intercom.client import Client
import pickle
import matplotlib.pyplot as plt
import numpy as np
import statistics
from pt_utils.utils import send_email



  
#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
mp_user = Mixpanel(os.environ.get('MIXPANEL_KEY_USER'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')
intercom_key = os.environ.get('INTERCOM_KEY')
intercom = Client(personal_access_token=intercom_key)



def creator_pull():

    """
    Pull app creators' info in Mixpanel.
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    api_creator_secret: str
        Client secret used to make calls to Mixpanel Creator Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe app creators info in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
                api_creator_secret,
                people=People({
                    'user_selectors': [{
                    }

                ]

                })
            ).group_by(
                keys=[
                    "e.properties.$email",
                    "e.properties.$username",
                    "e.properties.hs_owner"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    user_id_list = []
    hs_owner_list = []
    for row in query.send():
        if row['key'][0] is not None:
            email_list.append(row['key'][0])
            user_id_list.append(int(row['key'][1]))
            hs_owner_list.append(row['key'][2])

    #create dataframe 
    data = {'email':email_list, 'user_id': user_id_list, 'hs_owner': hs_owner_list}
    df_creators = pd.DataFrame(data=data)
    
    #only keep users with HubSpot owner field. Indicating that it exists in hubspot
    df_creators = df_creators[~df_creators.hs_owner.isnull()]
    
    return df_creators



def Editor_pull(from_date, to_date):

    """
    Pull app creator Editor events in Mixpanel.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: str
        Client secret used to make calls to Mixpanel Creator Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains user IDs, Editor event datetime, and Editor event count for app creators in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_creator_secret,
        events=Events({
                'event_selectors': [{'event': "Editor"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #store user IDs, Editor count, and Editor datetime
    user_id_list = []
    datetime_list = []
    Editor_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            Editor_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list,'Editor_datetime': datetime_list, 'Editor': Editor_list}
    df_Editor = pd.DataFrame(data)
    
    return df_Editor



def AppStart_pull(from_date, to_date):

    """
    
    Pull app creator AppStart events in Mixpanel.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: str
        Client secret used to make calls to Mixpanel Creator Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains user IDs, AppStart event datetime, and AppStart event count for app creators in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_creator_secret,
        events=Events({
                'event_selectors': [{'event': "AppStart"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #store user IDs, AppStart count, and AppStart datetime
    user_id_list = []
    datetime_list = []
    AppStart_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            AppStart_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list,'AppStart_datetime': datetime_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)
    
    return df_AppStart



def user_appstart_pull(from_date, to_date):
    
    """

    Pull app user AppStart events in Mixpanel.



    Parameters
    ----------

    from_date: date
        Start date of query.

    to_date: date
        End date of query.


    Global Variables
    ----------

    api_user_secret: str
        Client secret used to make calls to Mixpanel User Project.


    Returns
    ----------

    df_user_AppStart: dataframe
        Dataframe contains app creator user ID and number of app users.



    """

    #generate JQL query
    query = JQL(
        api_user_secret,
        events=Events({
                'event_selectors': [{'event': "AppStart"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "e.properties.zAppOwnerId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record app user IDs, app creator IDs, app name, and number of AppStarts
    app_user_id_list = []
    owner_id_list = []
    AppStart_list = []
    date_list = []

    #process query results
    for row in query.send():
        if (row['key'][0] is not None) & (row['key'][1] is not None):
            app_user_id_list.append(int(row['key'][0]))
            owner_id_list.append(int(row['key'][1]))
            date_list.append(datetime.strptime(row['key'][2][:10], '%Y-%m-%d'))
            AppStart_list.append(row['value'])

    #generate email
    data = {'date': date_list, 'app_user_id':app_user_id_list,'user_id': owner_id_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)
    
    #make sure IDs are valid
    df_AppStart = df_AppStart[(df_AppStart.app_user_id>1)&(df_AppStart.user_id>1)]
    
    #remove duplicate users on the same day. Since it was pulled based on timestamp, AppStarts in one day can be on multiple roads
    df_AppStart = df_AppStart.drop_duplicates(['user_id', 'date','app_user_id'])

    #get total users and list of user emails for each app creator
    df_user_AppStart = df_AppStart.groupby(['user_id', 'date']).app_user_id.count().reset_index()
    df_user_AppStart = df_user_AppStart.rename(columns={'app_user_id':'num_app_users'})
    
    
    
    return df_user_AppStart

def get_rolling_average_creator_usage(df_AppStart, df_Editor, df_date_range, df_creators):
    
    """
    Calculate app creator rolling 7-days average of editor and app sessions. 
    
    Parameters
    ----------
    
    df_AppStart: dataframe
        Dataframe containing app creator user IDs, AppStart event count, and AppStart event datetime.
                
    df_Editor: dataframe
        Dataframe containing app creator user IDs, Editor event count, and Editor event datetime.
        
    df_date_range: dataframe
        Dataframe containing date range.
    
    df_creators: dataframe
        Dataframe app creators info in Mixpanel.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creator email, owner, date, and rolling 7-days average of editor and app sessions. 
           
    """

    #change event names to ActiveUsage and datetime to date to merge dataframes
    df_AppStart = df_AppStart.rename(columns = {'AppStart': 'ActiveUsage', 'AppStart_datetime': 'date'})
    df_Editor = df_Editor.rename(columns = {'Editor': 'ActiveUsage', 'Editor_datetime': 'date'})

    #merge dataframes and sum to get total number of ActiveUsage events. 
    frames = [df_AppStart, df_Editor]
    df_ActiveUsage = pd.concat(frames).groupby(['user_id', 'date']).ActiveUsage.sum().reset_index()

    #only interested in creators with owners
    df_creators = df_creators[~df_creators.email.isnull()]
    df_ActiveUsage = pd.merge(df_ActiveUsage, df_creators, on='user_id', how='left')
    df_ActiveUsage = df_ActiveUsage[~df_ActiveUsage.hs_owner.isnull()]

    #get list of all creator user IDs
    user_id_list = df_ActiveUsage.drop_duplicates('user_id').user_id.tolist()
    
    #initiate dataframe to store rolling ActiveUsage averages for creators
    data = {'date': [], 'user_id': [], 'OWNER': [], 'EMAIL': [], 'ACTIVE_USAGE_ROLLING': []}
    df_final_creator_usage = pd.DataFrame(data)

    #calculate rolling ActiveUsage averages for each creator
    for user_id in user_id_list:
        
        #filter for creator
        df_ActiveUsage_creator = df_ActiveUsage[df_ActiveUsage.user_id==user_id]
        
        #merge date range with data. Fill in 0's for dates without activity
        df_creator_ActiveUsage_by_day = pd.merge(df_date_range, df_ActiveUsage_creator[['date','ActiveUsage']], on='date', how='left').fillna(0)
        
        #calculate rolling 7-day averages. Fill in 0's for dates without 7-days worth of data.
        df_creator_ActiveUsage_by_day['ACTIVE_USAGE_ROLLING'] = df_creator_ActiveUsage_by_day.iloc[:,1].rolling(window=7).mean().fillna(0)
        
        #record contact user ID, email, and owner
        df_creator_ActiveUsage_by_day['user_id'] = int(user_id)
        df_creator_ActiveUsage_by_day['EMAIL'] = df_ActiveUsage_creator.email.tolist()[0]
        df_creator_ActiveUsage_by_day['OWNER'] = df_ActiveUsage_creator.hs_owner.tolist()[0]

        #order columns to match with main
        df_creator_ActiveUsage_by_day = df_creator_ActiveUsage_by_day[['date','user_id','OWNER','EMAIL','ACTIVE_USAGE_ROLLING']]
                            
        #concat creator dataframe to final
        frames = [df_final_creator_usage, df_creator_ActiveUsage_by_day]
        df_final_creator_usage = pd.concat(frames)

    #rename columns to match Snowflake database and only keeping columns that are needed
    df_final_creator_usage = df_final_creator_usage.rename(columns={'date':'DATE'})
    df_final_creator_usage = df_final_creator_usage[['DATE', 'user_id', 'OWNER', 'EMAIL', 'ACTIVE_USAGE_ROLLING']]

    return df_final_creator_usage


def get_rolling_average_num_app_users(df_user_AppStart, df_date_range, df_creators):
    
    """
    Calculate app creator rolling 7-days average of number of app users. 
    
    Parameters
    ----------
    
    df_user_AppStart: dataframe
        Dataframe containing app creator user IDs, date, and number of app users.
                
    df_date_range: dataframe
        Dataframe containing date range.
    
    df_creators: dataframe
        Dataframe app creators info in Mixpanel.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creator user ID, owner, date, and rolling 7-days average of number of app users. 
           
    """
        
    #only interested in creators with owners
    df_user_AppStart = pd.merge(df_user_AppStart, df_creators, on='user_id', how='left')
    df_user_AppStart = df_user_AppStart[~df_user_AppStart.hs_owner.isnull()]

    #drop any duplicate creators in list use for loop
    user_id_list = df_user_AppStart.drop_duplicates('user_id').user_id.tolist()
    
    #initiate dataframe to store rolling active_users averages for creators
    data = {'date': [], 'user_id': [], 'OWNER': [], 'ACTIVE_USER_ROLLING': []}
    df_final_active_users = pd.DataFrame(data)

    #calculate rolling averages of number of app users for each creator
    for user_id in user_id_list:
        
        #filter for creator
        df_creator_user_AppStart = df_user_AppStart[df_user_AppStart.user_id==user_id]
        
        #merge date range with data. Fill in 0's for dates without any app users
        df_creator_active_users_by_day = pd.merge(df_date_range, df_creator_user_AppStart[['date','num_app_users']], on='date', how='left').fillna(0)
        
        #calculate rolling 7-day averages. Fill in 0's for dates without 7-days worth of data.
        df_creator_active_users_by_day['ACTIVE_USER_ROLLING'] = df_creator_active_users_by_day.iloc[:,1].rolling(window=7).mean().fillna(0)
        
        #record contact user ID, email, and owner
        df_creator_active_users_by_day['user_id'] = int(user_id)
        df_creator_active_users_by_day['OWNER'] = df_creator_user_AppStart.hs_owner.tolist()[0]
        
        #order columns to match with main
        df_creator_active_users_by_day = df_creator_active_users_by_day[['date', 'user_id', 'OWNER', 'ACTIVE_USER_ROLLING']]

        #concat creator dataframe to final
        frames = [df_final_active_users, df_creator_active_users_by_day]
        df_final_active_users = pd.concat(frames)

    #change dataframe name to fit with database
    df_final_active_users = df_final_active_users.rename(columns={'date': 'DATE'})[['DATE', 'user_id', 'OWNER', 'ACTIVE_USER_ROLLING']]

    return df_final_active_users


   
def get_rolling_sum_convo(df_user_convo_aggregate, df_date_range):

    """
    
    Calculate app creator 7-days rolling sum of number of support conversations.


    Parameters
    ----------
    
    df_user_convo_aggregate: dataframe
        Dataframe containing creator emails, owners, dates, and number of support conversations.
    
    df_date_range: dataframe
        Dataframe containing date range.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains creator email, date, owner, and 7-days rolling sum of number of support conversations.
           
    """

    #get email list to loop through
    email_list = df_user_convo_aggregate.drop_duplicates('email').email.tolist()

    #initiate dataframe to store rolling sum of support conversations
    data = {'date': [], 'EMAIL': [], 'NUM_CONVO_ROLLING': []}
    df_final_user_convo = pd.DataFrame(data)

    #calculate rolling sum of number of support conversations for each creator
    for email in email_list:
        
        #filter for creator
        df_creator_user_convo_aggregate = df_user_convo_aggregate[df_user_convo_aggregate.email==email]
        
        #merge date range with data. Fill in 0's for dates without any support conversations
        df_creator_user_convo_aggregate_by_day = pd.merge(df_date_range, df_creator_user_convo_aggregate[['date','convo']], on='date', how='left').fillna(0)
        
        #calculate rolling 7-day sums. Fill in 0's for dates without 7-days worth of data.
        df_creator_user_convo_aggregate_by_day['NUM_CONVO_ROLLING'] = df_creator_user_convo_aggregate_by_day.iloc[:,1].rolling(window=7).sum().fillna(0)
        
        #record contact email and owner
        df_creator_user_convo_aggregate_by_day['EMAIL'] = df_creator_user_convo_aggregate.email.tolist()[0]

        #organize column order to match with main
        df_creator_user_convo_aggregate_by_day[['date', 'EMAIL', 'NUM_CONVO_ROLLING']]
        
        #concat creator dataframe to final
        frames = [df_final_user_convo, df_creator_user_convo_aggregate_by_day]
        df_final_user_convo = pd.concat(frames)

    #change dataframe name to fit with database
    df_final_user_convo = df_final_user_convo.rename(columns={'date': 'DATE'})[['DATE', 'EMAIL', 'NUM_CONVO_ROLLING']]

    return df_final_user_convo


def get_intercom_conversations():
    
    
    """
    Pull app creator support conversations in Intercom. 
    
    
    Parameters
    ----------
        
        
    Global Variables
    ----------
    
    intercom_key: str
        Client secret used to make api calls to Intercom.
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creator user IDs, conversation IDs, and conversation dates. 
           
    """

    #initiate list to store conversation IDs, creator user IDs, and conversation dates.
    convo_id_list = []
    user_id_list = []
    convo_date_list = []
    
    #initiate parameters to loop make api calls
    page = 1
    loop = True
    
    #make intercom api requests
    while loop:
        
        #track page number for pagination
        page += 1
        
        #execute Intercom api call
        response = requests.get(
            'https://api.intercom.io/conversations',
            params={'q': 'requests+language:python', 'per_page': 50, 'page': page},
            headers={'Accept': 'application/json','Authorization': 'Bearer '+ intercom_key},
        )

        #check for conversations in response
        if 'conversations' in response.json().keys():

            #extract conversations from response
            conversations = response.json()['conversations']
            if len(conversations)>0: #stops when there are no conversations

                #loop through conversations
                for convo in response.json()['conversations']:
                    
                    #extract user and convo IDs
                    user_id = convo['user']['id']
                    user_id_list.append(user_id)
                    convo_id_list.append(convo['id'])
                    
                    #get convo date
                    convo_date = datetime.utcfromtimestamp(int(convo['created_at'])).date()
                    convo_date_list.append(convo_date)

            else:
                loop = False

        else:
            loop = False
           
        
    #generate dataframe
    data = {'user_id': user_id_list, 'convo': convo_id_list, 'convo_date': convo_date_list}
    df_user_convo = pd.DataFrame(data)

    return df_user_convo



def pull_intercom_users():
    
    """
    
    Pull creator info from Intercom.


    Parameters
    ----------
        
        
    Global Variables
    ----------
    
    intercom_key: str
        Client secret used to make api calls to Intercom.
        
    Returns
    ----------
    
    dataframe
        Dataframe contains creator Intercom email and user ID.
           
    """
    
    #initiate lists to store app creator emails and user IDs
    user_email_list = []
    user_id_list = []

    #execute first request to get scroll parameter
    response = requests.get(
        'https://api.intercom.io/users/scroll',
        params={'q': 'requests+language:python'},
        headers={'Accept': 'application/json','Authorization': 'Bearer '+ intercom_key},
    )

    #process creator info from api call
    for user in response.json()['users']:
        
        #record creator email and user ID
        user_email_list.append(user['email'])
        user_id_list.append(user['id'])

    #extract scroll parameter
    scroll_param = response.json()['scroll_param']

    #initiate loop parameters. loop=True: make api call
    loop = True
    while loop:
        
        #execute api call
        response = requests.get(
            'https://api.intercom.io/users/scroll',
            params={'q': 'requests+language:python','scroll_param': scroll_param},
            headers={'Accept': 'application/json','Authorization': 'Bearer '+ intercom_key},
        )

        #check for users in response
        if 'users' in response.json().keys():
            
            #extract users from response
            users = response.json()['users']
            
            #check if number of users > 0
            if len(users)>0:
                
                #process api response
                for user in response.json()['users']:
                    
                    #record creator email and user ID
                    user_email_list.append(user['email'])
                    user_id_list.append(user['id'])
                    
            else:
                loop = False
                
        else:
            loop = False

    
    #generate dataframe
    data = {'user_email': user_email_list, 'user_id': user_id_list}
    df_intercom_users = pd.DataFrame(data)

    return df_intercom_users
  


def get_intercom_convations_count(df_intercom_users, from_date):
    
    """
    
    Calculate app creator number of support conversations per day.


    Parameters
    ----------
    
    df_intercom_users: dataframe
        Dataframe containing creator Intercom email and user ID.
        
        
    Global Variables
    ----------
    
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains creator email, date, and number of support conversations.
           
    """
    
    
    #get intercom conversations
    df_user_convo = get_intercom_conversations()

    #merge with Intercom users dataframe with conversation dataframe to get emails
    #remove any null emails because these are Leads. Lead IDs are not retained when the contacts convert to Users
    df_user_convo = pd.merge(df_user_convo, df_intercom_users, on='user_id', how='left')
    df_user_convo = df_user_convo[~df_user_convo.user_email.isnull()]

    #remove duplicate convos
    df_user_convo = df_user_convo.drop_duplicates('convo')

    #filter date range of convo
    df_user_convo = df_user_convo[df_user_convo.convo_date>=from_date]

    #change column names to match with the other dataframes
    df_user_convo = df_user_convo.rename(columns={'convo_date': 'date', 'user_email': 'email'})
    df_user_convo = df_user_convo.drop('user_id', axis=1) #remove intercom user ID to avoid conflict with Mixpanel user ID


    #count number of convo started per day from contact
    df_user_convo_aggregate = df_user_convo.groupby(['email', 'date']).convo.count().reset_index()
    
    return df_user_convo_aggregate


def get_current_metrics(df_final, df_creators):
    
    """
    
    Get creator current CS Healthblast metrics.


    Parameters
    ----------
    
    df_final: dataframe
        Dataframe containing creator email, date, and 7-days rolling average of editor and app sessions, average of number of app users, and sum of number of support conversations
        
    df_creators: dataframe
        Dataframe containing creator info.
        
        
    Global Variables
    ----------
    
   
        
    Returns
    ----------
    
    dataframe
        Dataframe contains creator email and current CS Healthblast metrics
           
    """
    
    #get user ID list to loop through
    user_id_list = df_final.drop_duplicates('user_id').user_id.tolist()

    #initiate lists to store emails and current metrics
    ACTIVE_USAGE_ROLLING_CURRENT_list = []
    ACTIVE_USER_ROLLING_CURRENT_list = []
    NUM_CONVO_ROLLING_CURRENT_list = []
    CREATOR_user_id_list = []
    
    #loop through emails
    for user_id in user_id_list:

        #filter for creator
        df_creator_metrics = df_final[df_final.user_id==user_id]

        #get current rolling averages of creator editor and app sessions, averages of number of app users, and sum of number of support conversations 
        ACTIVE_USAGE_ROLLING_CURRENT = df_creator_metrics.ACTIVE_USAGE_ROLLING.tolist()[-1]
        ACTIVE_USER_ROLLING_CURRENT = df_creator_metrics.ACTIVE_USER_ROLLING.tolist()[-1]
        NUM_CONVO_ROLLING_CURRENT = df_creator_metrics.NUM_CONVO_ROLLING.tolist()[-1]

        #store creator's current metrics and email
        ACTIVE_USAGE_ROLLING_CURRENT_list.append(ACTIVE_USAGE_ROLLING_CURRENT)
        ACTIVE_USER_ROLLING_CURRENT_list.append(ACTIVE_USER_ROLLING_CURRENT)
        NUM_CONVO_ROLLING_CURRENT_list.append(NUM_CONVO_ROLLING_CURRENT)
        CREATOR_user_id_list.append(user_id)

    #generate dataframe
    data = {'user_id': CREATOR_user_id_list, 'ACTIVE_USAGE_ROLLING_CURRENT': ACTIVE_USAGE_ROLLING_CURRENT_list, 'ACTIVE_USER_ROLLING_CURRENT':ACTIVE_USER_ROLLING_CURRENT_list,
            'NUM_CONVO_ROLLING_CURRENT': NUM_CONVO_ROLLING_CURRENT_list}
    df_creator_current_metrics = pd.DataFrame(data)
    
    #merge list of creators with current metrics. Fill in 0's for any creator who has been inactive.
    df_creator_current_metrics_final = pd.merge(df_creators, df_creator_current_metrics, on='user_id', how='left').fillna(0)
    
    #lowercase and drop duplicate
    df_creator_current_metrics_final = df_creator_current_metrics_final.drop_duplicates('user_id')

    return df_creator_current_metrics_final


#new hubspot update
def update_hubspot_profiles(df_creator_current_metrics_final_status):

    """
    
    Update HubSpot profiles with current CS Healthblast metrics.
    
    
    Parameters
    ----------
    
    df_creator_current_metrics_final_status: dataframe
        Dataframe containing creator user ID, email, metric statuses, metric gaps, and current 7-days rolling average of editor and app sessions, average of number of app users, and sum of number of support conversations.
      
    Global Variables
    ----------
    
    hubspot_key: str
        Client secret used to make api calls to HubSpot.
        
    
    Returns
    ----------
    
    None
        Function updates HubSpot profiles with current CS Healthblast metrics
    
    """

    #HubSpot authentication
    requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)

    #get email and user ID list to make HubSpot api calls
    email_list = df_creator_current_metrics_final_status.email.tolist()
    user_id_list = df_creator_current_metrics_final_status.user_id.tolist()

    i = 0
    #loop through email list to execute api calls
    for email, user_id in zip(email_list, user_id_list):
        
        i += 1
        print(i)

        #filter for creator
        df_creator_metrics = df_creator_current_metrics_final_status[df_creator_current_metrics_final_status.user_id==user_id]
        
        #get contact current metrics, statuses and signficance score
        ACTIVE_USAGE_ROLLING_CURRENT = df_creator_metrics.ACTIVE_USAGE_ROLLING_CURRENT.tolist()[0]
        ACTIVE_USER_ROLLING_CURRENT = df_creator_metrics.ACTIVE_USER_ROLLING_CURRENT.tolist()[0]
        NUM_CONVO_ROLLING_CURRENT = df_creator_metrics.NUM_CONVO_ROLLING_CURRENT.tolist()[0]
        number_of_app_users_status = df_creator_metrics.ACTIVE_USER_ROLLING_ANOMALY.tolist()[0]
        number_of_editor_and_app_sessions_status = df_creator_metrics.ACTIVE_USAGE_ROLLING_ANOMALY.tolist()[0]
        app_users_change_significance_score = df_creator_metrics.ACTIVE_USER_ROLLING_ANOMALY_GAP.tolist()[0]
        editor_and_app_sessions_change_significance_score = df_creator_metrics.ACTIVE_USAGE_ROLLING_ANOMALY_GAP.tolist()[0]

        health_status = df_creator_metrics.HEALTH_STATUS.tolist()[0]
        alert_reason = df_creator_metrics.ALERT_REASON.tolist()[0]
        
        #generate Mixpanel report editor and app sessions and number of app users links
        creator_editor_app_sessions_link = "https://mixpanel.com/report/487607/insights#~(displayOptions~(chartType~'line~plotStyle~'standard~analysis~'rolling~value~'absolute~rollingWindowSize~7)~isNewQBEnabled~true~sorting~(bar~(sortBy~'column~colSortAttrs~(~(sortBy~'value~sortOrder~'desc)~(sortBy~'value~sortOrder~'desc)))~line~(sortBy~'value~sortOrder~'desc)~table~(sortBy~'column~colSortAttrs~(~(sortBy~'label~sortOrder~'asc)~(sortBy~'label~sortOrder~'asc))))~columnWidths~(bar~())~title~'~querySamplingEnabled~false~sections~(show~(~(dataset~'!mixpanel~value~(id~1077769~custom~true~name~'ActiveUsage_Event~resourceType~'events)~resourceType~'events~profileType~null~search~'~dataGroupId~null~math~'total~property~null))~group~(~(dataset~'!mixpanel~value~'zAppName~resourceType~'events~profileType~null~search~'~dataGroupId~null~propertyType~'string~typeCast~null~unit~null))~filter~(clauses~(~(dataset~'!mixpanel~value~'zUserId~resourceType~'events~profileType~null~search~'~dataGroupId~null~filterType~'number~defaultType~'number~filterOperator~'is*20equal*20to~filterValue~" + str(user_id) + "~propertyObjectKey~null))~determiner~'all)~time~(~(dateRangeType~'in*20the*20last~unit~'day~window~(unit~'month~value~3))))~legendIdxSelections~(~))"
        
        num_app_users_link = "https://mixpanel.com/report/461315/insights#~(displayOptions~(chartType~'line~plotStyle~'standard~analysis~'rolling~value~'absolute~rollingWindowSize~7)~isNewQBEnabled~true~sorting~(bar~(sortBy~'column~colSortAttrs~(~(sortBy~'value~sortOrder~'desc)))~line~(sortBy~'value~sortOrder~'desc)~table~(sortBy~'column~colSortAttrs~(~(sortBy~'label~sortOrder~'asc))))~columnWidths~(bar~())~title~'~querySamplingEnabled~false~sections~(show~(~(dataset~'!mixpanel~value~(name~'AppStart~resourceType~'events)~resourceType~'events~profileType~null~search~'~dataGroupId~null~math~'unique~property~null))~filter~(clauses~(~(dataset~'!mixpanel~value~'zAppOwnerId~resourceType~'events~profileType~null~search~'~dataGroupId~null~filterType~'number~defaultType~'number~filterOperator~'is*20equal*20to~filterValue~" + str(user_id) + "~propertyObjectKey~null)~(dataset~'!mixpanel~value~'zUserId~resourceType~'events~profileType~null~search~'~dataGroupId~null~filterType~'number~defaultType~'number~filterOperator~'is*20greater*20than~filterValue~1~propertyObjectKey~null))~determiner~'all)~time~(~(dateRangeType~'in*20the*20last~unit~'day~window~(unit~'month~value~3))))~legendIdxSelections~(~))"
        
        
        
        
        #generate API call
        url ='http://api.hubapi.com/contacts/v1/contact/email/' + email + '/profile?hapikey=' + hubspot_key
        headers = {}
        headers['Content-Type']= 'application/json'
        data=json.dumps({
          "properties": [
            {
              "property": "number_of_editor_and_app_sessions_rolling_average_",
              "value": round(ACTIVE_USAGE_ROLLING_CURRENT,2)
            },
            {
              "property": "number_of_app_users_rolling_average_",
              "value": round(ACTIVE_USER_ROLLING_CURRENT,2)
            },
            {
              "property": "number_of_support_conversations_rolling_average_",
              "value": round(NUM_CONVO_ROLLING_CURRENT,2)
            },
            {
              "property": "number_of_editor_and_app_sessions_report_rolling_average_",
              "value": creator_editor_app_sessions_link
            },
            {
              "property": "number_of_app_users_report_rolling_average_",
              "value": num_app_users_link
            },
            {
              "property": "number_of_app_users_status",
              "value": number_of_app_users_status
            },
            {
              "property": "number_of_editor_and_app_sessions_status",
              "value": number_of_editor_and_app_sessions_status
            },
            {
              "property": "app_users_change_significance_score",
              "value": app_users_change_significance_score
            },
            {
              "property": "editor_and_app_sessions_change_significance_score",
              "value": editor_and_app_sessions_change_significance_score
            },
            {
              "property": "health_status",
              "value": health_status
            },
            {
              "property": "alert_reason",
              "value": alert_reason
            }
              
              
          ]
        })

        #execute api calls
        r = requests.post(data=data, url=url, headers=headers)
        
    return



def get_intercom_convo_data(df_date_range, from_date):

    #get intercom users with tags
    df_intercom_users = pull_intercom_users()

    # get intercom conversations count per day by users
    df_user_convo_aggregate = get_intercom_convations_count(df_intercom_users, from_date)

    # convert datetime to py datetime to match with date dataframe
    df_user_convo_aggregate['date'] = pd.to_datetime(df_user_convo_aggregate.date)

    #get rolling average intercom conversations count per day by users
    df_final_user_convo = get_rolling_sum_convo(df_user_convo_aggregate, df_date_range)

    return df_final_user_convo



def identify_anomaly(df_final):
    
    #get list of emails to loop through
    user_id_list = df_final.drop_duplicates('user_id').user_id.tolist()

    #initiate list to store statuses
    ACTIVE_USAGE_ROLLING_anomaly_list = []
    ACTIVE_USER_ROLLING_anomaly_list = []
    NUM_CONVO_ROLLING_anomaly_list = []
    contact_user_id_list = []
    ACTIVE_USAGE_ROLLING_anomaly_gap_list = []
    ACTIVE_USER_ROLLING_anomaly_gap_list = []
    ALERT_REASON_list = []
    HEALTH_STATUS_list = []


    for user_id in user_id_list:

        #initiate anomaly detention status
        ACTIVE_USAGE_ROLLING_anomaly = ''
        ACTIVE_USER_ROLLING_anomaly = ''
        NUM_CONVO_ROLLING_anomaly = ''
        ACTIVE_USAGE_ROLLING_anomaly_gap = 0
        ACTIVE_USER_ROLLING_anomaly_gap = 0
        ALERT_REASON = ''
        HEALTH_STATUS = 'ACTIVE'


        #filter down to recent data
        df_contact = df_final[df_final.user_id==user_id].tail(15)

        #get prior 14 days and current metrics
        df_contact_prior = df_contact.head(14)
        df_contact_current = df_contact.tail(1)
        
        
        
  
        #find ACTIVE_USAGE_ROLLING anomaly
        ACTIVE_USAGE_ROLLING_current = df_contact_current.ACTIVE_USAGE_ROLLING.tolist()[0]
        contact_ACTIVE_USAGE_ROLLING_list = df_contact_prior.ACTIVE_USAGE_ROLLING.tolist()
        ACTIVE_USAGE_ROLLING_upper = np.percentile(contact_ACTIVE_USAGE_ROLLING_list,95)
        ACTIVE_USAGE_ROLLING_lower = np.percentile(contact_ACTIVE_USAGE_ROLLING_list,5)

        if (ACTIVE_USAGE_ROLLING_current < ACTIVE_USAGE_ROLLING_lower) & (statistics.mean(contact_ACTIVE_USAGE_ROLLING_list) > 20): #only consider for alerts if seeing signicant traffic prior to drop
            ACTIVE_USAGE_ROLLING_anomaly = 'DECREASE IN ACTIVITY'
            ACTIVE_USAGE_ROLLING_anomaly_gap = abs(round(((ACTIVE_USAGE_ROLLING_current/ACTIVE_USAGE_ROLLING_lower)-1)*100))
        elif (ACTIVE_USAGE_ROLLING_current > ACTIVE_USAGE_ROLLING_upper) & (ACTIVE_USAGE_ROLLING_current>10): #only consider for alerts if seeing significant metric after increase
            ACTIVE_USAGE_ROLLING_anomaly = 'INCREASE IN ACTIVITY'
            
            #check if upper is 0 to avoid dividing by 0
            if ACTIVE_USAGE_ROLLING_upper == 0:
                ACTIVE_USAGE_ROLLING_anomaly_gap = 100 #anyone who just became active again should have a high score
            else:
                ACTIVE_USAGE_ROLLING_anomaly_gap = min([round(((ACTIVE_USAGE_ROLLING_current/ACTIVE_USAGE_ROLLING_upper)-1)*100),100]) #cap at 100


        #find ACTIVE_USER_ROLLING anomaly
        ACTIVE_USER_ROLLING_current = df_contact_current.ACTIVE_USER_ROLLING.tolist()[0]
        contact_ACTIVE_USER_ROLLING_list = df_contact_prior.ACTIVE_USER_ROLLING.tolist()
        ACTIVE_USER_ROLLING_upper = np.percentile(contact_ACTIVE_USER_ROLLING_list,95)
        ACTIVE_USER_ROLLING_lower = np.percentile(contact_ACTIVE_USER_ROLLING_list,5)

        if (ACTIVE_USER_ROLLING_current < ACTIVE_USER_ROLLING_lower)  & (statistics.mean(contact_ACTIVE_USER_ROLLING_list) > 20): #only consider for alerts if seeing signicant traffic prior to drop
            ACTIVE_USER_ROLLING_anomaly = 'DECREASE IN USERS'
            ACTIVE_USER_ROLLING_anomaly_gap = abs(round(((ACTIVE_USER_ROLLING_current/ACTIVE_USER_ROLLING_lower)-1)*100))
        elif (ACTIVE_USER_ROLLING_current > ACTIVE_USER_ROLLING_upper) & (ACTIVE_USER_ROLLING_current>20): #only consider for alerts if seeing significant metric after increase
            ACTIVE_USER_ROLLING_anomaly = 'INCREASE IN USERS'
            
            #check if upper is 0 to avoid dividing by 0
            if ACTIVE_USER_ROLLING_upper == 0:
                ACTIVE_USER_ROLLING_anomaly_gap = 100 #anyone who just became active again should have a high score
            else:
                ACTIVE_USER_ROLLING_anomaly_gap = min([round(((ACTIVE_USER_ROLLING_current/ACTIVE_USER_ROLLING_upper)-1)*100),100]) #cap at 100

                
        #find current number of support conversations
        NUM_CONVO_ROLLING_current = df_contact_current.NUM_CONVO_ROLLING.tolist()[0]
        
         
        #assign alert reason
        if (ACTIVE_USER_ROLLING_anomaly=='INCREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly=='INCREASE IN ACTIVITY'):
            ALERT_REASON = 'Increase in creator activity and number of users.'
            
        elif (ACTIVE_USER_ROLLING_anomaly=='DECREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly=='DECREASE IN ACTIVITY'):
            ALERT_REASON = 'Decrease in creator activity and number of users.'
            
        elif ((ACTIVE_USER_ROLLING_anomaly=='DECREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly=='INCREASE IN ACTIVITY'))| ((ACTIVE_USER_ROLLING_anomaly=='DECREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly=='')):
            ALERT_REASON = 'Decrease in number of users.'
            
        elif ((ACTIVE_USER_ROLLING_anomaly=='INCREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly=='DECREASE IN ACTIVITY'))| ((ACTIVE_USER_ROLLING_anomaly=='') & (ACTIVE_USAGE_ROLLING_anomaly=='DECREASE IN ACTIVITY')):
            ALERT_REASON = 'Decrease in creator activity.'
            
        elif (ACTIVE_USER_ROLLING_anomaly=='INCREASE IN USERS') & (ACTIVE_USAGE_ROLLING_anomaly==''):
            ALERT_REASON = 'Increase in number of users.'
            
        elif (ACTIVE_USER_ROLLING_anomaly=='') & (ACTIVE_USAGE_ROLLING_anomaly=='INCREASE IN ACTIVITY'):
            ALERT_REASON = 'Increase in creator activity.'
            
        if (NUM_CONVO_ROLLING_current>3): #high number of support conversations will override the other statuses
            ALERT_REASON = 'High number of support conversations.'
            
        #assign health status based on alert reason
        if (ALERT_REASON=='Decrease in creator activity and number of users.') | (ALERT_REASON=='Decrease in number of users.') | (ALERT_REASON=='Decrease in creator activity.') | (ALERT_REASON=='High number of support conversations.'):
            HEALTH_STATUS = 'NEEDS ATTENTION!'
        elif (ALERT_REASON=='Increase in creator activity and number of users.') | (ALERT_REASON=='Increase in number of users.') | (ALERT_REASON=='Increase in creator activity.'):
            HEALTH_STATUS = 'INCREASE ACTIVITY'
        
            
            
        #record contact metric statuses
        ACTIVE_USAGE_ROLLING_anomaly_list.append(ACTIVE_USAGE_ROLLING_anomaly)
        ACTIVE_USER_ROLLING_anomaly_list.append(ACTIVE_USER_ROLLING_anomaly)
        NUM_CONVO_ROLLING_anomaly_list.append(NUM_CONVO_ROLLING_anomaly)
        ACTIVE_USAGE_ROLLING_anomaly_gap_list.append(ACTIVE_USAGE_ROLLING_anomaly_gap)
        ACTIVE_USER_ROLLING_anomaly_gap_list.append(ACTIVE_USER_ROLLING_anomaly_gap)
        contact_user_id_list.append(user_id)
        ALERT_REASON_list.append(ALERT_REASON)
        HEALTH_STATUS_list.append(HEALTH_STATUS)

    #create dataframe
    data = {'user_id': contact_user_id_list, 'ACTIVE_USAGE_ROLLING_ANOMALY': ACTIVE_USAGE_ROLLING_anomaly_list, 'ACTIVE_USAGE_ROLLING_ANOMALY_GAP': ACTIVE_USAGE_ROLLING_anomaly_gap_list, 
            'ACTIVE_USER_ROLLING_ANOMALY': ACTIVE_USER_ROLLING_anomaly_list, 'ACTIVE_USER_ROLLING_ANOMALY_GAP': ACTIVE_USER_ROLLING_anomaly_gap_list,
            'HEALTH_STATUS': HEALTH_STATUS_list, 'ALERT_REASON': ALERT_REASON_list}
    df_metric_status = pd.DataFrame(data)

    return df_metric_status



def main():
    
    """

    Update HubSpot profiles with current CS Healthblast metrics.


    Parameters
    ----------


    Global Variables
    ----------


    Returns
    ----------

    None

    """

    #send initial email
    send_email('Initiate CS Healthblast Update')

    # pulling 46 days for now to generate the first batch
    today = date.today()
    from_date = today - timedelta(days=22)
    to_date = today - timedelta(days=1)


    #generate dataframe with dates address for days when there is no activity
    numdays = 23 
    base = to_date
    date_list = [pd.Timestamp(base - timedelta(days=x)) for x in range(numdays)]
    data = {'date': date_list}
    df_date_range = pd.DataFrame(data).sort_values('date', ascending=True)

    print('starting')
    #pull app creator info, Editors, AppStarts and app users AppStarts
    df_user_AppStart = user_appstart_pull(from_date, to_date)
    df_Editor = Editor_pull(from_date, to_date)
    df_AppStart = AppStart_pull(from_date, to_date)
    df_creators = creator_pull()


    #gather metrics for contacts
    df_final_creator_usage = get_rolling_average_creator_usage(df_AppStart, df_Editor, df_date_range, df_creators) 
    df_final_active_users = get_rolling_average_num_app_users(df_user_AppStart, df_date_range, df_creators) 
    df_final_user_convo = get_intercom_convo_data(df_date_range, from_date)

    #merge final dataframes for the 3 metrics
    df_final = pd.merge(df_final_creator_usage[['DATE', 'user_id', 'OWNER', 'EMAIL', 'ACTIVE_USAGE_ROLLING']], df_final_active_users[['user_id', 'DATE', 'ACTIVE_USER_ROLLING']], on=['user_id', 'DATE'], how='left')
    df_final = pd.merge(df_final, df_final_user_convo[['DATE', 'EMAIL', 'NUM_CONVO_ROLLING']], on=['EMAIL', 'DATE'], how='left')

    #fill any nulls with zeroes
    df_final = df_final.fillna(0)

    #get contact current metrics and update HubSpot profiles
    df_creator_current_metrics_final = get_current_metrics(df_final, df_creators)




    # identify anomaly for each metric
    df_metric_status = identify_anomaly(df_final)

    #merge current metric with anomaly status
    df_creator_current_metrics_final_status = pd.merge(df_creator_current_metrics_final, df_metric_status, on='user_id', how='left')

    #Fill empty gap values with 0's. Then fill null values with '' to indicate that there is no anomaly. 
    df_creator_current_metrics_final_status['ACTIVE_USAGE_ROLLING_ANOMALY_GAP'] = df_creator_current_metrics_final_status.ACTIVE_USAGE_ROLLING_ANOMALY_GAP.fillna(0)
    df_creator_current_metrics_final_status['ACTIVE_USER_ROLLING_ANOMALY_GAP'] = df_creator_current_metrics_final_status.ACTIVE_USER_ROLLING_ANOMALY_GAP.fillna(0)
    df_creator_current_metrics_final_status['HEALTH_STATUS'] = df_creator_current_metrics_final_status.HEALTH_STATUS.fillna('INACTIVE')
    df_creator_current_metrics_final_status['ALERT_REASON'] = df_creator_current_metrics_final_status.ALERT_REASON.fillna('')
    df_creator_current_metrics_final_status = df_creator_current_metrics_final_status.fillna('')

    #lower case email addresses and remove duplicates. Keep account with high current activity
    df_creator_current_metrics_final_status['email'] = df_creator_current_metrics_final_status.email.str.lower()
    df_creator_current_metrics_final_status = df_creator_current_metrics_final_status.sort_values('ACTIVE_USAGE_ROLLING_CURRENT', ascending=False).drop_duplicates('email', keep='first')


    print(len(df_creator_current_metrics_final_status))
    update_hubspot_profiles(df_creator_current_metrics_final_status)

    #send completed email
    send_email('CS Healthblast Update Completed')

    return




#execute update
if __name__ == '__main__':
    main()
