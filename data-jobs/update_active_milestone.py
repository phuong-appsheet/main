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
from pt_utils.utils import send_email
from pt_utils import mandrillpt
  
#import credentials object
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))








def app_creator_pull():

    """
    Pull app creators' info from Mixpanel.
    
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
    query_user = JQL(
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
                    "e.properties.$distinct_id",
                    "e.properties.active_milestone"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, distinct_id, active_milestone in lists
    email_list = []
    user_id_list = []
    distinct_id_list = []
    active_milestone_list = []
    for row in query_user.send():
        if row['key'][1] is not None:
            email_list.append(row['key'][0])
            user_id_list.append(int(row['key'][1]))
            distinct_id_list.append(row['key'][2])
            active_milestone_list.append(row['key'][3])

    #create dataframe 
    data = {'email':email_list, 'app_owner_id': user_id_list, 'distinct_id': distinct_id_list, 'active_milestone': active_milestone_list}
    df_creators = pd.DataFrame(data=data)
    return df_creators


def pull_usage(from_date, to_date):

    """
    Pull usage events from Mixpanel.
    
    
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
        Dataframe contains owner ID, user IDs, and usage event count in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_creator_secret,
        events=Events({
                'event_selectors': [{'event': "Usage"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.OwnerId",
                    "e.properties.UserId"
                ],
                accumulator=Reducer.count()
            )

    #store app owner, app user IDs, and usage count
    app_owner_id_list = []
    app_user_id_list = []
    usage_list = []
    for row in query.send():
        if row['key'][0] is not None:
            app_owner_id_list.append(int(row['key'][0]))
            app_user_id_list.append(int(row['key'][1]))
            usage_list.append(row['value'])

    #generate dataframe
    data = {'app_owner_id':app_owner_id_list,'app_user_id': app_user_id_list, 'usage': usage_list}
    df_usage = pd.DataFrame(data)
    
    #only keep app owners and users with proper IDs
    df_usage = df_usage[(df_usage.app_owner_id>1)&(df_usage.app_user_id>1)]
    df_usage = df_usage[~df_usage.app_owner_id.isin([10305, 71626])] #remove for demo accounts


    return df_usage





def get_usage_metrics(from_date_usage, to_date_usage): 

    """
    Pull list of active creators based on current definition: App creator must meet one of the following conditions in the past 30 days - 1.) At least 40 usage events in the past 30 days or 2.) At least one other user using their apps.
    
    
    Parameters
    ----------
    
    
    from_date_usage: date
        Start date of query.
        
    to_date_usage: date
        End date of query. 
  
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    list
        List of active app creators.
    
    """

    #pull usage by owner and user IDs
    df_usage = pull_usage(from_date_usage, to_date_usage) 

    ###Identify active app creators based on current definition.###
    
    
    ##1st condition##
    #group by usage count
    df_usage_sum = df_usage.groupby('app_owner_id').usage.sum().reset_index()

    #only keep accounts with at least 40 usage events in the past 30 days
    df_usage_40 = df_usage_sum[df_usage_sum.usage>=40]

    
    ##2nd condition##
    #exclude cases when app owner ID is the same as app user ID
    df_usage_other_user = df_usage[df_usage.app_owner_id!=df_usage.app_user_id]

    #count the number of "other" users
    df_usage_other_user = df_usage_other_user.groupby('app_owner_id').app_user_id.count().reset_index()

    #only keep app owners with at least one other user using their apps
    df_usage_other_user = df_usage_other_user[df_usage_other_user.app_user_id>0]

    ##calculate the number of active creators##
    active_creators_list = list(set(df_usage_40.app_owner_id.tolist()+df_usage_other_user.app_owner_id.tolist()))

  
    return active_creators_list

def update_mixpanel_active_status(df_creators_active):
    
    """
    Update creators' active_milestone status in Mixpanel.
    
    Parameters
    ----------
    
    
    df_creators_active: DataFrame
        Dataframe of active creators, w/ distinct_id to be used to update their Mixpanel profiles.
  
 
    Global Variables
    ----------
    
    mp: Mixpanel Client
        Client used to update Mixpanel profiles.
        
        
    Returns
    ----------
    
   
    
    """
    
    #set active_milestone to True
    params = {'active_milestone': True}
    
    #update Mixpanel app creator profiles with new active_milestone status
    mixpanel_distinct_id_list = df_creators_active.distinct_id.tolist()
    for i in range(len(mixpanel_distinct_id_list)):
        
        #get distinct_id
        distinct_id = mixpanel_distinct_id_list[i]

        #execute update
        mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        
    return



def main():
    
    """
    Update app creator's active milestone in Mixpanel.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    
    #send initial email
    send_email("Initiate Adoption Milestone Update")

    
    try:
        #date range for usage metrics
        to_date_usage = date.today() - timedelta(days=1)
        from_date_usage = to_date_usage - timedelta(days=30) 

        #pull creators profile
        df_creators = app_creator_pull()

        #only keep creators who did not have active_milestone set to true
        df_creators = df_creators[df_creators.active_milestone!=True]

        #pull active creators
        active_creators_list = get_usage_metrics(from_date_usage, to_date_usage)

        #only keep active app creators
        df_creators_active = df_creators[df_creators.app_owner_id.isin(active_creators_list)]

        print(len(df_creators_active))

        #update Mixpanel profile with status
        update_mixpanel_active_status(df_creators_active)
        
    except:
        time.sleep(30)

    #send completion email
    send_email("Active Milestone Update Completed")
    
    return
    
    
    
    
#execute update
if __name__ == "__main__":
    main()
