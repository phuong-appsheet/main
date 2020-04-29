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
import gspread  
from oauth2client.service_account import ServiceAccountCredentials
from pt_utils.utils import send_email

#import credentials object
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')
google_drive_client_secrets = os.path.expanduser(os.environ.get('GD_CLIENT_SECRETS'))


 
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
                    "e.properties.$username"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    user_id_list = []
    for row in query.send():
        if row['key'][0] is not None:
            email_list.append(row['key'][0])
            user_id_list.append(int(row['key'][1]))

    #create dataframe 
    data = {'email':email_list, 'creator_id': user_id_list}
    df_creators = pd.DataFrame(data=data)
    
    
    return df_creators



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
                    "e.properties.zAppOwnerId"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record app user IDs, app creator IDs, app name, and number of AppStarts
    app_user_id_list = []
    owner_id_list = []
    AppStart_list = []

    #process query results
    for row in query.send():
        if (row['key'][0] is not None) & (row['key'][1] is not None):
            app_user_id_list.append(int(row['key'][0]))
            owner_id_list.append(int(row['key'][1]))
            AppStart_list.append(row['value'])

    #generate email
    data = {'app_user_id':app_user_id_list,'creator_id': owner_id_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)
    
    #make sure IDs are valid
    df_user_AppStart = df_AppStart[(df_AppStart.app_user_id>1)&(df_AppStart.creator_id>1)]
    
    return df_user_AppStart


def auth_google_services():
    
    """
    Authenticate to Google Drive.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    google_drive_client_secrets: str
        Client secret used to make calls to Google Drive API.
        
        
    Returns
    ----------
    
    user_sheet: Google Sheet Object
    
    
        Objects to make pull data from GA and to update dashboards in Google Sheets.
        
    """
    
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_drive_client_secrets, scope)
    client = gspread.authorize(creds)

    # accessing Google Sheets
    sheet = client.open("Customer Success Dashboard")
    user_sheet = sheet.get_worksheet(1)
    
    
    return user_sheet


def update_spreadsheet(user_sheet, num_user_dictionary):
    
    """
    
    Update Google Sheet with new data.
    
    Parameters
    ----------
    
    user_sheet: Google Sheet object
        Use to access to Sales CS Google Sheet.
    
    num_user_dictionary: dict
        Dictionary contains number of users in past 30 days associated to the domains.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    None
        Function updates Sales CS spreadsheet with number of users data for domains.
           
    """

    #make updates to Google Sheets
    i = 0
    num_companies = len(user_sheet.col_values(1))
    while i in range(num_companies):
        
        
        index = i + 2 #reset index for page

        try:
            domain = user_sheet.cell(index, 2).value.lower().lower()

            #check if domain is in num_user dictionary
            if domain in num_user_dictionary.keys():
                
                #number of active users in past 30 days
                num_active_users = num_user_dictionary[domain]

                #update cells with recent number of active users
                user_sheet.update_cell(index,3,num_active_users)

            i += 1

        except Exception as e:
                print(e)
                time.sleep(10)
                
    return






def main():
    
    """
    Update Number of Users Metrics in Sales CS Dashboard.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """

    #send initial email
    send_email("Initiate Sales CS Update")
    
    #auth and access Google sheet
    user_sheet = auth_google_services()

    #generate date range
    today = date.today()
    to_date = today - timedelta(days=1)
    from_date = to_date - timedelta(days=30)

    #pull app creator info, Editors, AppStarts and app users AppStarts
    df_user_AppStart = user_appstart_pull(from_date, to_date)
    df_creators = creator_pull()

    #merge creator and AppStart data
    df_final = pd.merge(df_user_AppStart, df_creators, on='creator_id', how='left')
    df_final = df_final.dropna() #remove nulls

    #lowercase emails for domain extractions
    df_final['email'] = df_final.email.str.lower()

    #extract creator email domains
    df_final['user_email_domains'] = df_final['email'].str.split('@').str[1].fillna('')

    #removing gmail, hotmail, outlook, and yahoo
    df_final = df_final[~df_final.user_email_domains.isin(['appsheet.com','1track.com','gmail.com','hotmail.com','outlook.com','live.com','yahoo.com'])]

    #count the number of users for by domains and store as dictionary
    df_final = df_final.groupby(['user_email_domains']).app_user_id.count().reset_index()
    num_user_dictionary = dict(zip(df_final.user_email_domains.tolist(), df_final.app_user_id.tolist()))

    #update spreadsheet with new num_user data
    update_spreadsheet(user_sheet, num_user_dictionary)
    
    #send email when completed
    send_email("Sales CS Update Completed")
    
    return



    
#execute update
if __name__ == "__main__":
    main()
