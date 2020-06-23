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
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')
google_drive_client_secrets = os.path.expanduser(os.environ.get('GD_CLIENT_APPSHEET'))

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
    
    main_sheet: Google Sheet Object. Resource: https://medium.com/@denisluiz/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
        
    """

    # use credentials to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_drive_client_secrets, scope)
    client = gspread.authorize(creds)

    # accessing Google Sheets
    main_sheet = client.open("appsheet_adoption_metrics").get_worksheet(0)

    return main_sheet

def update_google_sheet(today_new_signup, today_total_number_of_active_creators, today_monthly_active_app_users, to_date_usage, main_sheet):
    
    """
    
    Update Google Sheet with new data.
    
    Parameters
    ----------
    
    today_new_signup: int
        Today's number of new signups.
    
    today_total_number_of_active_creators: int
        Today's total number of active creators.
        
    today_monthly_active_app_users: int
        Today's monthly active app users.
        
    to_date_usage: datetime
        Current date.
        
    main_sheet: Google Sheet instance
        AppSheet's adoption metrics Google Sheet.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    None
        Function updates dashboard.
           
    """
    
    #get current index value in Google Sheet
    index = (to_date_usage - date(year = 2020, month = 1, day = 16)).days + 2
    print(index)
    print(to_date_usage)
    #make updates to Google Sheet

    #update referral signup
    main_sheet.update_cell(index, 2, today_new_signup)
    main_sheet.update_cell(index, 3, today_total_number_of_active_creators)
    main_sheet.update_cell(index, 4, today_monthly_active_app_users)


    print("Google Sheet has been updated!")
    
    return

def New_Signup_Web_pull(from_date, to_date):

    """
    Pull app creator sign up events in Mixpanel.
    
    
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
        Dataframe contains user IDs, sign up datetime, new sign up event count, and country for app creators in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_creator_secret,
        events=Events({
                'event_selectors': [{'event': "New Signup Web"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.userId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )

    #store emails, user IDs, sign up datetime, and country
    user_id_list = []
    datetime_list = []
    new_sign_up_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            new_sign_up_list.append(row['value'])

    #generate dataframe
    data = {'app_owner_id':user_id_list,'date': datetime_list,'new_sign_up': new_sign_up_list}
    df_New_Signup_Web = pd.DataFrame(data)
    
    #only keeping ones with actual IDs
    df_New_Signup_Web = df_New_Signup_Web[df_New_Signup_Web.app_owner_id>1]
    
    return df_New_Signup_Web


def app_creator_pull():

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
                    "e.properties.$username"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    user_id_list = []
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])

    #create dataframe 
    data = {'email':email_list, 'app_owner_id': user_id_list}
    df_creators = pd.DataFrame(data=data)
    return df_creators


def pull_usage(from_date, to_date):

    """
    Pull usage events in Mixpanel.
    
    
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
        Dataframe contains app ID, user IDs, owner ID, and usage event count in Mixpanel.
           
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

    #store app owner, app, and app user IDs
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


def get_signup_metric(from_date_signup, to_date_signup, df_creators):
    
    """
    Pull current day total number of signups.
    
    
    Parameters
    ----------
    
    
    from_date_signup: date
        Start date of query.
        
    to_date_signup: date
        End date of query. 
        
    df_creators: dataframe
        Dataframe containing app creator profiles.
    
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    int
        Total number of new signups.
    
    """
    
    #pull number of new signups
    df_New_Signup_Web = New_Signup_Web_pull(from_date_signup, to_date_signup)

    #check if we have emails associated to these signups and drop duplicate emails
    df_New_Signup_Web = pd.merge(df_New_Signup_Web, df_creators, on='app_owner_id', how='left')
    df_New_Signup_Web = df_New_Signup_Web[~df_New_Signup_Web.email.isnull()]
    df_New_Signup_Web = df_New_Signup_Web.drop_duplicates('email')

    #calculate total number of sign ups per day
    df_New_Signup_Web = df_New_Signup_Web.groupby('date').new_sign_up.count().reset_index()
    today_new_signup = df_New_Signup_Web.new_sign_up.tolist()[0]
    
    return today_new_signup



def get_usage_metrics(from_date_usage, to_date_usage): 

    """
    Pull current day total number of signups.
    
    
    Parameters
    ----------
    
    
    from_date_signup: date
        Start date of query.
        
    to_date_signup: date
        End date of query. 
  
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    ints
        Total number of active app creators.
        Monthly number of active app users.
    
    """

    #pull usage data
    df_usage = pull_usage(from_date_usage, to_date_usage)

    #group by usage count
    df_usage_sum = df_usage.groupby('app_owner_id').usage.sum().reset_index()

    #only keep accounts with more than 40 usage events in the past 30 days
    df_usage_40 = df_usage_sum[df_usage_sum.usage>=40]

    #count number of users 
    df_usage_other_user = df_usage[df_usage.app_owner_id!=df_usage.app_user_id]

    #count the number of other users
    df_usage_other_user = df_usage_other_user.groupby('app_owner_id').app_user_id.count().reset_index()

    #only keep app owners with at least one other user using their apps
    df_usage_other_user = df_usage_other_user[df_usage_other_user.app_user_id>0]

    #calculate the number of active creators
    today_total_number_of_active_creators = len(list(set(df_usage_40.app_owner_id.tolist()+df_usage_other_user.app_owner_id.tolist())))


    #calculate total number of active users
    today_monthly_active_app_users = len(df_usage.drop_duplicates('app_user_id'))
                       
    return today_total_number_of_active_creators, today_monthly_active_app_users

def main():
    
    """
    Update adoption metrics dashboard.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    #send initial email
    send_email("Initiate Adoption Metrics Update")

    #date range for new signups
    to_date_signup = date.today() - timedelta(days=1)
    from_date_signup = date.today() - timedelta(days=1)
    
    #date range for usage metrics
    to_date_usage = date.today() - timedelta(days=1)
    from_date_usage = to_date_usage - timedelta(days=30)
 
    
    #pull creator profiles
    df_creators = app_creator_pull()
    
    #get metrics
    today_new_signup = get_signup_metric(from_date_signup, to_date_signup, df_creators)
    today_total_number_of_active_creators, today_monthly_active_app_users = get_usage_metrics(from_date_usage, to_date_usage)
    
    #update Google Sheet
    main_sheet = auth_google_services()
    update_google_sheet(today_new_signup, today_total_number_of_active_creators, today_monthly_active_app_users, to_date_usage, main_sheet)


    #send email when completed  
    send_email("Adoption Metrics Update Completed")
     
    
    
    return 


 
#execute update
if __name__ == "__main__":
    main()
    
    
