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
hubspot_key = os.environ.get('HUBSPOT_KEY')
google_drive_client_secrets = os.path.expanduser(os.environ.get('GD_CLIENT_APPSHEET'))
mandrill_api_key = os.environ.get('MANDRILL_KEY')
mandrill_client = mandrillpt.Mandrill(mandrill_api_key)

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
        if row['key'][1] is not None:
            email_list.append(row['key'][0])
            user_id_list.append(int(row['key'][1]))

    #create dataframe 
    data = {'email':email_list, 'user_id': user_id_list}
    df_creators = pd.DataFrame(data=data)
    return df_creators


def app_user_pull():

    """
    Pull app users' info in Mixpanel.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    api_user_secret: str
        Client secret used to make calls to Mixpanel User Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app users info in Mixpanel.
           
    """
    
    #generate JQL query
    query_user = JQL(
                api_user_secret,
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


    #store emails and user IDs
    email_list = []
    user_id_list = []
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])

    #create dataframe 
    data = {'email':email_list, 'app_user_id': user_id_list}
    df_app_users = pd.DataFrame(data=data)
    return df_app_users

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
    data = {'user_id':user_id_list,'sign_up_datetime': datetime_list,'new_sign_up': new_sign_up_list}
    df_New_Signup_Web = pd.DataFrame(data)
    
    
    return df_New_Signup_Web

def New_Signup_App_pull(from_date, to_date):

    """
    
    Pull app user sign up events in Mixpanel.
    
    
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
    
    dataframe
        Dataframe contains user IDs, sign up datetime, and new sign up event count for app users in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_user_secret,
        events=Events({
                'event_selectors': [{'event': "New Signup App"}],
                'from_date': from_date,
                'to_date': to_date
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and sign up datetime
    user_id_list = []
    datetime_list = []
    new_sign_up_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            new_sign_up_list.append(row['value'])

    #generate dataframe
    data = {'app_user_id':user_id_list,'sign_up_datetime': datetime_list,'new_signup_app': new_sign_up_list}
    df_New_Signup_App = pd.DataFrame(data)
    return df_New_Signup_App



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




def get_creator_invite_creators_myteam(yesterday, today):

    """
    Pull number of creators who were invited by other creators to be part of "My Team".
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
    today: date
        Today's date.
       
       
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe with email, email date, email template, and subject for sent emails.
        
        
    """
    
    #just have to pull one time now then store. then pull on a daily basis after that
  
    
    #initialize template, email address, subject, and date lists
    template_list = []
    to_email_list = []
    subject_list = []
    date_list = []
    
    #generate timestamp range for query
    ts_start = datetime.fromordinal(yesterday.toordinal()).timestamp()
    ts_end_final = datetime.fromordinal(today.toordinal()).timestamp()
    
    #generage query calls until timestamp range is met using while loop and one hour increments since there is a 1000 object limit
    while ts_start < ts_end_final:
        
        #construct timestamp for query
        ts_end = ts_start + 3600
        ts_end_string = str(int(ts_end))
        ts_start_string = str(int(ts_start))
        ts_query = f"ts:[{ts_start_string} TO {ts_end_string}]"

        #generate query
        query = 'subject:"please join the" AND subject:"team using AppSheet" AND ' + ts_query

        #execute query
        result = mandrill_client.messages.search(query=query,
                                                  limit=1000)
        #process query results
        for email in result:
            template_list.append(email['template'])
            to_email_list.append(email['email'])
            subject_list.append(email['subject'])
            date_list.append(yesterday)

        ts_start = ts_end 
        time.sleep(3)

        
    #generate dataframe
    data = {'email':to_email_list, 'email_date': date_list, 'template': template_list, 'subject': subject_list}
    df_creator_invite_creators_myteam_daily = pd.DataFrame(data)
    df_creator_invite_creators_myteam_daily = df_creator_invite_creators_myteam_daily[df_creator_invite_creators_myteam_daily.template=='final-invitetoteam']

    #load current creator_invite_creators_myteam
    df_creator_invite_creators_myteam = pd.read_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_myteam_Q2_2020.csv')
    
    #concat current and new creator_invite_creators_myteam dataframes
    frames = [df_creator_invite_creators_myteam, df_creator_invite_creators_myteam_daily]
    df_creator_invite_creators_myteam_updated = pd.concat(frames)
    
    #convert datetime.date to pandas timestamp
    df_creator_invite_creators_myteam_updated['email_date'] = pd.to_datetime(df_creator_invite_creators_myteam_updated.email_date)
    
    #log new df_creator_invite_creators_myteam_updated
    df_creator_invite_creators_myteam_updated.to_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_myteam_Q2_2020.csv', index=False)
    
    return df_creator_invite_creators_myteam_updated


def get_creator_invite_creators(yesterday, today):

    
    """
    Pull number of creators who were invited by other creators to be co-authors.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
    today: date
        Today's date.
       
       
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe with email, email date, email template, and subject for sent emails.
        
        
    """
    
    
    #initialize template, email address, subject, and date lists
    template_list = []
    to_email_list = []
    subject_list = []
    date_list = []
    
    #generate timestamp range for query
    ts_start = datetime.fromordinal(yesterday.toordinal()).timestamp()
    ts_end_final = datetime.fromordinal(today.toordinal()).timestamp()
    
    #generage query calls until timestamp range is met using while loop and one hour increments since there is a 1000 object limit
    while ts_start < ts_end_final:
        
        #construct timestamp for query
        ts_end = ts_start + 3600
        ts_end_string = str(int(ts_end))
        ts_start_string = str(int(ts_start))
        ts_query = f"ts:[{ts_start_string} TO {ts_end_string}]"

        #generate query
        query = 'subject:help AND subject:build AND ' + ts_query

        #execute query
        result = mandrill_client.messages.search(query=query,
                                                  limit=1000)
        
        #proccess query results
        for email in result:
            template_list.append(email['template'])
            to_email_list.append(email['email'])
            subject_list.append(email['subject'])
            date_list.append(yesterday)

        ts_start = ts_end 
        time.sleep(3)

    #generate dataframe
    data = {'email':to_email_list, 'email_date': date_list, 'template': template_list, 'subject': subject_list}
    df_creator_invite_creators_daily = pd.DataFrame(data)
    df_creator_invite_creators_daily = df_creator_invite_creators_daily[df_creator_invite_creators_daily.template=='collaborator-invite']

    #load current creator_invite_creators as co-authors
    df_creator_invite_creators = pd.read_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_Q2_2020.csv')
    
    #concat new and current creator_invite_creators (co-authors) dataframes
    frames = [df_creator_invite_creators, df_creator_invite_creators_daily]
    df_creator_invite_creators_updated = pd.concat(frames)
    
    #convert datetime.date to pandas timestamp
    df_creator_invite_creators_updated['email_date'] = pd.to_datetime(df_creator_invite_creators_updated.email_date)
    
    #log current df_creator_invite_creators_updated
    df_creator_invite_creators_updated.to_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_Q2_2020.csv', index=False)
    
    return df_creator_invite_creators_updated



def pull_data(date_beginning_quarter, yesterday, today): 
    
    """
    Pull all relevant data.
    
    Parameters
    ----------
    
    date_beginning_quarter: date
        Date marks beginning of the quarter.

    yesterday: date
        Yesterday's date.
        
    today: date
        Today's date.
       
       
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframes
        Dataframes containing number of New Sign Up Web events, number of New Signup App events, number of Editor events, number of creators invited by other creators to be co-authors for yesterday, and number of creators invited by other creators for My Team for yesterday.
        
        
    """
    
    #pull relevant events from Mixpanel
    df_creators = app_creator_pull()
    df_app_users = app_user_pull()
    df_New_Signup_Web = New_Signup_Web_pull(date_beginning_quarter, yesterday) 
    df_New_Signup_App = New_Signup_App_pull(date_beginning_quarter, yesterday)
    df_Editor = Editor_pull(date_beginning_quarter, yesterday)

    #only keeping legit user IDs
    df_New_Signup_Web = df_New_Signup_Web[df_New_Signup_Web.user_id>1]
    df_New_Signup_App = df_New_Signup_App[df_New_Signup_App.app_user_id>1]
    
    #pull creator invite data from Mandrill
    df_creator_invite_creators_updated = get_creator_invite_creators(yesterday, today)
    df_creator_invite_creators_myteam_updated = get_creator_invite_creators_myteam(yesterday, today)


    return df_creators, df_app_users, df_New_Signup_Web, df_New_Signup_App, df_Editor, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated
   



def process_data(date_beginning_quarter, yesterday, df_New_Signup_Web,df_New_Signup_App, df_creators, df_app_users, df_Editor, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated):
    
    """
    Process data before sending over to Google Sheets.
    
    
    Parameters
    ----------
    
    date_beginning_quarter: date
        Date marks beginning of the quarter.
    
    yesterday: date
        Yesterday's date.
    
    df_New_Signup_Web: dataframe
        Dataframe contains number of New Signup Web events
        
    df_New_Signup_App: dataframe
        Dataframe contains number of New Signup App events
        
    df_creators: dataframe
        Dataframe contains emails, user IDs, and user journey stage of app creators in Mixpanel.
    
    df_app_users: dataframe
        Dataframe contains emails, user IDs, and user journey stage of app users in Mixpanel.
    
    df_Editor: dataframe
        Dataframe contains user IDs, Editor event datetime, and Editor event count for app creators in Mixpanel.
        
    df_creator_invite_creators_updated: dataframe
        Dataframe contains number of creators who were invited by others creators to be co-authors.
    
    df_creator_invite_creators_myteam_updated: dataframe
        Dataframe contains number of creators who were invited by others creators for My Team.
    
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframes
        Dataframes contains Marketing metrics to be update dashboards.
           
    """
    



    #only keeping the first sign up event
    df_New_Signup_Web_final = df_New_Signup_Web.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_Web_final = df_New_Signup_Web_final.drop_duplicates('user_id', keep='first')
    
    #only keeping the first app sign up event
    df_New_Signup_App_final = df_New_Signup_App.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_App_final = df_New_Signup_App_final.drop_duplicates('app_user_id', keep='first')

    #only keep the first editor action event
    df_Editor_final = df_Editor.sort_values('Editor_datetime', ascending=True)
    df_Editor_final = df_Editor_final.drop_duplicates('user_id', keep='first')

    #merge user profiles with sign up events
    df_New_Signup_Web_final = pd.merge(df_New_Signup_Web_final[['user_id','new_sign_up', 'sign_up_datetime']],df_creators, on='user_id', how='left')
    df_New_Signup_Web_final = df_New_Signup_Web_final[~df_New_Signup_Web_final.email.isnull()]
    df_New_Signup_Web_final = df_New_Signup_Web_final[df_New_Signup_Web_final.email!='guest']

    #only keep users with successful sign ups this quarter
    df_New_Signup_App_final = pd.merge(df_New_Signup_App_final[['app_user_id','new_signup_app', 'sign_up_datetime']],df_app_users, on='app_user_id', how='left')
    df_New_Signup_App_final = df_New_Signup_App_final[~df_New_Signup_App_final.email.isnull()]
    df_New_Signup_App_final = df_New_Signup_App_final[df_New_Signup_App_final.email!='guest']
    
    #merge with df users to get creator ID
    df_New_Signup_App_final = pd.merge(df_New_Signup_App_final[['app_user_id','sign_up_datetime','email']], df_creators[['email', 'user_id']], on='email', how='left')
    df_New_Signup_App_final = df_New_Signup_App_final[df_New_Signup_App_final.user_id>1] #only keeping users with valid creator IDs
    
    #use user_id to map with creator event
    df_user_to_creator = pd.merge(df_New_Signup_App_final, df_Editor_final[['user_id','Editor_datetime']], on='user_id', how='left')
    df_user_to_creator = df_user_to_creator[~df_user_to_creator.Editor_datetime.isnull()]
    df_user_to_creator = df_user_to_creator.drop_duplicates('user_id')
    df_user_to_creator = df_user_to_creator[df_user_to_creator['Editor_datetime']>=df_user_to_creator['sign_up_datetime']]
    
    #process creator invite creator data
    df_New_Signup_Web_final_creator_invite_creators = pd.merge(df_New_Signup_Web_final, df_creator_invite_creators_updated[['email','email_date']], on='email', how='left')
    df_New_Signup_Web_final_creator_invite_creators_myteam = pd.merge(df_New_Signup_Web_final, df_creator_invite_creators_myteam_updated[['email','email_date']], on='email', how='left')

    #only keep the ones that did sign up after email was sent
    df_New_Signup_Web_final_creator_invite_creators = df_New_Signup_Web_final_creator_invite_creators[df_New_Signup_Web_final_creator_invite_creators.sign_up_datetime>=df_New_Signup_Web_final_creator_invite_creators.email_date]
    df_New_Signup_Web_final_creator_invite_creators_myteam = df_New_Signup_Web_final_creator_invite_creators_myteam[df_New_Signup_Web_final_creator_invite_creators_myteam.sign_up_datetime>=df_New_Signup_Web_final_creator_invite_creators_myteam.email_date]
    
    #labeling creators who signed up after invitation
    df_New_Signup_Web_final_creator_invite_creators['signup_after_creator_invite'] = 1
    df_New_Signup_Web_final_creator_invite_creators_myteam['signup_after_creator_invite'] = 1
    frames = [df_New_Signup_Web_final_creator_invite_creators, df_New_Signup_Web_final_creator_invite_creators_myteam]
    df_New_Signup_Web_final_creator_invite_creators_final = pd.concat(frames)

    #identify users who signed up after invite
    df_New_Signup_Web_final_creator_invite_creators_final = pd.merge(df_New_Signup_Web_final, df_New_Signup_Web_final_creator_invite_creators_final[['email', 'signup_after_creator_invite']], on='email', how='left')
    df_New_Signup_Web_final_creator_invite_creators_final['signup_after_creator_invite'] = df_New_Signup_Web_final_creator_invite_creators_final.signup_after_creator_invite.fillna(0)
    df_New_Signup_Web_final_creator_invite_creators_final = df_New_Signup_Web_final_creator_invite_creators_final.drop_duplicates('email', keep='first') #only keeping the first sign up
  


    ###Sign Up Count###

    #count number of sign ups per day
    df_New_Signup_Web_final_count = df_New_Signup_Web_final.groupby(['sign_up_datetime']).email.count().reset_index()
    df_New_Signup_Web_final_count = df_New_Signup_Web_final_count.rename(columns={'email': 'consider_events', 'sign_up_datetime': 'date'})

    
    ###Community Referral Count###
    
    #user to creator
    df_user_to_creator_count = df_user_to_creator.groupby(['Editor_datetime']).email.count().reset_index()
    df_user_to_creator_count = df_user_to_creator_count.rename(columns={'email': 'become_creator_events'})

    #creator invite creator
    df_New_Signup_Web_final_creator_invite_creators_final_count = df_New_Signup_Web_final_creator_invite_creators_final.groupby(['sign_up_datetime']).signup_after_creator_invite.sum().reset_index()
    df_New_Signup_Web_final_creator_invite_creators_final_count = df_New_Signup_Web_final_creator_invite_creators_final_count.rename(columns={'sign_up_datetime':'date','signup_after_creator_invite':'creator_invite_creator_signup_events'})
    
    #rename datetime fields to match with main dataframe
    df_user_to_creator_count = df_user_to_creator_count.rename(columns={'Editor_datetime': 'date'})

    #generate date range dataframe
    date_beginning_quarter = date(year = 2020, month = 4, day = 1)
    today = date.today()
    yesterday = today - timedelta(days=1)
    numdays = (yesterday -  date_beginning_quarter).days + 1
    date_list = [pd.Timestamp(yesterday - timedelta(days=x)) for x in range(numdays)]
    data = {'date': date_list}
    df_date_range = pd.DataFrame(data).sort_values('date', ascending=True)

    #merge all dataframes
    df_final = pd.merge(df_date_range,df_user_to_creator_count, on='date', how='left')
    df_final = pd.merge(df_final, df_New_Signup_Web_final_creator_invite_creators_final_count, on='date', how='left')
    df_final = pd.merge(df_final, df_New_Signup_Web_final_count, on='date', how='left')

    #fill days without values as 0
    df_final = df_final.fillna(0)

    print(df_final.columns)
    
    #cumulated sum the necessary metrics
    df_final_cumsum = df_final[['consider_events','become_creator_events','creator_invite_creator_signup_events']].cumsum(axis = 0)
    df_final_cumsum['date'] = df_final['date']
    
    return df_final_cumsum 


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
    main_sheet = client.open("referral_signup_Q2").get_worksheet(0)

    return main_sheet



def update_google_sheet(df_final_cumsum, main_sheet):
    
    """
    
    Update Google Sheet with new data.
    
    Parameters
    ----------
    
    df_final_cumsum: dataframe
        Dataframe with referral signup metric.
    
    main_sheet: Google Sheet object
        Use to access to access referral signup metric Google Sheet.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    None
        Function updates dashboard.
           
    """
    
    #extract data to lists
    referral_list = df_final_cumsum.referral_sign_up.tolist()
    date_list = df_final_cumsum.date.tolist()

    #make updates to Google Sheet
    i = 0 
    while i in range(len(date_list)):
        print(i)
        index = i + 2 #reset index for page

        try:
            #update referral signup
            main_sheet.update_cell(index,2,referral_list[i])
            i += 1
            time.sleep(5)

        except Exception as e:
            print(e)
            time.sleep(10)

    print("Google Sheet has been updated!")
    
    return


def main():
    
    """
    Update Marketing metrics dashboards.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    #send initial email
    send_email("Initiate Referral Metric Update")
    
    #auth into Google Drive and GA Analytics Reporting
    main_sheet = auth_google_services()

    #get date range
    today = date.today()
    yesterday = today - timedelta(days=1)
    date_beginning_quarter = date(year = 2020, month = 4, day = 1)

    print('Start Data Pull')
    df_creators, df_app_users, df_New_Signup_Web, df_New_Signup_App, df_Editor, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated = pull_data(date_beginning_quarter, yesterday,today)
    print('Data Pull Completed')
    
    df_final_cumsum = process_data(date_beginning_quarter, yesterday, df_New_Signup_Web,df_New_Signup_App, df_creators, df_app_users, df_Editor, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated)
    
    print('Data Prep Completed')

    #store current data
    df_final_cumsum.to_csv('df_final_cumsum.csv', index=False)
    
    #get total referral signups
    df_final_cumsum['referral_sign_up'] = df_final_cumsum['become_creator_events'] + df_final_cumsum['creator_invite_creator_signup_events']
    
    #only keep necessary columns to update
    df_final_cumsum_to_update = df_final_cumsum[['date','referral_sign_up']]

    #update Google Sheet
    update_google_sheet(df_final_cumsum, main_sheet)

    #send email when completed
    send_email("Referral Metric Update Completed")

    return df_final_cumsum


 
#execute update
if __name__ == "__main__":
    main()


