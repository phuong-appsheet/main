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
google_drive_client_secrets = os.path.expanduser(os.environ.get('GD_CLIENT_SECRETS'))
google_analytics_client_secrets = os.path.expanduser(os.environ.get('GA_CLIENT_SECRETS'))
mandrill_api_key = os.environ.get('MANDRILL_KEY')
mandrill_client = mandrillpt.Mandrill(mandrill_api_key)



def auth_google_services():
    
    """
    Authenticate to Google Drive and GA Analytics Report.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    google_drive_client_secrets: str
        Client secret used to make calls to Google Drive API.
        
    google_analytics_client_secrets: str
        Client secret used to make calls to GA Analytics Report Service.
    
    Returns
    ----------
    
    acquisition_sheet: Google Sheet Object
    
    adoption_sheet: Google Sheet Object
    
    credentials: GA Client
    
        Objects to make pull data from GA and to update dashboards in Google Sheets.
        
    """
    
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_drive_client_secrets, scope)
    client = gspread.authorize(creds)

    # accessing Google Sheets
    acquisition_sheet = client.open("Dashboard").get_worksheet(0)
    adoption_sheet = client.open("Dashboard").get_worksheet(1)

    #credentials for GA
    credentials = service_account.Credentials.from_service_account_file(
            google_analytics_client_secrets,
            scopes=['https://www.googleapis.com/auth/analytics.readonly'],
        )
    
    
    return acquisition_sheet, adoption_sheet, credentials



def user_pull():

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
                    "e.properties.$username",
                    "e.properties.user_journey_stage"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    user_id_list = []
    user_journey_stage_list = []
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])
        user_journey_stage_list.append(row['key'][2])

    #create dataframe 
    data = {'email':email_list, 'user_id': user_id_list, 'user_journey_stage': user_journey_stage_list}
    df_users = pd.DataFrame(data=data)
    return df_users


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
                    "new Date(e.time).toISOString()",
                    "e.properties.mp_country_code"
                ],
                accumulator=Reducer.count()
            )

    #store emails, user IDs, sign up datetime, and country
    user_id_list = []
    datetime_list = []
    new_sign_up_list = []
    country_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            country_list.append(row['key'][2])
            new_sign_up_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list,'sign_up_datetime': datetime_list,'new_sign_up': new_sign_up_list, 'country': country_list}
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


def generate_commit_mau(df_AppStart_mau, df_Editor_mau, yesterday):
    
    """
    Calculate number of monthly active app creators.
    
    Parameters
    ----------
    
    df_AppStart_mau: dataframe
        Dataframe containing app creator user IDs, AppStart event count, and AppStart event datetime in the past 30 days.
        
    df_Editor_mau: dataframe
        Dataframe containing app creator user IDs, Editor event count, and Editor event datetime in the past 30 days.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and number of active app creators (i.e. either did AppStart or Editor event) in the past 30 days. Dataframe only contains one row that would be appended to main df_commit_mau that is stored in an external file.
           
    """
    
    #rename columns in dataframes
    df_AppStart_mau = df_AppStart_mau.rename(columns={'AppStart_datetime': 'date', 'AppStart': 'event'})
    df_Editor_mau = df_Editor_mau.rename(columns={'Editor_datetime': 'date', 'Editor': 'event'})


    #join AppStart and Editor dataframes. Removing duplicate user IDs because a creator who did either event would be considered "active"
    frames = [df_AppStart_mau, df_Editor_mau]
    df_active = pd.concat(frames)
    df_active = df_active.drop_duplicates(['user_id', 'date']).sort_values('date', ascending=True)

    #initiate lists to store end dates and number of active creators 
    end_date_list = []
    mau_list = []

    #generate date range
    end_date = yesterday
    start_date = end_date - timedelta(days=30) # 30 day rolling window. Mixpanel uses 28 days rolling

    #only looking at data for the past 30 days
    df_filtered = df_active[(df_active.date<end_date)&(df_active.date>=start_date)]
    df_filtered = df_filtered.drop_duplicates('user_id') #get unique events by users for past 30 days
    mau_list.append(len(df_filtered))
    end_date_list.append(end_date)

    #generate dataframe
    data = {'date': end_date_list, 'commit_mau': mau_list}
    df_commit_mau = pd.DataFrame(data)
    
    return df_commit_mau

def generate_commit_dau(df_AppStart_mau, df_Editor_mau, df_commit_mau):
    
    """
    
    Calculate number of daily active app creators.
    
    
    Parameters
    ----------
    
    df_AppStart_mau: dataframe
        Dataframe containing app creator user IDs, AppStart event count, and AppStart event datetime in the past 30 days.
        
    df_Editor_mau: dataframe
        Dataframe containing app creator user IDs, Editor event count, and Editor event datetime in the past 30 days.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and number of active daily app creators (i.e. either did AppStart or Editor event).
           
    """
    
    #calculating monthy active users
    df_AppStart_mau = df_AppStart_mau.rename(columns={'AppStart_datetime': 'date', 'AppStart': 'event'})
    df_Editor_mau = df_Editor_mau.rename(columns={'Editor_datetime': 'date', 'Editor': 'event'})


    #only need to keep one event for user per day
    frames = [df_AppStart_mau, df_Editor_mau]
    df_active = pd.concat(frames)

    df_active = df_active.drop_duplicates(['user_id', 'date']).sort_values('date', ascending=True)

    #initiate list to store number of daily active creators  
    dau_list = []

    #calculate the number of active users for each date
    for date_val in df_commit_mau.date.tolist():
        df_filtered = df_active[df_active.date==date_val]
        df_filtered = df_filtered.drop_duplicates('user_id') #get unique events by users for past 30 days
        dau_list.append(len(df_filtered))

    #generate dataframe
    data = {'date': df_commit_mau.date.tolist(), 'commit_dau': dau_list}
    df_commit_dau = pd.DataFrame(data)
    
    return df_commit_dau



def AppStart_user_pull(yesterday):

    """
    
    Pull app user AppStart events in Mixpanel.
    
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
    api_user_secret: str
        Client secret used to make calls to Mixpanel User Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains device IDs, AppStart event datetime, and AppStart event count for app users in Mixpanel.
           
    """
    
    #generate JQL query
    query = JQL(
        api_user_secret,
        events=Events({
                'event_selectors': [{'event': "AppStart"}],
                'from_date': yesterday,
                'to_date': yesterday
            })).group_by(
                keys=[
                    "e.properties.$device_id",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )

    #store device IDs, AppStart count, and AppStart datetime
    device_id_list = []
    datetime_list = []
    AppStart_list = []
    for row in query.send():
        if row['key'][0] is not None:
            device_id_list.append(row['key'][0])
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            AppStart_list.append(row['value'])

    #generate dataframe
    data = {'device_id':device_id_list,'AppStart_datetime': datetime_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)
    
    return df_AppStart



def generate_promote_mau(df_AppStart_user_mau, yesterday):
    
    """
    Calculate number of monthly active app users.
    
    
    Parameters
    ----------
    
    df_AppStart_user_mau: dataframe
        Dataframe containing app user device IDs, AppStart event count, and AppStart event date in the past 30 days.
        
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and number of active monthly app users.
           
    """
    
    #removing duplicate device ID and date combinations since one AppStart within the same day would consider the user as "active"
    df_active = df_AppStart_user_mau
    df_active = df_active.drop_duplicates(['device_id', 'date']).sort_values('date', ascending=True)

    #initializing end date and monthly active app user list
    end_date_list = []
    mau_list = []

    #get date range
    end_date = yesterday
    start_date = end_date - timedelta(days=30) # 30 day rolling window. Mixpanel uses 28 days rolling

    #calculating monthly active app users
    df_filtered = df_active[(df_active.date<end_date)&(df_active.date>=start_date)]
    df_filtered = df_filtered.drop_duplicates('device_id') #get unique events by users for past 30 days
    mau_list.append(len(df_filtered))
    end_date_list.append(end_date)

    #generate dataframe
    data = {'date': end_date_list, 'promote_mau': mau_list}
    df_promote_mau = pd.DataFrame(data)
    
    return df_promote_mau

def generate_promote_dau(df_AppStart_user_mau, df_promote_mau):
    
    """
    Calculate number of daily active app users.
    
    Parameters
    ----------
    
    df_AppStart_user_mau: dataframe
        Dataframe containing app user device IDs, AppStart event count, and AppStart event date in the past 30 days.
        
    df_promote_mau: dataframe
        Dataframe containing date and number of active monthly app users. Only inputed to keep track of the dates for merge after outputting the resulting dataframe.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and number of active daily app users.
           
    """
    
    #removing duplicate device ID and date combinations since one AppStart within the same day would consider the user as "active"
    df_active = df_AppStart_user_mau
    df_active = df_active.drop_duplicates(['device_id', 'date']).sort_values('date', ascending=True)

    #initializing dau list
    dau_list = []

    #calculate daily active app users
    for date_val in df_promote_mau.date.tolist():
        df_filtered = df_active[df_active.date==date_val]
        df_filtered = df_filtered.drop_duplicates('device_id') #get unique events by users for past 30 days
        dau_list.append(len(df_filtered))

    #generate dataframe
    data = {'date': df_promote_mau.date.tolist(), 'promote_dau': dau_list}
    df_promote_dau = pd.DataFrame(data)
    
    return df_promote_dau
    
def get_retention_data(yesterday):
    
    """
    
    Pull app creator retention data.
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and one week retention rate.
           
    """
    
    #authenticate to Mixpanel Creator Project
    enc = base64.b64encode(bytes(api_creator_secret, encoding='utf-8')).decode("ascii")
    headers = {f'Authorization': 'Basic {enc}'}

    #generate post call parameters
        #$custom_event:846097 - True Sign Up Web
        #$custom_event:1077769 - Editor and AppStart
    data = {
        'from_date': '2019-09-30',
        'to_date': yesterday,
        'born_event': "$custom_event:846097",
        'event': '$custom_event:1077769',
        'format': 'json',
        'interval': 7
              }

    #make post call
    response = requests.post('https://mixpanel.com/api/2.0/retention/', data=data, auth=HTTPBasicAuth(api_creator_secret,''))

    #initialize date and retention rate lists
    date_list = []
    retention_rate_list = []

    #process json response
    report = response.json()
    for date_val in report.keys():
        print(report[date_val])
        if len(report[date_val]['counts']) > 1:
            retention_rate = report[date_val]['counts'][1]/report[date_val]['first']
            retention_rate_list.append(retention_rate)
            if date_val == '2019-09-30': #adjust to match with quarter data
                date_list.append(datetime.strptime(date_val, '%Y-%m-%d') + timedelta(days=1))
            else:
                date_list.append(datetime.strptime(date_val, '%Y-%m-%d'))

    #generate dataframe
    data = {'date': date_list, 'retention_rate': retention_rate_list}
    df_retention = pd.DataFrame(data)
    df_retention = df_retention.sort_values('date', ascending=True)

    #find two week lag and only keep time period before this for complete retention data
    today = date.today()
    three_week_lag = today - timedelta(days=20) 
    df_retention = df_retention[df_retention.date<three_week_lag]
    
    return df_retention


def get_attract_count(from_date, to_date, credentials):
    
    """
    Get number of new users from yesterday in Google Analytics.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
       
    to_date: date
        End date of query.
        
    credentials: GA Client
        Used to make API calls to GA Analytics Reports
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains date and number of new visitors.
           
    """
    

    #build analyticsreporting object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    #pull report
    report = analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': '91335443',
              'metrics': [{'expression': 'ga:newUsers'}],
              'dateRanges': [{'startDate': from_date, 'endDate': to_date}],
              'dimensions': [{'name': 'ga:date'}]
            }]
          }
      ).execute()

    #initialize date and number of new visitors lists
    attract_datetime_list = []
    attract_events_list = []
    
    #proccess report
    for row in report['reports'][0]['data']['rows']:
        
        #get value:
        value = int(row['metrics'][0]['values'][0])

        #get date
        date = row['dimensions'][0]
        year = date[:4]
        month = date[4:6]
        day = date[6:8]
        final_date = datetime.strptime(year + '-' + month + '-' +day, '%Y-%m-%d') 

        #record date and number of new visitors
        attract_datetime_list.append(final_date)
        attract_events_list.append(value)

    #generate dataframe
    data = {'attract_events': attract_events_list, 'date': attract_datetime_list}
    df_Attract_count = pd.DataFrame(data)
    
    return df_Attract_count
    
def get_hubspot_deals(yesterday):
    
    """
    Pull number of deals created and deal values.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
       
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    df_deal_current_quarter_day_count: dataframe
        Dataframe contains date and number of deals created.
    
    df_deal_current_quarter_day_average: dataframe
        Dataframe contains date and cumulative average deal size for the quarter.
           
    """
    
    #set parameters to call Hubspot deal service 
    limit = 20
    deal_id_list = []
    get_all_deals_url = "https://api.hubapi.com/deals/v1/deal/paged?"
    parameter_dict = {'hapikey': hubspot_key, 'limit': limit}
    headers = {}

    #paginate request using offset
    has_more = True
    while has_more:
        parameters = urllib.parse.urlencode(parameter_dict)
        get_url = get_all_deals_url + parameters
        
        try: #maintain loop if hit api limit
            r = requests.get(url= get_url, headers = headers)
            response_dict = json.loads(r.text)
            has_more = response_dict['hasMore']

            #loop through list of return deals
            deals = response_dict['deals']
            for deal in deals:
                deal_id_list.append(deal['dealId'])
            parameter_dict['offset']= response_dict['offset']
        except Exception as e:
            print(parameter_dict['offset'])
            time.sleep

    #initiate deal ID, deal value, and date lists
    deal_id_list_final = []
    deal_value_list = []
    time_value_list = []

    
    #process response
    i=0
    while i < len(deal_id_list):
        deal_id = deal_id_list[i]

        try:
            #service to hit
            url = "https://api.hubapi.com/deals/v1/deal/" + str(deal_id) + "?hapikey=" + hubspot_key

            #pull deal information
            r= requests.get(url = url)
            deal = r.json()
        except:
            time.sleep(10)

        #deal value
        deal_keys = deal['properties'].keys()
        deal_value = 0
        if 'hs_closed_amount_in_home_currency' in deal_keys:
            deal_value_temp = float(deal['properties']['hs_closed_amount_in_home_currency']['value'])
            if deal_value_temp>0:
                deal_value = deal_value_temp
        if ('amount_in_home_currency' in deal_keys) & (deal_value == 0):
            value = deal['properties']['amount_in_home_currency']['value']
            if value!='':
                deal_value = float(value)

        #time stamp
        timestamp = ''.join(deal['properties']['createdate']['value'].split())[:-3]
        if timestamp != '':
            time_value = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
            time_value = datetime.strptime(time_value[:10], '%Y-%m-%d')

        #store deal id, value, and create date
        deal_value_list.append(deal_value)
        time_value_list.append(time_value)
        deal_id_list_final.append(deal_id)
        i+=1


    #generate deal dataframe
    data = {'deal_id': deal_id_list_final, 'deal_value': deal_value_list, 'created_date': time_value_list}
    df_deal = pd.DataFrame(data)
    
    #only want deals created after 10/1/2019 and up until yesterday
    df_deal_current_quarter = df_deal[(df_deal.created_date>=date(year = 2019, month = 10, day = 1))&
                                     (df_deal.created_date<=yesterday)]

    #only count deals above $0
    df_deal_current_quarter = df_deal_current_quarter[df_deal_current_quarter.deal_value>0]

    #count the number of deals per day
    df_deal_current_quarter_day_count = df_deal_current_quarter.groupby(['created_date']).deal_id.count().reset_index()
    df_deal_current_quarter_day_count = df_deal_current_quarter_day_count.rename(columns={'deal_id': 'deal_num', 'created_date': 'date'})

    #calculate average deal value per day and generate dataframe
    df_deal_current_quarter_day_average = df_deal_current_quarter.groupby(['created_date']).deal_value.mean().reset_index()
    df_deal_current_quarter_day_average = df_deal_current_quarter_day_average.rename(columns={'deal_value': 'deal_value_average', 'created_date': 'date'})
    df_deal_current_quarter_day_average['deal_value_expanding_mean'] = df_deal_current_quarter_day_average['deal_value_average'].expanding().mean() #get expanding mean


    return df_deal_current_quarter_day_count, df_deal_current_quarter_day_average


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
    
  
    
    #initialize template, email address, subject, and date lists
    template_list = []
    to_email_list = []
    subject_list = []
    date_list = []
    
    #generate timestamp range for query
    ts_start = datetime.fromordinal(yesterday.toordinal()).timestamp()
    ts_end_today = datetime.fromordinal(today.toordinal()).timestamp()
    
    #generage query calls until timestamp range is met using while loop and one hour increments since there is a 1000 object limit
    while ts_start < ts_end_today:
        
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
    df_creator_invite_creators_myteam_hourly = pd.DataFrame(data)
    df_creator_invite_creators_myteam_hourly = df_creator_invite_creators_myteam_hourly[df_creator_invite_creators_myteam_hourly.template=='final-invitetoteam']

    #load current creator_invite_creators_myteam
    df_creator_invite_creators_myteam = pd.read_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_myteam.csv')
    
    #concat current and new creator_invite_creators_myteam dataframes
    frames = [df_creator_invite_creators_myteam, df_creator_invite_creators_myteam_hourly]
    df_creator_invite_creators_myteam_updated = pd.concat(frames)
    
    #convert datetime.date to pandas timestamp
    df_creator_invite_creators_myteam_updated['email_date'] = pd.to_datetime(df_creator_invite_creators_myteam_updated.email_date)
    
    #log new df_creator_invite_creators_myteam_updated
    df_creator_invite_creators_myteam_updated.to_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators_myteam.csv', index=False)
    
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
    ts_end_today = datetime.fromordinal(today.toordinal()).timestamp()
    
    #generage query calls until timestamp range is met using while loop and one hour increments since there is a 1000 object limit
    while ts_start < ts_end_today:
        
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
    df_creator_invite_creators_hourly = pd.DataFrame(data)
    df_creator_invite_creators_hourly = df_creator_invite_creators_hourly[df_creator_invite_creators_hourly.template=='collaborator-invite']

    #load current creator_invite_creators as co-authors
    df_creator_invite_creators = pd.read_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators.csv')
    
    #concat new and current creator_invite_creators (co-authors) dataframes
    frames = [df_creator_invite_creators, df_creator_invite_creators_hourly]
    df_creator_invite_creators_updated = pd.concat(frames)
    
    #convert datetime.date to pandas timestamp
    df_creator_invite_creators_updated['email_date'] = pd.to_datetime(df_creator_invite_creators_updated.email_date)
    
    #log current df_creator_invite_creators_updated
    df_creator_invite_creators_updated.to_csv('/home/phuong/automated_jobs/dashboard/creator_invite_creators.csv', index=False)
    
    return df_creator_invite_creators_updated



def pull_data(date_beginning_quarter, prior_30_days, yesterday, today): 
    
    """
    Pull all relevant data.
    
    Parameters
    ----------
    
    date_beginning_quarter: date
        Date marks beginning of the quarter.
        
    prior_30_days: date
        Date marks 30 days ago.
        
    yesterday: date
        Yesterday's date.
        
    today: date
        Today's date.
       
       
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframes
        Dataframes containing number of New Sign Up Web events, number of New Signup App events, number of Editor events, number of AppStart events broken out by date for the past 30 days, number of Editor events broken out by date for the past 30 days, number of creators invited by other creators to be co-authors for yesterday, and number of creators invited by other creators for My Team for yesterday.
        
        
    """
    
    #pull relevant events from Mixpanel
    df_users = user_pull()
    df_app_users = app_user_pull()
    df_New_Signup_Web = New_Signup_Web_pull(date_beginning_quarter, yesterday) #pull new sign up since yesterday
    df_New_Signup_App = New_Signup_App_pull(date_beginning_quarter, yesterday)
    df_Editor = Editor_pull(date_beginning_quarter, yesterday)
    df_AppStart_mau = AppStart_pull(prior_30_days, yesterday)
    df_Editor_mau = Editor_pull(prior_30_days, yesterday)
    

    #only keeping legit user IDs
    df_New_Signup_Web = df_New_Signup_Web[df_New_Signup_Web.user_id>1]
    df_New_Signup_App = df_New_Signup_App[df_New_Signup_App.app_user_id>1]
    
    #pull creator invite data from Mandrill
    df_creator_invite_creators_updated = get_creator_invite_creators(yesterday, today)
    df_creator_invite_creators_myteam_updated = get_creator_invite_creators_myteam(yesterday, today)


    return df_users, df_app_users, df_New_Signup_Web, df_New_Signup_App, df_Editor, df_AppStart_mau, df_Editor_mau, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated
   
def get_active_users(yesterday):
    
    """
    Calculate number of active app users from yesterday.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains device IDs, dates, and number of active users.
           
    """
    
    #load current user AppStart data and remove oldest date to limit data size
    df_AppStart_user_mau_current = pd.read_csv('/home/phuong/automated_jobs/dashboard/user_mau_recent_final.csv')
    df_AppStart_user_mau_current.date = df_AppStart_user_mau_current.date.astype('datetime64[ns]') #conver to datetime
    earliest_date = min(df_AppStart_user_mau_current.drop_duplicates('date').date.tolist())
    df_AppStart_user_mau_current = df_AppStart_user_mau_current[df_AppStart_user_mau_current.date!=earliest_date]

    #only pull user AppStarts for yesterday
    df_user_Appstart_yesterday = AppStart_user_pull(yesterday)
    df_user_Appstart_yesterday = df_user_Appstart_yesterday.rename(columns = {'AppStart_datetime': 'date'})

    #merge yesterday with current 
    frames = [df_AppStart_user_mau_current, df_user_Appstart_yesterday]
    df_AppStart_user_mau_updated = pd.concat(frames)
    df_AppStart_user_mau_updated = df_AppStart_user_mau_updated.drop_duplicates(['device_id','date'])
    
    #update current AppStart csv
    df_AppStart_user_mau_updated.to_csv('/home/phuong/automated_jobs/dashboard/user_mau_recent_final.csv', index=False)
    
    return df_AppStart_user_mau_updated


def process_data(date_beginning_quarter, yesterday, df_New_Signup_Web,df_New_Signup_App, df_users, df_app_users, df_Editor, df_AppStart_mau, df_Editor_mau, df_AppStart_user_mau_updated, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated, credentials):
    
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
        
    df_users: dataframe
        Dataframe contains emails, user IDs, and user journey stage of app creators in Mixpanel.
    
    df_app_users: dataframe
        Dataframe contains emails, user IDs, and user journey stage of app users in Mixpanel.
    
    df_Editor: dataframe
        Dataframe contains user IDs, Editor event datetime, and Editor event count for app creators in Mixpanel.
        
    df_AppStart_mau: dataframe
        Dataframe contains date and number of AppStart events in past 30 days.
        
    df_Editor_mau: dataframe
        Dataframe contains date and number of Editor events in past 30 days.
        
    df_AppStart_user_mau_updated: dataframe
        Dataframe contains date and number of monthly active app users.
    
    df_creator_invite_creators_updated: dataframe
        Dataframe contains number of creators who were invited by others creators to be co-authors.
    
    df_creator_invite_creators_myteam_updated: dataframe
        Dataframe contains number of creators who were invited by others creators for My Team.
    
    credentials: GA Client
        Used to make API calls to GA Analytics Reports
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    dataframes
        Dataframes contains Marketing metrics to be update dashboards.
           
    """
    

    #date filter
    df_New_Signup_Web_GB_US_CA = df_New_Signup_Web[df_New_Signup_Web.country.isin(['GB','US', 'CA'])]
    
    #only keeping the first sign up event
    df_New_Signup_Web_GB_US_CA_final = df_New_Signup_Web_GB_US_CA.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_Web_GB_US_CA_final = df_New_Signup_Web_GB_US_CA_final.drop_duplicates('user_id', keep='first')
    
    #specify GB, US, and CA dates
    df_New_Signup_Web_GB_US_CA_final = df_New_Signup_Web_GB_US_CA_final.rename(columns={'sign_up_datetime': 'GB_US_CA_sign_up_datetime'})


    #only keeping the first sign up event
    df_New_Signup_Web_final = df_New_Signup_Web.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_Web_final = df_New_Signup_Web_final.drop_duplicates('user_id', keep='first')
    
    #only keeping the first app sign up event
    df_New_Signup_App_final = df_New_Signup_App.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_App_final = df_New_Signup_App_final.drop_duplicates('app_user_id', keep='first')

    #only keep the first editor action event
    df_Editor_final = df_Editor.sort_values('Editor_datetime', ascending=True)
    df_Editor_final = df_Editor.drop_duplicates('user_id', keep='first')

 
    #successful merging with sign up events
    df_New_Signup_Web_final = pd.merge(df_New_Signup_Web_final[['user_id','new_sign_up', 'sign_up_datetime']],df_users, on='user_id', how='left')
    df_New_Signup_Web_final = pd.merge(df_New_Signup_Web_final,df_New_Signup_Web_GB_US_CA_final[['user_id', 'GB_US_CA_sign_up_datetime']], on='user_id', how='left')
    df_New_Signup_Web_final = df_New_Signup_Web_final[~df_New_Signup_Web_final.email.isnull()]
    df_New_Signup_Web_final = df_New_Signup_Web_final[df_New_Signup_Web_final.email!='guest']


    #only keep users with successfull sign ups this quarter
    df_New_Signup_App_final = pd.merge(df_New_Signup_App_final[['app_user_id','new_signup_app', 'sign_up_datetime']],df_app_users, on='app_user_id', how='left')
    df_New_Signup_App_final = df_New_Signup_App_final[~df_New_Signup_App_final.email.isnull()]
    df_New_Signup_App_final = df_New_Signup_App_final[df_New_Signup_App_final.email!='guest']
    
    #merge with df users to get creator ID
    df_New_Signup_App_final = pd.merge(df_New_Signup_App_final[['app_user_id','sign_up_datetime','email']], df_users[['email', 'user_id']], on='email', how='left')
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
  

     

    ###Attract Count###

    #only one row
    df_Attract_count = get_attract_count(str(date_beginning_quarter), str(yesterday), credentials) #have to convert datetime to string


    ###Sign Up Count###

    #count number of sign ups per day
    df_New_Signup_Web_final_count = df_New_Signup_Web_final.groupby(['sign_up_datetime']).email.count().reset_index()
    df_New_Signup_Web_final_count = df_New_Signup_Web_final_count.rename(columns={'email': 'consider_events'})



    ###Sign Up GB, US, and CA Count###

    #only keep contacts with GB, US, and CA  sign up
    df_New_Signup_Web_GB_US_CA = df_New_Signup_Web_final[~df_New_Signup_Web_final.GB_US_CA_sign_up_datetime.isnull()]

    #count number of sign ups per day
    df_New_Signup_Web_GB_US_CA_count = df_New_Signup_Web_GB_US_CA.groupby(['GB_US_CA_sign_up_datetime']).email.count().reset_index()
    df_New_Signup_Web_GB_US_CA_count = df_New_Signup_Web_GB_US_CA_count.rename(columns={'email': 'consider_GB_US_CA_events'})

    
    
    
    ###Community Referral Count###
    
    #user to creator
    df_user_to_creator_count = df_user_to_creator.groupby(['Editor_datetime']).email.count().reset_index()
    df_user_to_creator_count = df_user_to_creator_count.rename(columns={'email': 'become_creator_events'})

    #creator invite creator
    df_New_Signup_Web_final_creator_invite_creators_final_count = df_New_Signup_Web_final_creator_invite_creators_final.groupby(['sign_up_datetime']).signup_after_creator_invite.sum().reset_index()
    df_New_Signup_Web_final_creator_invite_creators_final_count = df_New_Signup_Web_final_creator_invite_creators_final_count.rename(columns={'sign_up_datetime':'creator_invite_creator_signup_datetime','signup_after_creator_invite':'creator_invite_creator_signup_events'})
    
    
    ###Editor Retention###
    df_retention = get_retention_data(yesterday)

    ###Commit Count###
    #only keep users who signed up on or after 2019-10-01 
    df_Commit = df_users[(df_users.user_journey_stage=='Commit')|(df_users.user_journey_stage=='Convert')]

    #for the first batch we will just group commit stage users to the month they signed up
    Commit_count = len(df_Commit)

    #generate commit total dataframe
    data = {'date': [yesterday], 'Commit_count': Commit_count}
    df_commit_total = pd.DataFrame(data)

    #mau and dau
    df_commit_mau = generate_commit_mau(df_AppStart_mau, df_Editor_mau, yesterday)
    df_commit_dau = generate_commit_dau(df_AppStart_mau, df_Editor_mau, df_commit_mau)
    #get yesterday commit mau and dau
    commit_mau = df_commit_mau.commit_mau.tolist()[0]
    commit_dau = df_commit_dau.commit_dau.tolist()[0]

    ###Convert count###
    #get deals metrics: can pull all time since deal size can change
    df_deal_current_quarter_day_count, df_deal_current_quarter_day_average = get_hubspot_deals(yesterday)

    ###Promote Count###
    #since we are pulling from device ID, we could double count users who are on multiple devices
    #however, this will allow us to capture public users (users without account)
    #different from numbers in mixpanel report because they only count users with IDs (so undercounting)
    df_promote_mau = generate_promote_mau(df_AppStart_user_mau_updated, yesterday)
    df_promote_dau = generate_promote_dau(df_AppStart_user_mau_updated, df_promote_mau)
    promote_mau = df_promote_mau.promote_mau.tolist()[0]
    promote_dau = df_promote_dau.promote_dau.tolist()[0]

    df_New_Signup_Web_final_count = df_New_Signup_Web_final_count.rename(columns={'sign_up_datetime': 'date'})
    df_New_Signup_Web_GB_US_CA_count = df_New_Signup_Web_GB_US_CA_count.rename(columns={'GB_US_CA_sign_up_datetime': 'date'})
    df_user_to_creator_count = df_user_to_creator_count.rename(columns={'Editor_datetime': 'date'})
    df_New_Signup_Web_final_creator_invite_creators_final_count = df_New_Signup_Web_final_creator_invite_creators_final_count.rename(columns={'creator_invite_creator_signup_datetime': 'date'})

    df_final = pd.merge(df_New_Signup_Web_final_count, df_New_Signup_Web_GB_US_CA_count, on='date', how='left')
    df_final = pd.merge(df_final, df_Attract_count, on='date', how='left')
    df_final = pd.merge(df_final, df_deal_current_quarter_day_count[['date','deal_num']] , on='date', how='left')
    df_final = pd.merge(df_final, df_user_to_creator_count, on='date', how='left')
    df_final = pd.merge(df_final, df_New_Signup_Web_final_creator_invite_creators_final_count, on='date', how='left')
    # fill days without milestone event as 0
    df_final = df_final.fillna(0)

    #cumulated sum the necessary metrics
    df_final_cumsum = df_final[['consider_events','consider_GB_US_CA_events',
           'attract_events','deal_num', 'become_creator_events','creator_invite_creator_signup_events']].cumsum(axis = 0)
    df_final_cumsum['date'] = df_final['date']
    #merge deal size
    df_final_cumsum = pd.merge(df_final_cumsum, df_deal_current_quarter_day_average[['date','deal_value_expanding_mean']], on='date', how='left')



    #these fields are can be extracted

    commit_dau_mau = commit_dau/commit_mau
    promote_dau_mau = promote_dau/promote_mau

    df_final_cumsum.to_csv('/home/phuong/automated_jobs/dashboard/current_final_dashboard_updated.csv', index=False)

    return df_final_cumsum, Commit_count, commit_dau_mau, promote_mau, promote_dau_mau, df_retention
            

def update_google_sheet(df_final_cumsum, Commit_count, commit_dau_mau, promote_mau, promote_dau_mau, df_retention, acquisition_sheet, adoption_sheet, yesterday):
    
    """
    
    Update Google Sheet with new data.
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
    
    df_final_cumsum: dataframe
        Dataframe with Marketing metrics.
    
    Commit_count: int
        Number of app creators who are in the Commit and Convert stages.
    
    commit_dau_mau: float
        Rate of app creator dau/mau.
        
    promote_mau: int
        Number of monthly active app users.
    
    promote_dau_mau: float
        Rate of app users dau/mau
    
    df_retention: dataframe
        Dataframe containing dates and retention rate.
    
    acquisition_sheet: Google Sheet object
        Use to access to access acquisition metric Google Sheet.
    
    adoption_sheet: Google Sheet object
        Use to access to access adoption metric Google Sheet.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    None
        Function updates dashboard.
           
    """
    
    #push data to google sheets
    attract_list = df_final_cumsum.attract_events.tolist()
    consider_list = df_final_cumsum.consider_events.tolist()
    consider_GB_US_CA_list = df_final_cumsum.consider_GB_US_CA_events.tolist()
    become_creator_events_list = df_final_cumsum.become_creator_events.tolist()
    creator_invite_creator_signup_events_list = df_final_cumsum.creator_invite_creator_signup_events.tolist()
    deals_created_list = df_final_cumsum.deal_num.tolist()
    deal_value_expanding_mean_list = df_final_cumsum.deal_value_expanding_mean.tolist()
    date_list = df_final_cumsum.date.tolist()

    #make updates to Google Sheets
    i = 0 
    while i in range(len(date_list)):
        print(i)
        index = i + 2 #reset index for page

        try:
            #update Attract
            acquisition_sheet.update_cell(index,2,attract_list[i])

            #update Consider
            acquisition_sheet.update_cell(index,4,consider_list[i])

            #update Consider GB, US, CA
            acquisition_sheet.update_cell(index,5,consider_GB_US_CA_list[i])
            
            #update community attributed signups
            acquisition_sheet.update_cell(index,7,become_creator_events_list[i])
            acquisition_sheet.update_cell(index,8,creator_invite_creator_signup_events_list[i])

            #update Commit and Promote active users and dau_mau only if date is yesterday
            if str(date_list[i])[:10] == str(yesterday): #only add total commit users values when it gets to today
                adoption_sheet.update_cell(index,2,Commit_count)
                adoption_sheet.update_cell(index,3,commit_dau_mau)
                adoption_sheet.update_cell(index,8,promote_mau)
                adoption_sheet.update_cell(index,9,promote_dau_mau)

            #update Convert number of deals created and average deal size
            adoption_sheet.update_cell(index,5,deals_created_list[i])

            if deal_value_expanding_mean_list[i]>0: #only add if deal aeverage is above 0
                adoption_sheet.update_cell(index,6,deal_value_expanding_mean_list[i])
            i += 1
            time.sleep(5)

        except Exception as e:
            print(e)
            time.sleep(10)





    #update editor retention data in Google Sheet
    i = 0 

    explore_retention_list = df_retention.retention_rate.tolist()

    while i in range(len(explore_retention_list)):
        index = i + 2 #reset index for page
        
        try:
            #update Explore retention
            adoption_sheet.update_cell(index,26,explore_retention_list[i])
            time.sleep(5)
            i += 1
        
        except Exception as e:
            print(e)
            time.sleep(10)
    print("Dashboard has been updated!")
    
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
    send_email("Initiate Dashboard Update")
    
    #auth into Google Drive and GA Analytics Reporting
    acquisition_sheet, adoption_sheet, credentials = auth_google_services()
    
    #get date range
    today = date.today()
    date_since = date(year = 2014, month = 8, day = 8)
    date_since_user_project = date(year = 2017, month = 3, day = 1)
    yesterday = today - timedelta(days=1)
    prior_30_days = yesterday - timedelta(days=30)

    #beginning of quarter
    date_beginning_quarter = date(year = 2019, month = 10, day = 1)

    #since month prior
    month_prior = date(year = 2019, month = 9, day = 1)


    #get mixpanel data
    df_users, df_app_users, df_New_Signup_Web, df_New_Signup_App, df_Editor, df_AppStart_mau, df_Editor_mau, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated = pull_data(date_beginning_quarter, prior_30_days, yesterday,today)

    #get active app users
    df_AppStart_user_mau_updated = get_active_users(yesterday)

    #process data before pushing to Google Sheet
    df_final_cumsum, Commit_count, commit_dau_mau, promote_mau, promote_dau_mau, df_retention = process_data(date_beginning_quarter, yesterday, df_New_Signup_Web, df_New_Signup_App, df_users, df_app_users, df_Editor, df_AppStart_mau, df_Editor_mau, df_AppStart_user_mau_updated, df_creator_invite_creators_updated, df_creator_invite_creators_myteam_updated, credentials)

    #update google sheet
    update_google_sheet(df_final_cumsum, Commit_count, commit_dau_mau, promote_mau, promote_dau_mau, df_retention, acquisition_sheet, adoption_sheet, yesterday)
   
    #send email when completed
    send_email("Dashboard Update Completed")
    
    
    return 


 
#execute update
if __name__ == "__main__":
    main()


