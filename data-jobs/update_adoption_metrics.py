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
baremetrics_key = os.environ.get('BAREMETRICS_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY') 

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

def update_google_sheet(today_new_signup, today_total_number_of_active_creators, today_monthly_active_app_users, num_active_apps, num_of_active_app_users_by_num_of_active_apps, arr, num_deals, deal_value, to_date_usage, main_sheet):
    
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
    date_string = str(to_date_usage) #convert date to string
    print(index)
    print(to_date_usage)
    #make updates to Google Sheet

    #update referral signup
    main_sheet.update_cell(index, 1, date_string)
    main_sheet.update_cell(index, 2, today_new_signup)
    main_sheet.update_cell(index, 3, today_total_number_of_active_creators)
    main_sheet.update_cell(index, 4, today_monthly_active_app_users)
    main_sheet.update_cell(index, 5, num_active_apps)
    main_sheet.update_cell(index, 7, num_of_active_app_users_by_num_of_active_apps)
    main_sheet.update_cell(index, 8, arr)
    main_sheet.update_cell(index, 9, deal_value)
    main_sheet.update_cell(index, 10, num_deals)
    


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

def pull_usage_owner(from_date, to_date):

    """
    Pull app usage data 
    
    
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
                    "e.properties.AppId"
                ],
                accumulator=Reducer.count()
            )

    #store app owner, app, and app user IDs
    app_owner_id_list = []
    app_id_list = []
    usage_list = []
    for row in query.send():
        if row['key'][0] is not None:
            app_owner_id_list.append(int(row['key'][0]))
            app_id_list.append(row['key'][1])
            usage_list.append(row['value'])

    #generate dataframe
    data = {'app_owner_id': app_owner_id_list, 'app_id': app_id_list, 'usage': usage_list}
    df_usage_owner = pd.DataFrame(data)
    
    #only keep app owners and users with proper IDs
    df_usage_owner = df_usage_owner[(df_usage_owner.app_owner_id>1)]
    df_usage_owner = df_usage_owner[~df_usage_owner.app_owner_id.isin([10305, 71626])] #remove demo and sample accounts
    
    return df_usage_owner

def pull_usage_app(from_date, to_date):

    """
    Pull app usage data 
    
    
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
        Dataframe contains app ID, user IDs, and usage event count in Mixpanel.
           
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
                    "e.properties.AppId",
                    "e.properties.UserId"
                ],
                accumulator=Reducer.count()
            )

    #store app owner, app, and app user IDs
    app_id_list = []
    app_user_id_list = []
    usage_list = []
    for row in query.send():
        if row['key'][0] is not None:
            app_id_list.append(row['key'][0])
            app_user_id_list.append(int(row['key'][1]))
            usage_list.append(row['value'])

    #generate dataframe
    data = {'app_id': app_id_list, 'app_user_id': app_user_id_list, 'usage': usage_list}
    df_usage_app = pd.DataFrame(data)
    
    return df_usage_app

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

def get_arr_metric(to_date_signup):
    
    """
    Pull current day arr in Baremetrics.
    
    
    Parameters
    ----------
    
    
    to_date_signup: date
        End date of query. 
        
 
 
    Global Variables
    ----------
    
    baremetrics_key: str
        API key to access Baremetrics.
    
    Returns
    ----------
    
    int
        Current ARR.
    
    """
    
    #generate call to Baremetrics
    url = "https://api.baremetrics.com/v1/metrics/arr"
    querystring = {"start_date":to_date_signup,"end_date":to_date_signup}
    headers = {'Authorization': 'Bearer ' + baremetrics_key}

    #execute call and extract arr value
    response = requests.request("GET", url, headers=headers, params=querystring)
    arr = round(response.json()['metrics'][-1]['value']/100)
    
    return arr



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
    df_usage = pull_usage(from_date_usage, to_date_usage) #pull usage by owner and user IDs
    df_usage_app = pull_usage_app(from_date_usage, to_date_usage) #pull usage by app and user IDs
    df_usage_owner = pull_usage_owner(from_date_usage, to_date_usage) #pull usage by owner and app IDs

    
    ###Calculate number of active apps and # of active app users / # of active apps###
    #get owner and app mapping
    df_usage_owner = df_usage_owner.drop_duplicates(['app_owner_id','app_id'])
    df_usage_app = pd.merge(df_usage_app, df_usage_owner[['app_owner_id','app_id']], on='app_id', how='left')

    #remove apps that we can't associate an owner to and convert app owner ID column to int type
    df_usage_app = df_usage_app[~df_usage_app.app_owner_id.isnull()]
    df_usage_app['app_owner_id'] = df_usage_app['app_owner_id'].astype(int)
    
    #only keep app owner or user IDs above 1
    df_usage_app = df_usage_app[df_usage_app.app_user_id>1]
    df_usage_app = df_usage_app[df_usage_app.app_owner_id>1]
    
    #number of syncs per user
    df_usage_app_per_user = df_usage_app.groupby('app_user_id').usage.sum().reset_index()
    
    
    #get active apps: at least 40 usage events, or > 1 user who is not the owner
    
    
    ###get apps that met the first condition
    df_usage_app_sum = df_usage_app.groupby('app_id').usage.sum().reset_index()
    
    #only keep accounts with more than 40 usage events in the past 30 days
    df_usage_app_40 = df_usage_app_sum[df_usage_app_sum.usage>=40]
    
    
    ###get apps that met the second condition
    #count number of users 
    df_usage_app_other_user = df_usage_app[df_usage_app.app_owner_id!=df_usage_app.app_user_id]
    
    #count the number of other users
    df_usage_app_other_user = df_usage_app_other_user.groupby('app_id').app_user_id.count().reset_index()
    
    #only keep app owners with at least one other user using their apps
    df_usage_app_other_user = df_usage_app_other_user[df_usage_app_other_user.app_user_id>0]
    
    #merge list of apps that met at least one of the conditions
    frames = [df_usage_app_40[['app_id']], df_usage_app_other_user[['app_id']]]
    df_active_apps = pd.concat(frames)

    #remove duplicate apps
    df_active_apps = df_active_apps.drop_duplicates('app_id')
    

    ##number of active apps##
    num_active_apps = len(df_active_apps)
    
    
    ## # of active app users / # of active apps ##
    num_of_active_app_users_by_num_of_active_apps = df_usage_app_other_user.app_user_id.mean()
    
    
    
    ###Calculate total_number_of_active_creators and monthly_active_app_users
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

    ##calculate the number of active creators##
    today_total_number_of_active_creators = len(list(set(df_usage_40.app_owner_id.tolist()+df_usage_other_user.app_owner_id.tolist())))


    ##calculate total number of active users##
    today_monthly_active_app_users = len(df_usage.drop_duplicates('app_user_id'))
                       
    return today_total_number_of_active_creators, today_monthly_active_app_users, num_active_apps, num_of_active_app_users_by_num_of_active_apps


def get_hubspot_deals(to_date_signup):

    """
    Pull deals closed for the current date.


    Parameters
    ----------


    Global Variables
    ----------

    to_date_signup: date
        Date to pull data.


    Returns
    ----------

    num_deals: int
        Number of deals closed.

    deal_value: float
        Booking value.

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

        #create request
        parameters = urllib.parse.urlencode(parameter_dict)
        get_url = get_all_deals_url + parameters

        try: #maintain loop if hit api limit

            #execute API request
            r = requests.get(url= get_url, headers = headers)
            response_dict = json.loads(r.text)
            has_more = response_dict['hasMore']

            #loop through list of return deals
            deals = response_dict['deals']
            for deal in deals:
                deal_id_list.append(deal['dealId'])

            #store parameter for pagination
            parameter_dict['offset']= response_dict['offset']

        except Exception as e:
            print(e)
            time.sleep

    #initiate deal ID, deal value, date, and BAP/GCP indicator lists
    deal_value_list = []

    #loop through response to extract deal information
    i=0
    print(len(deal_id_list))
    while i < len(deal_id_list):
        print(i)
        #extract deal ID
        deal_id = deal_id_list[i]

        try:

            #request to deal service
            url = "https://api.hubapi.com/deals/v1/deal/" + str(deal_id) + "?hapikey=" + hubspot_key
            r= requests.get(url = url)
            deal = r.json()

            #deal keys
            deal_keys = deal['properties'].keys()

            #initial deal value and BAP/GCP indicator
            deal_value = 0

            #only interested in the deals that are closed
            if deal['properties']['dealstage']['value']=='closedwon':

                #extract deal value (Has two fields. amount_in_home_currency is more reliable if available) and BAP/GCP indicator
                if 'hs_closed_amount_in_home_currency' in deal_keys:
                    deal_value_temp = float(deal['properties']['hs_closed_amount_in_home_currency']['value'])
                    if deal_value_temp>0:
                        deal_value = deal_value_temp

                if ('amount_in_home_currency' in deal_keys) & (deal_value == 0):
                    value = deal['properties']['amount_in_home_currency']['value']
                    if value!='':
                        deal_value = float(value)
                        
                #extract deal closed date timestamp
                timestamp = ''.join(deal['properties']['closedate']['value'].split())[:-3]
                if timestamp != '':
                    time_value = (datetime.utcfromtimestamp(int(timestamp)) - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  
                    time_value = datetime.strptime(time_value[:10], '%Y-%m-%d')

                #check to see if the deal was closed in Q2 2020 and is in the Corporate/Enterprise Sales Pipeline (pipeline = "default")
                if (time_value == datetime.strptime(str(to_date_signup), '%Y-%m-%d')) & (deal['properties']['pipeline']['value'] == 'default'): 
                    deal_value_list.append(deal_value)
            i+=1


        except Exception as e:
            print(e)
            time.sleep(10)



    #generate deal value dataframe
    num_deals = len(deal_value_list)
    deal_value = sum(deal_value_list)




    return num_deals, deal_value



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

    #set status
    status = 1
    
    while status == 1: #add to rerun job if it fails for any reason
        
        try:
            #date range for new signups
            to_date_signup = date.today() - timedelta(days=1)
            from_date_signup = date.today() - timedelta(days=1)

            #date range for usage metrics
            to_date_usage = date.today() - timedelta(days=1)
            from_date_usage = to_date_usage - timedelta(days=30)

            print(from_date_signup)  
            print(to_date_signup)

            #pull creator profiles
            df_creators = app_creator_pull()

            #get metrics
            today_new_signup = get_signup_metric(from_date_signup, to_date_signup, df_creators)
            today_total_number_of_active_creators, today_monthly_active_app_users, num_active_apps, num_of_active_app_users_by_num_of_active_apps = get_usage_metrics(from_date_usage, to_date_usage)
            arr = get_arr_metric(to_date_signup)
            num_deals, deal_value = get_hubspot_deals(to_date_usage) #pull deal metrics for deals closed yesterday

            #update Google Sheet
            main_sheet = auth_google_services()
            update_google_sheet(today_new_signup, today_total_number_of_active_creators, today_monthly_active_app_users, num_active_apps, num_of_active_app_users_by_num_of_active_apps, arr, num_deals, deal_value, to_date_usage, main_sheet)

            status = 0

        except:
            print('Job failed. Rerunning...')


    #send email when completed  
    send_email("Adoption Metrics Update Completed")
     
    
    
    return 


 
#execute update
if __name__ == "__main__":
    main()
    
    
