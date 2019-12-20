#import libraries
from mixpanel_jql import JQL, Reducer, Events
from datetime import date, timedelta
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
from mixpanel import Mixpanel
from tqdm import tqdm
import os
from pt_utils.utils import send_email

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')


def user_pull():

    """
    
    Pull app creators Mixpanel information.
    
    
    Parameters
    ----------
    
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator information from Mixpanel.
    
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
                    "e.properties.hs_mrr"
                ],
                accumulator=Reducer.count()
            )


    #initialize list to track emails, user IDs, distinct IDs, and mrr
    email_list = []
    user_id_list = []
    distinct_id_list = []
    hs_mrr_list = []
    
    #process query results
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])
        distinct_id_list.append(row['key'][2])
        hs_mrr_list.append(row['key'][3])

    #create dataframe 
    data = {'email':email_list, 'user_id': user_id_list, 'distinct_id': distinct_id_list, 'hs_mrr': hs_mrr_list}
    df_users = pd.DataFrame(data=data)
    
    return df_users


def app_user_pull():

    """
    
    Pull app users' information in Mixpanel.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    api_user_secret: str
        Client secret used to make calls to Mixpanel User Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app users' information in Mixpanel.
           
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


    #initialize lists to record email and user ID
    email_list = []
    user_id_list = []
    
    #process query results
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])

    #create dataframe 
    data = {'user_email':email_list, 'app_user_id': user_id_list}
    df_app_users = pd.DataFrame(data=data)
    
    return df_app_users

def user_appstart_pull(from_date, to_date, df_app_users):
    
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
        Dataframe contains app creator user ID, list of app user email domains, and number of app users
        
    df_top_appstarts: dataframe
        Dataframe contains app creator user ID and number of AppStarts from top apps
    
           
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
                    "e.properties.zAppName"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record app user IDs, app creator IDs, app name, and number of AppStarts
    app_user_id_list = []
    owner_id_list = []
    app_name_list = []
    AppStart_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            app_user_id_list.append(row['key'][0])
            owner_id_list.append(row['key'][1])
            app_name_list.append(row['key'][2])
            AppStart_list.append(row['value'])

    #generate email
    data = {'app_user_id':app_user_id_list,'user_id': owner_id_list, 'app_name': app_name_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)

    #merge AppStart and app user dataframes to associate app user emails to number of AppStarts
    df_AppStart = pd.merge(df_AppStart, df_app_users, on='app_user_id', how='left')

    #get total users and list of user emails for each app creator
    df_user_AppStart = df_AppStart.groupby(['user_id','app_user_id'])['app_name'].count().reset_index()
    df_user_AppStart = df_user_AppStart.groupby('user_id')['app_user_id'].count().reset_index()
    df_user_AppStart = df_user_AppStart.rename(columns={'app_user_id':'num_app_users'})
    
    #get app user domain list for each app creator
    df_AppStart_temp = df_AppStart.groupby(['user_id','app_user_id','user_email'])['app_name'].count().reset_index()
    df_AppStart_temp['user_email_domains'] = df_AppStart_temp['user_email'].str.split('@').str[1].fillna('')
    df_AppStart_temp = df_AppStart_temp[df_AppStart_temp['user_email_domains']!='']
    df_user_domains_by_creators = df_AppStart_temp.groupby('user_id')['user_email_domains'].apply(list).reset_index()
    df_user_AppStart = pd.merge(df_user_AppStart, df_user_domains_by_creators, on='user_id', how='left')
    df_user_AppStart.loc[df_user_AppStart['user_email_domains'].isnull(),['user_email_domains']] = df_user_AppStart.loc[df_user_AppStart['user_email_domains'].isnull(),'user_email_domains'].apply(lambda x: [])

    #only keep the app with the highest number of AppStarts for each app creator
    df_top_appstarts = df_AppStart.groupby(['user_id','app_name']).AppStart.sum().reset_index().sort_values('AppStart', ascending=False).drop_duplicates('user_id', keep='first')
    df_top_appstarts = df_top_appstarts.rename(columns = {'AppStart': 'appstart_by_top_app'})

    
    return df_user_AppStart, df_top_appstarts



def EditorAction_pull(from_date, to_date):
    
    """
    Pull app creator EditorAction-Save events in Mixpanel.
    
    
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
        Dataframe contains app creator emails, number of EditorAction events, and zEvent = Save filter.
           
    """
    
    #generate JQL query
    query = JQL(
            api_creator_secret,
            events=Events({
                    'event_selectors': [{'event': "EditorAction"}],
                    'from_date': from_date,
                    'to_date': to_date
                })).group_by(
                    keys=[
                        "e.properties.zUserEmail",
                        "e.properties.zEvent"
                    ],
                    accumulator=Reducer.count()
                )


    #initialize lists to record emails, EditorAction events, and zEvent
    user_email_list = []
    EditorAction_list = []
    zevent_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            user_email_list.append(row['key'][0])
            zevent_list.append(row['key'][1])
            EditorAction_list.append(row['value'])

    #create dataframe
    data = {'email':user_email_list,'zevent':zevent_list, 'EditorAction': EditorAction_list}
    df_editor_action = pd.DataFrame(data)
    
    #filter to only include "Save" editor events
    df_editor_action = df_editor_action[df_editor_action.zevent=='Save']

    return df_editor_action


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
        Dataframe contains user IDs and new sign up event count for app creators in Mixpanel.
           
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
                    "e.properties.userId", #use userId
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record user IDs and New Signup Web
    user_id_list = []
    new_sign_up_list = []
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            new_sign_up_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list,'new_sign_up': new_sign_up_list}
    df_New_Signup_Web = pd.DataFrame(data)
    
    return df_New_Signup_Web

def main():
    
    """
    
    Update app creator journey stage in Mixpanel.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns 
    ----------
    
    dataframe
        Dataframe contains app creator info and current journey stage.
        
    
    """
    
    #send initial email
    send_email('Initiate App Creator Journey Stage Update')

    #initialize date range
    today = date.today() 
    date_since = date(year = 2014, month = 8, day = 8)
    date_since_user_project = date(year = 2017, month = 3, day = 1)
    yesterday = today - timedelta(days=1)

    #pull all app creator and user profiles from Mixpanel
    df_users = user_pull()
    df_app_users = app_user_pull()
    
    #pull Editor, AppStart and New Signup Web events from Mixpanel
    df_editor_action = EditorAction_pull(date_since, today) 
    df_user_AppStart, df_top_appstarts = user_appstart_pull(date_since_user_project, today, df_app_users)
    df_New_Signup_Web = New_Signup_Web_pull(yesterday, today)

    #pull in all time sign up web. It is stored to prevent having to pull the same data every day
    df_New_Signup_All_Time = pd.read_csv('/home/phuong/automated_jobs/user_journey/new_sign_up_since_beginning_final.csv')

    #merge new and old sign up to create new all-time
    frames = [df_New_Signup_All_Time, df_New_Signup_Web]
    df_New_Signup_Web_final = pd.concat(frames)

    #drop duplicate app creator signups
    df_New_Signup_Web_final = df_New_Signup_Web_final.drop_duplicates('user_id')

    #update new sign up all time csv
    df_New_Signup_Web_final.to_csv('/home/phuong/automated_jobs/user_journey/new_sign_up_since_beginning_final.csv', index=False)

    #merge app creator profiles, app user domains, top app AppStarts, and EditorAction-Save dataframes
    df_final = pd.merge(df_users, df_New_Signup_Web_final[['user_id','new_sign_up']], on='user_id', how='left')
    df_final = pd.merge(df_final, df_user_AppStart[['user_id','num_app_users','user_email_domains']], on='user_id', how='left') 
    df_final = pd.merge(df_final, df_top_appstarts[['user_id', 'appstart_by_top_app']], on='user_id', how='left') #merge existing 
    df_final = pd.merge(df_final, df_editor_action[['email', 'EditorAction']], on='email', how='left')

    #remove app creators with missing emails
    df_final = df_final[~df_final.email.isnull()]

    #assign empty list to app creators without users 
    df_final = df_final.fillna(0)
    df_final.loc[df_final['user_email_domains']==0,['user_email_domains']] = df_final.loc[df_final['user_email_domains']==0,'user_email_domains'].apply(lambda x: [])
 
    #convert string mrr to int
    df_final.loc[df_final.hs_mrr == '', 'hs_mrr'] = 0
    df_final['hs_mrr'] = df_final.hs_mrr.astype(float)
    
    #categorize app creators journey stage based on given conditions
    df_final['user_journey_stage']  = ''
    df_final['user_journey_stage'] = np.where((df_final['num_app_users']>=2) | (df_final['appstart_by_top_app']>=40), 'Commit', df_final['user_journey_stage'] ) #40 is 70th percentile of users with appstart
    df_final['user_journey_stage'] = np.where((df_final['num_app_users']<2)&(df_final['appstart_by_top_app']<40)&(df_final['EditorAction']>0), 'Explore', df_final['user_journey_stage'])
    df_final['user_journey_stage'] = np.where(df_final['hs_mrr']>0, 'Convert', df_final['user_journey_stage'] )
    df_final['user_journey_stage'] = np.where(df_final['user_journey_stage']=='', 'Consider', df_final['user_journey_stage'])
    df_final['sign_up_channel'] = np.where((df_final['new_sign_up']>0), 'Web', 'App')

    #extract app creator distinct ID, number of app users, app user domains, sign up channel, journey stage, and top app AppStarts to lists
    distinct_id_list = df_final.distinct_id.tolist()
    num_app_users_list = df_final.num_app_users.tolist()
    email_domains_list = df_final.user_email_domains.tolist()
    user_journey_stage_list = df_final.user_journey_stage.tolist()
    user_sign_up_channel_list = df_final.sign_up_channel.tolist()
    appstart_by_top_app_list = df_final.appstart_by_top_app.tolist()
    

    #update Mixpanel profiles
    for i in tqdm(range(len(distinct_id_list))):
        
        #extract app creator distinct ID, number of app users, journey stage, sign up channel, top app AppStarts, and user email domains
        distinct_id = distinct_id_list[i]
        num_app_users = num_app_users_list[i]
        user_journey_stage = user_journey_stage_list[i] 
        user_sign_up_channel = user_sign_up_channel_list[i]
        appstarts_by_top_app = appstart_by_top_app_list[i]
        user_domains = email_domains_list[i]

        #generate parameters and update Mixpanel profile
        try:
            params = {'user_domains': user_domains, 'num_app_users': num_app_users,'num_appstarts_by_top_app': appstarts_by_top_app, 'user_journey_stage': user_journey_stage, 'sign_up_channel': user_sign_up_channel}
            mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        except Exception as e: 
            print(e)
            print(distinct_id)  
            
            
    #send completed email
    send_email('App Creator Journey Stage Update Completed')
            
    return df_final


#execute update
if __name__ == '__main__':
    main()

