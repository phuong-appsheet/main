#import libraries
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
from mixpanel import Mixpanel
import os
from pt_utils.utils import send_email


#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY')) #creator project
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')



def Editor_pull(yesterday):

    """
    Pull app creator Editor events in Mixpanel.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
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
                'from_date': yesterday,
                'to_date': yesterday
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record user ID, Editor datetime, and Editor events
    user_id_list = []
    datetime_list = []
    Editor_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            Editor_list.append(row['value'])

    #generate dataframe
    data = {'creator_user_id':user_id_list,'Editor_datetime': datetime_list, 'Editor': Editor_list}
    df_Editor = pd.DataFrame(data)
    
    return df_Editor

def New_Signup_App_pull(yesterday):

    """
    
    Pull app user sign up events in Mixpanel.
    
    
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
        Dataframe contains user IDs, sign up datetime, and new sign up event count for app users in Mixpanel.
           
    """
    
    
    #generate JQL query
    query = JQL(
        api_user_secret,
        events=Events({
                'event_selectors': [{'event': "New Signup App"}],
                'from_date': yesterday,
                'to_date': yesterday
            })).group_by(
                keys=[
                    "e.properties.zUserId",
                    "new Date(e.time).toISOString()"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to store emails, user IDs, and sign up datetime
    user_id_list = []
    datetime_list = []
    new_sign_up_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            datetime_list.append(datetime.strptime(row['key'][1][:10], '%Y-%m-%d'))
            new_sign_up_list.append(row['value'])

    #generate dataframe
    data = {'app_user_id':user_id_list,'sign_up_datetime': datetime_list,'new_signup_app': new_sign_up_list}
    df_New_Signup_App = pd.DataFrame(data)
    
    return df_New_Signup_App


#grabbing new sign up web events
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
        Dataframe contains app creators info in Mixpanel.
           
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
                    "e.properties.user_journey_stage"],
                accumulator=Reducer.count()
            )


    #initialize lists to store user IDs, email, and journey stage
    email_list = []
    user_id_list = []
    user_journey_stage_list = []
    
    #process query results
    for row in query.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])
        user_journey_stage_list.append(row['key'][2])

    #create dataframe 
    data = {'email':email_list, 'creator_user_id': user_id_list, 'user_journey_stage': user_journey_stage_list}
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
    
    
    #generate query
    query = JQL(
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


    #initialize list to record user IDs and email
    email_list = []
    user_id_list = []
    for row in query.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])

    #create dataframe 
    data = {'email':email_list, 'app_user_id': user_id_list}
    df_app_users = pd.DataFrame(data=data)
    return df_app_users


def pull_data(yesterday):
    
    """
    Pull app creator info, app user info, Editor events, and New Signup App events in Mixpanel.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    df_app_users: dataframe
        Dataframe contains app users info in Mixpanel.
        
    df_creator: dataframe
            Dataframe contains app creators info in Mixpanel.
            
    df_Editor: dataframe
            Dataframe contains user IDs, Editor datetime, and Editor event count for app creators in Mixpanel.
            
    df_New_Signup_App: dataframe
            Dataframe contains user IDs, New Signup App datetime, and New Signup App event count for app users in Mixpanel.
           
    """
    
    #pull new app sign up, editor sessions, and app user and creator profiles
    df_New_Signup_App = New_Signup_App_pull(yesterday)
    df_Editor = Editor_pull(yesterday)
    df_creator = creator_pull()
    df_app_users = app_user_pull()
    
    #merge current and new new signup app data. Storing previously pulled data to avoid having to pull old data each time
    df_New_Signup_App_current = pd.read_csv('/home/phuong/automated_jobs/user_to_creator/new_signup_app_current.csv')
    df_New_Signup_App_current.sign_up_datetime = df_New_Signup_App_current.sign_up_datetime.astype('datetime64[ns]')
    frames = [df_New_Signup_App_current, df_New_Signup_App]
    df_New_Signup_App = pd.concat(frames)
    
    #remove invalid user IDs and keeping the first app sign up event
    df_New_Signup_App = df_New_Signup_App[df_New_Signup_App.app_user_id>1]
    df_New_Signup_App = df_New_Signup_App.sort_values('sign_up_datetime', ascending=True)
    df_New_Signup_App = df_New_Signup_App.drop_duplicates('app_user_id', keep='first')
    df_New_Signup_App.to_csv('/home/phuong/automated_jobs/user_to_creator/new_signup_app_current.csv', index=False)
    
    #merge current and new editor data. Storing previously pulled data to avoid having to pull old data each time
    df_Editor_current = pd.read_csv('/home/phuong/automated_jobs/user_to_creator/editor_current.csv')
    df_Editor_current.Editor_datetime = df_Editor_current.Editor_datetime.astype('datetime64[ns]')
    frames = [df_Editor_current,df_Editor]
    df_Editor = pd.concat(frames)
    
    #only keep the first editor action event
    df_Editor = df_Editor.sort_values('Editor_datetime', ascending=True)
    df_Editor = df_Editor.drop_duplicates('creator_user_id', keep='first')
    df_Editor.to_csv('/home/phuong/automated_jobs/user_to_creator/editor_current.csv', index=False)
    
    
    return df_New_Signup_App, df_Editor, df_creator, df_app_users
    
    
def identify_user_to_creator(df_New_Signup_App, df_Editor, df_creator, df_app_users):
    
    """
    Idenfity app creators who originally signed up as app users.
   
    
    Parameters
    ----------
    
    df_app_users: dataframe
        Dataframe contains app users info in Mixpanel.
        
    df_creator: dataframe
            Dataframe contains app creators info in Mixpanel.
            
    df_Editor: dataframe
            Dataframe contains user IDs, Editor datetime, and Editor event count for app creators in Mixpanel.
            
    df_New_Signup_App: dataframe
            Dataframe contains user IDs, New Signup App datetime, and New Signup App event count for app users in Mixpanel.
        
        
        
    Global Variables
    ----------
    
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators who originally signed up as app users.
        
    """
    
    #merge sign up event with app user profile and remove those with invalid emails
    df_New_Signup_App_final = pd.merge(df_New_Signup_App[['app_user_id','new_signup_app', 'sign_up_datetime']],df_app_users, on='app_user_id', how='left')
    df_New_Signup_App_final = df_New_Signup_App_final[~df_New_Signup_App_final.email.isnull()]
    df_New_Signup_App_final = df_New_Signup_App_final[df_New_Signup_App_final.email!='guest']

    #merge sign up event and app user profile with creator profile information. Remove invalid user IDs and duplicates.
    df_New_Signup_App_final = pd.merge(df_New_Signup_App_final[['app_user_id','sign_up_datetime','email']], df_creator[['email', 'creator_user_id']], on='email', how='left')
    df_New_Signup_App_final = df_New_Signup_App_final[df_New_Signup_App_final.creator_user_id>1]
    df_New_Signup_App_final = df_New_Signup_App_final.drop_duplicates('email')
    
    #merge with Editor event dataframe to identify those who transitioned to app creator status
    df_user_to_creator = pd.merge(df_New_Signup_App_final, df_Editor[['creator_user_id','Editor_datetime']], on='creator_user_id', how='left')
    df_user_to_creator = df_user_to_creator[~df_user_to_creator.Editor_datetime.isnull()]
    df_user_to_creator = df_user_to_creator[df_user_to_creator['Editor_datetime']>=df_user_to_creator['sign_up_datetime']]
    df_user_to_creator = df_user_to_creator.drop_duplicates('creator_user_id')

    #convert user ID str to int
    df_user_to_creator.creator_user_id = df_user_to_creator.creator_user_id.astype(int) #convert creator id to int

    return df_user_to_creator


def push_user_to_creator(df_user_to_creator):

    """
    
    Update user_to_creator field in app creator profiles in Mixpanel.
    
    
    Parameters
    ----------
    
    df_user_to_creator: dataframe
        Dataframe contains app creators who originally signed up as app users.
        
    
    Global Variables
    ----------
    
    mp: Mixpanel object
        Mixpanel object used to make API calls to Mixpanel Creator Project.
        
        
    Returns
    ----------
    
    None
    
    """
    
    #extract lists of user ID and sign up time
    creator_id_list = df_user_to_creator.creator_user_id.tolist()
    signup_time_list = df_user_to_creator.sign_up_datetime.tolist()

    #update app creator profiles
    for i in range(len(creator_id_list)):
        
        #extract app creator's user ID and signup time and construct parameters
        user_id = creator_id_list[i]
        signup_time = str(signup_time_list[i]+timedelta(hours=8)).replace(' ', 'T')
        params = {'user_to_creator': True, 'Signup': signup_time}
        
        #execute API call
        mp.people_set(user_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        
    return


def main():
    
    """
    
    Identify app creators who originally signed up as app users.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    #send initial email
    send_email('Initiate User to Creator Tag Update')
    
    #initialize date range
    today = date.today()
    yesterday = today - timedelta(days=1)

    #pull new app sign up, editor sessions, and app creator and user profiles
    df_New_Signup_App, df_Editor, df_creator, df_app_users = pull_data(yesterday)

    #identify app creators who originally signed up as app users
    df_user_to_creator = identify_user_to_creator(df_New_Signup_App, df_Editor, df_creator, df_app_users)

    #update app creator profile in Mixpanel and store data in csv
    push_user_to_creator(df_user_to_creator)
    df_user_to_creator.to_csv('/home/phuong/automated_jobs/user_to_creator/user_to_creator.csv', index=False)
    
    #send completed email
    send_email('User to Creator Tag Update Completed')
     
    return 




#execute updates
if __name__ == '__main__':
    main()
