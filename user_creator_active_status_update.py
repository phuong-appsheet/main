#import libraries
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
from mixpanel import Mixpanel
from datetime import date, timedelta, datetime
from tqdm import tqdm
import os
from pt_utils.utils import send_email


#import credentials object
mp_user = Mixpanel(os.environ.get('MIXPANEL_KEY_USER')) #user project
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')





def AppStart_pull(from_date, to_date):

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
    
    dataframe
        Dataframe contains user IDs and AppStart event count for app users in Mixpanel.
           
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
                    "e.properties.zUserId"
                ],
                accumulator=Reducer.count()
            )


    #initalize lists to record user ID and AppStarts
    user_id_list = []
    AppStart_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            AppStart_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list, 'AppStart': AppStart_list}
    df_AppStart = pd.DataFrame(data)
    df_AppStart = df_AppStart.dropna()
    df_AppStart.user_id = df_AppStart.user_id.astype(int)
   
    return df_AppStart



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
                    "e.properties.$username"],
                accumulator=Reducer.count()
            )


    #initialize lists to record email and user ID
    email_list = []
    creator_id_list = []
    
    #process query results
    for row in query.send():
        email_list.append(row['key'][0])
        creator_id_list.append(row['key'][1])

    #create dataframe 
    data = {'email':email_list, 'app_creator_id': creator_id_list}
    df_creators = pd.DataFrame(data=data)
    df_creators = df_creators.dropna()
    df_creators.app_creator_id = df_creators.app_creator_id.astype(int)
    
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
                    "e.properties.$username",
                    "e.properties.$distinct_id",
                    "e.properties.creator", #pulling current creator status
                    "e.properties.active_user"], #pulling current active user status
                accumulator=Reducer.count()
            )


    #initialize lists to record email, user ID, creator status, active user status, and distinct ID
    email_list = []
    user_id_list = []
    creator_list = []
    active_user_list = []
    distinct_id_list = []
    
    #process query results
    for row in query.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])
        distinct_id_list.append(row['key'][2])
        creator_list.append(row['key'][3])
        active_user_list.append(row['key'][4])

    #create dataframe 
    data = {'email':email_list, 'app_user_id': user_id_list, 'distinct_id': distinct_id_list, 'creator_current': creator_list, 'active_user_current': active_user_list}
    df_app_users = pd.DataFrame(data=data)
    df_app_users = df_app_users.dropna()
    df_app_users.app_user_id = df_app_users.app_user_id.astype(int)
    
    return df_app_users



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
        Dataframe contains user IDs and Editor event count for app creators in Mixpanel.
           
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
                    "e.properties.zUserId"],
                accumulator=Reducer.count()
            )


    #intialize lists to record user ID and Editor events
    user_id_list = []
    Editor_list = []
    
    #process query results
    for row in query.send():
        if row['key'][0] is not None:
            user_id_list.append(int(row['key'][0]))
            Editor_list.append(row['value'])

    #generate dataframe
    data = {'user_id':user_id_list,'Editor': Editor_list}
    df_Editor = pd.DataFrame(data)
    df_Editor = df_Editor.dropna()
    df_Editor.user_id = df_Editor.user_id.astype(int)
    
    return df_Editor


def pull_data(prior_30_days, date_since, yesterday):
    
    """
    Pull app creator info, app user info, Editor events, and AppStart events in Mixpanel.
    
    
    Parameters
    ----------
    
    prior_30_days: date
        Date of 30 days ago.
        
    date_since: date
        Date when AppSheet started to record data in Mixpanel.
        
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    df_app_user: dataframe
        Dataframe contains app users info in Mixpanel.
        
    df_app_creator: dataframe
            Dataframe contains app creators info in Mixpanel.
            
    df_editor: dataframe
            Dataframe contains user IDs and Editor event count for app creators in Mixpanel.
            
    df_appstart: dataframe
            Dataframe contains user IDs and AppStart event count for app users in Mixpanel.
           
    """
    
    #execute data pull
    df_app_user = app_user_pull()
    df_app_creator = creator_pull()
    df_appstart = AppStart_pull(prior_30_days, yesterday)
    df_editor = Editor_pull(date_since, yesterday)

    return df_app_user, df_app_creator, df_appstart, df_editor



def count_appstart_editor(df_appstart, df_editor):
    
    """
    Count AppStart and Editor sessions by app user and creator, respectively.
    
    
    Parameters
    ----------
    
    df_appstart: dataframe
            Dataframe contains user IDs and AppStart event count for app users in Mixpanel.
    
    df_editor: dataframe
            Dataframe contains user IDs and Editor event count for app creators in Mixpanel.
            
    
    Global Variables
    ----------
    
    
        
    Returns
    ----------
    
    df_appstart_count: dataframe
            Dataframe contains user IDs and AppStart event total count for app users in Mixpanel.
    
    df_editor_count: dataframe
            Dataframe contains user IDs and Editor event total count for app creators in Mixpanel.
           
    """
    
    #count appstart and editor sessions
    df_appstart_count = df_appstart.groupby(['user_id']).AppStart.sum().reset_index()
    df_editor_count = df_editor.groupby(['user_id']).Editor.sum().reset_index()
    
    #only keep valid emails
    df_appstart_count = df_appstart_count[df_appstart_count.user_id>1]
    df_editor_count = df_editor_count[df_editor_count.user_id>1]
    
    return df_appstart_count, df_editor_count


def update_user_creator_active_status(df_app_creator, df_app_user, df_appstart_count, df_editor_count):
    
    """
    Merge AppStart and Editor events with creator and user profiles.
    
    
    Parameters
    ----------
    
    df_app_user: dataframe
        Dataframe contains app users info in Mixpanel.
        
    df_app_creator: dataframe
            Dataframe contains app creators info in Mixpanel.
            
    df_appstart_count: dataframe
            Dataframe contains user IDs and AppStart event total count for app users in Mixpanel.
    
    df_editor_count: dataframe
            Dataframe contains user IDs and Editor event total count for app creators in Mixpanel.
           
    
    Global Variables
    ----------
    
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains user info, creator status, and active_user status for app users.
    
    """
    
    #merge appstart and editor event counts to app user and creator profiles, respectively
    df_appstart_count_user_email = pd.merge(df_app_user[['email','user_id']], df_appstart_count[['user_id','AppStart']], on='user_id', how='left')
    df_editor_count_creator_email = pd.merge(df_app_creator[['email','user_id']], df_editor_count[['user_id','Editor']], on='user_id', how='left')

    #only keeping creators defined in this case as anyone who has at least 1 editor session
    df_editor_count_creator_email = df_editor_count_creator_email[df_editor_count_creator_email.Editor>0]

    #only keep app creators and users with emails
    df_appstart_count_user_email = df_appstart_count_user_email[~df_appstart_count_user_email.email.isnull()]
    df_editor_count_creator_email = df_editor_count_creator_email[~df_editor_count_creator_email.email.isnull()]  
    
    #set creator status to True for anyone with an editor session
    df_not_creator = df_editor_count_creator_email
    df_not_creator['creator'] = True
 
    #set active_user status to True for anyone with at least 30 AppStarts in the past 30 days
    df_active_user = df_appstart_count_user_email[df_appstart_count_user_email.AppStart>30]
    df_active_user['active_user'] = True
    
    #merge creator and active_user True status app users to profiles
    df_users_creator_active_status_current = pd.merge(df_app_user, df_not_creator[['email','creator']], on='email', how='left')
    df_users_creator_active_status_current = pd.merge(df_users_creator_active_status_current, df_active_user[['email','active_user']], on='email', how='left')

    #filter out appsheet, 1track and abm accounts
    df_users_creator_active_status_current = df_users_creator_active_status_current[~df_users_creator_active_status_current.email.isnull()]
    df_users_creator_active_status_current = df_users_creator_active_status_current[~df_users_creator_active_status_current['email'].str.contains('@appsheet.com|@1track.com|@att.com|@athensservices.com|@belk.com|@bridgewater.com|@costafarms.com|@ebsco.com|@husqvarnagroup.com|@issworld.com|@juul.com|@mortenson.com|@nestle.com|@nike.com|@opusteam.com|@shell.com|@smithgroup.com|@state.co.us|@stryker.com|@unilever.com|@vailresorts.com|@vice.com|@walmart.com|@coca-cola.com|@pepsico.com|@roche.com|@aep.com|@ge.com|@clearlink.com|@amazon.com|@curtisswright.com|@enerflex.com|@eogresources.com|@getcruise.com|@honeywell.com|@jpost.jp|@johnsoncontrols.com|@novonordisk.com|@polycor.com|@riwal.com|@snclavalin.com|@solvay.com|@sossecurity.com|@tarmac.com|@unipart.com|@group.issworld.com|@be.issworld.com|@dk.issworld.com|@us.issworld.com|.issworld.com|@juullabs.com|@ch.nestle.com|@us.nestle.com|@walmartlabs.com|@coca-cola.in|@ccbagroup.com|aia.com|fb.com|whirlpool.com|lime-energy.com|costco.com|riwal.com|disney.com|starbucks.com|parker.com|getcruise.com|homedepot.com|kubota.com|davey.com|oldcastle.com|inspirebrands.com|cbre.com|shell.com|.fb.com|.disney.com|oldcastlematerials.com|crhamericas.com|.shell.com')]

    #drop duplicate emails
    df_users_creator_active_status_current = df_users_creator_active_status_current.drop_duplicates('email')
    
    #set False creator and active_user statuses respectively for anyone in which these statuses were not True
    df_users_creator_active_status_current.loc[df_users_creator_active_status_current.creator.isnull(), 'creator'] = False
    df_users_creator_active_status_current.loc[df_users_creator_active_status_current.active_user.isnull(), 'active_user'] = False
    
    
    return df_users_creator_active_status_current


def push_update(df_app_user_merged_current):

    """
    Push current creator and active_user statuses to app user profiles in Mixpanel.
    
    
    Parameters
    ----------
    
    df_app_user_merged_current: dataframe
        Dataframe contains app users info and current creator and active_user statuses.
        
    
    Global Variables
    ----------
    
    mp_user: Mixpanel object
        Mixpanel object used to make API calls to Mixpanel User Project.
        
        
    Returns
    ----------
    
    None
    
    """
    
    
    #extract lists of distinct IDs, creator statuses, and active_user statuses
    creator_list = df_app_user_merged_current.creator.tolist()
    distinct_id_list = df_app_user_merged_current.distinct_id.tolist()
    active_user_list = df_app_user_merged_current.active_user.tolist()

    #update app user profiles
    for i in tqdm(range(len(distinct_id_list))):
        
        #extract app user distinct ID, creator status, and active_user status
        distinct_id = distinct_id_list[i]
        creator = creator_list[i]
        active_user = active_user_list[i]
        
        #generate dataframe
        params = {'creator': creator, 'active_user': active_user}
        mp_user.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})

    return

def generate_current_user_active_creator(prior_30_days, date_since, yesterday):
    
    """
    Pull activity data in Mixpanel and update app user creator and active_user statuses.
    
    
    Parameters
    ----------
    
    prior_30_days: date
        Date of 30 days ago.
    
    date_since: date
        Date when AppSheet started recording data in Mixpanel.
        
    yesterday: date
        Yesterday's date.
        
    
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    df_users_creator_active_status_current: dateframe
        Dataframe contains app users info and current creator and active_user statuses.
   
    df_app_user: dataframe
        Dataframe contains app users info in Mixpanel.
    
    """
    
    #pull activity data and count AppStart and Editor events
    df_app_user, df_app_creator, df_appstart, df_editor = pull_data(prior_30_days, date_since, yesterday)
    df_appstart_count, df_editor_count = count_appstart_editor(df_appstart, df_editor)
    
    
    #change creator_id and app_user_id to user_id to map across projects
    df_app_user = df_app_user.rename(columns={'app_user_id':'user_id'})
    df_app_creator = df_app_creator.rename(columns={'app_creator_id': 'user_id'})

    
    #identify active users who are current not app creators 
    df_users_creator_active_status_current = update_user_creator_active_status(df_app_creator, df_app_user[['email', 'user_id', 'distinct_id']], df_appstart_count, df_editor_count)
    
    return df_users_creator_active_status_current, df_app_user

def main():
    
    """
    
    Update app users' creator and active_user statuses in Mixpanel.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    
    #send initial email
    send_email('Initiate App User Creator and Active Statuses Update')
    
    
    #initialize date range
    yesterday = date.today() - timedelta(days=1)
    prior_30_days = yesterday - timedelta(days=30)
    date_since = date(year = 2014, month = 8, day = 8)

    #generate current app user creator and active_user statuses
    df_users_creator_active_status_current, df_app_user = generate_current_user_active_creator(prior_30_days, date_since, yesterday)

    #merge current statuses with app user profiles
    df_app_user_merged_current = pd.merge(df_app_user[['email','creator_current', 'active_user_current']], df_users_creator_active_status_current, on = 'email', how='left')
    df_app_user_merged_current = df_app_user_merged_current[~df_app_user_merged_current.email.isnull()] #remove null emails
    df_app_user_merged_current = df_app_user_merged_current[~df_app_user_merged_current.user_id.isnull()] #remove null user_id

    #only need to update app users whose current statuses differ from records in Mixpanel
    df_app_user_merged_current = df_app_user_merged_current[(df_app_user_merged_current.creator_current!=df_app_user_merged_current.creator)
                                                           |(df_app_user_merged_current.active_user_current!=df_app_user_merged_current.active_user)]

    #avoid duplicate accounts and only focus on ones with activities
    df_app_user_merged_current = df_app_user_merged_current.sort_values('creator_current', ascending=False).drop_duplicates('user_id', keep='first')
        
    #push updates to Mixpanel
    push_update(df_app_user_merged_current[['distinct_id','creator', 'active_user']])
    
    #send completed email
    send_email('App User Creator and Active Statuses Update Completed')

    return 

#execute update
if __name__ == '__main__':
    main()
    
    
    
    


