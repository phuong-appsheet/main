#import model
import pandas as pd
import numpy as np
import pickle
from sklearn.tree import DecisionTreeClassifier
import requests
import json
from mixpanel_jql import JQL, Reducer, Events, People
from mixpanel import Mixpanel
import time
from datetime import datetime, date, timedelta
import os
from intercom.client import Client
from pt_utils.utils import send_email



#import credentials object
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')

#intercom key
#documentation: https://buildmedia.readthedocs.org/media/pdf/python-intercom/0.2.13/python-intercom.pdf
#https://python-intercom.readthedocs.io/en/latest/
intercom_key = os.getenv('INTERCOM_KEY')
intercom = Client(personal_access_token=intercom_key)


def mobile_preview_pull(from_date, to_date):
    
    """
    
    Pull app creators MobilePreview events.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and number of MobilePreview events.
    
    """
    
    
    #generate JQL query
    query_mobile_preview = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "MobilePreview"}],
                    'from_date': from_date,
                    'to_date': to_date
                })
            ).group_by(
                keys=[
                    "e.properties.zUserId",
                ],
                accumulator=Reducer.count()
            )


    #initialize user ID and MobilePreview lists
    userid_list = []
    MobilePreview_list = []
    
    #process query results
    for row in query_mobile_preview.send():
        if row['key'][0] is not None:
            userid_list.append(int(row['key'][0]))
            MobilePreview_list.append(row['value'])

    #create dataframe 
    data = {'user_id': userid_list,'MobilePreview': MobilePreview_list}
    df_mobile_preview = pd.DataFrame(data=data)
    
    return df_mobile_preview


def editor_pull(from_date, to_date):

    """
    
    Pull app creators Editor events.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and number of Editor events.
    
    """
    
    #generate JQL query
    query_editor = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "Editor"}],
                    'from_date': from_date,
                    'to_date': to_date
                })
            ).group_by(
                keys=[
                    "e.properties.zUserId",
                ],
                accumulator=Reducer.count()
            )


    #initialize user ID and Editor lists
    userid_list = []
    editor_list = []
    
    #process query results
    for row in query_editor.send():
        if row['key'][0] is not None:
            userid_list.append(int(row['key'][0]))
            editor_list.append(row['value'])

    #create dataframe 
    data = {'user_id': userid_list,'editor': editor_list}
    df_editor = pd.DataFrame(data=data)
    
    return df_editor



def SubscribeEmail_pull(from_date, to_date):

    """
    
    Pull app creators #SubscribeEmail events.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and number of #SubscribeEmail events.
    
    """
    
    #generat JQL query
    query_SubscribeEmail = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "#SubscribeEmail"}],
                    'from_date': from_date,
                    'to_date': to_date
                })
            ).group_by(
                keys=[
                    "e.properties.zUserId",
                ],
                accumulator=Reducer.count()
            )


    #initialize user ID and #SubscribeEmail lists
    userid_list = []
    SubscribeEmail_list = []
    
    #process query results
    for row in query_SubscribeEmail.send():
        if row['key'][0] is not None:
            userid_list.append(int(row['key'][0]))
            SubscribeEmail_list.append(row['value'])

    #create dataframe 
    data = {'user_id': userid_list,'subscribe_email': SubscribeEmail_list}
    df_SubscribeEmail = pd.DataFrame(data=data)
    
    return df_SubscribeEmail


def Home_Page_pull(from_date, to_date):

    """
    
    Pull app creators Home Page events.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and number of Home Page events.
    
    """
    
    #generate JQL query
    query_Home_Page = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "Home Page"}],
                    'from_date': from_date,
                    'to_date': to_date
                })
            ).group_by(
                keys=[
                    "e.properties.zUserId",
                ],
                accumulator=Reducer.count()
            )


    #initialize user ID and Home Page lists
    userid_list = []
    Home_Page_list = []
    
    #process query results
    for row in query_Home_Page.send():
        if row['key'][0] is not None:
            userid_list.append(int(row['key'][0]))
            Home_Page_list.append(row['value'])

    #create dataframe 
    data = {'user_id': userid_list,'home_page': Home_Page_list}
    df_Home_Page = pd.DataFrame(data=data)
    
    return df_Home_Page


def Sql_Auth_Info_pull(from_date, to_date):

    """
    
    Pull app creators Sql Auth Info events.
    
    
    Parameters
    ----------
    
    from_date: date
        Start date of query.
        
    to_date: date
        End date of query.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and number of Sql Auth Info events.
    
    """
    
    #generate JQL query
    query_Sql_Auth_Info = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "Sql Auth Info"}],
                    'from_date': from_date,
                    'to_date': to_date
                })
            ).group_by(
                keys=[
                    "e.properties.zUserId",
                ],
                accumulator=Reducer.count()
            )


    #initialize user ID and Sql Auth Info lists
    userid_list = []
    Sql_Auth_Info_list = []
    
    #process query results
    for row in query_Sql_Auth_Info.send():
        if row['key'][0] is not None:
            userid_list.append(int(row['key'][0]))
            Sql_Auth_Info_list.append(row['value'])

    #create dataframe 
    data = {'user_id': userid_list,'sql_auth_info': Sql_Auth_Info_list}
    df_Sql_Auth_Info = pd.DataFrame(data=data)
    
    return df_Sql_Auth_Info


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
                    "e.properties.$username",
                    "e.properties.$email",
                    "e.properties.hs_analytics_num_page_views",
                    "e.properties.hs_create_date",
                    "e.properties.hs_zautomationsource",
                    "e.properties.hs_analytics_source",
                    "e.properties.company_foundedYear",
                    "e.properties.company_metrics_employees",
                    "e.properties.hs_lifecycle_stage",
                    "e.properties.hs_mrr",
                    "e.properties.company_category_industry",
                    "e.properties.AuthSource",
                    "e.properties.$distinct_id",
                    "e.properties.$last_seen"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record app creator information
    email_list = []
    userid_list = []
    hs_analytics_num_page_views_list = []
    hs_create_date_list = []
    hs_zautomationsource_list = []
    hs_analytics_source_list = []
    company_foundedYear_list = []
    company_metrics_employees_list = []
    hs_lifecycle_stage_list = []
    hs_mrr_list = []
    company_category_industry_list = []
    auth_source_list = []
    distinct_id_list = []
    last_seen_list = []
    
    #process query results
    for row in query_user.send():
        userid_list.append(row['key'][0])
        email_list.append(row['key'][1])
        hs_analytics_num_page_views_list.append(row['key'][2])
        hs_create_date_list.append(row['key'][3]) 
        hs_zautomationsource_list.append(row['key'][4])
        hs_analytics_source_list.append(row['key'][5])
        company_foundedYear_list.append(row['key'][6])
        company_metrics_employees_list.append(row['key'][7])
        hs_lifecycle_stage_list.append(row['key'][8])
        hs_mrr_list.append(row['key'][9])
        company_category_industry_list.append(row['key'][10])
        auth_source_list.append(row['key'][11])
        distinct_id_list.append(row['key'][12])
        last_seen_list.append(row['key'][13])

    #create dataframe 
    data = {'user_id': userid_list,'email':email_list, 'hs_analytics_num_page_views': hs_analytics_num_page_views_list,
           'hs_create_date': hs_create_date_list,'hs_zautomationsource': hs_zautomationsource_list, 'hs_analytics_source': hs_analytics_source_list,
           'company_foundedYear': company_foundedYear_list, 'company_metrics_employees': company_metrics_employees_list,
           'hs_lifecycle_stage': hs_lifecycle_stage_list, 'hs_mrr': hs_mrr_list,'company_category_industry': company_category_industry_list,
           'auth_source': auth_source_list, 'mixpanel_distinct_id': distinct_id_list, 'last_seen': last_seen_list}
    df_users = pd.DataFrame(data=data)
    
    
    #remove creators with missing last seen information
    df_users = df_users[~df_users.last_seen.isnull()]
    
    #convert last seen datetime to acceptable format for Hubspot
    df_users['last_seen'] = df_users.last_seen.apply(lambda x: str(datetime.strptime(x[4:15], '%b %d %Y')))
    df_users['last_seen'] = df_users.last_seen.apply(lambda x: str(int((datetime(int(x[:4]),int(x[5:7]),int(x[8:10]))- datetime(1970, 1, 1)).total_seconds() * 1000)))
    
    return df_users


def pull_data(past_30_days, yesterday):
    
    """
    
    Pull app creator data from Mixpanel.
    
    
    Parameters
    ----------
    
    past_30_days: date
        Date of 30 days ago.
        
    yesterday: date
        Yesterday's date
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator information and activity from Mixpanel.
    
    """
    
    #pull app creator information and activity data
    df_mobile_preview = mobile_preview_pull(past_30_days, yesterday)
    time.sleep(60)
    df_editor = editor_pull(past_30_days, yesterday)
    time.sleep(60)
    df_SubscribeEmail = SubscribeEmail_pull(past_30_days, yesterday)
    time.sleep(60)
    df_Home_Page = Home_Page_pull(past_30_days, yesterday)
    time.sleep(60)
    df_Sql_Auth_Info = Sql_Auth_Info_pull(past_30_days, yesterday)
    time.sleep(60)

    #merge dataframes containing app creator information and activity data
    df_merge_temp = pd.merge(df_SubscribeEmail,df_Home_Page,on='user_id',how='outer')
    df_merge_temp = pd.merge(df_merge_temp,df_editor,on='user_id',how='outer')
    df_merge_temp = pd.merge(df_merge_temp,df_mobile_preview,on='user_id',how='outer')
    df_merge_temp = pd.merge(df_merge_temp,df_Sql_Auth_Info,on='user_id',how='outer')

    return df_merge_temp


def data_prep(df_final):
    
    """
    
    Identify potential MQLs and prep data to execute MQL generator.
   
        
    
    Parameters
    ----------
    
    df_final: dataframe
        Dataframe contains app creator info, number of Editor events, number of MobilePreview events, number of #SubscribeEmail events, and number of Home Page events.  
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    dataframe
        Dataframes containing potential MQL contacts and the associated MQL activity score.
    
    """

    #only considering contacts whose stages do not include marketingqualifiedlead, opportunity, customer, other, and salesqualifiedlead
    df_final = df_final[~df_final['hs_lifecycle_stage'].isin(['marketingqualifiedlead','opportunity','customer','other','salesqualifiedlead'])]

    #rename columns to align with MQL generation model
    df_final = df_final.rename(columns={"hs_analytics_num_page_views": "Number of Pageviews", 
                                                "company_foundedYear": "Year Founded", 
                                                "hs_analytics_source": "Original Source Type",
                                                "company_metrics_employees": "Number of Employees",
                                                "email": "Email",
                                                "hs_lifecycle_stage": "Lifecycle Stage"})

     
    #extract list of events from dataframe
    Editor_list = df_final['editor'].tolist()
    MobilePreview_list = df_final['MobilePreview'].tolist()
    SubscribeEmail_list = df_final['subscribe_email'].tolist()
    Home_Page_list = df_final['home_page'].tolist()
    
    #initialize list to track app creator MQL activity scores
    score_list = []
    
    #calculate activity score based on the following conditions: Editor > 138, MobilePreview > 14, #SubscribeEmail > 7, and Home Page > 8
    for i in range(len(df_final)):
        score = 0
        if Editor_list[i] >= 138:
            score += 1
        if MobilePreview_list[i] >= 14:
            score += 1
        if SubscribeEmail_list[i] >= 7:
            score += 1
        if Home_Page_list[i] >= 8:
            score += 1
        score_list.append(score)
        
    #log contact scores
    df_final['activity_score'] = score_list
    
    #hold for Method column
    df_final['Method'] = ''

    #remove contacts without emails
    df_final = df_final[~df_final.Email.isnull()]
    
    #turn mrr field into int
    df_final.loc[(df_final.hs_mrr == '')|(df_final.hs_mrr.isnull()), 'hs_mrr'] = 0
    df_final['hs_mrr'] = pd.to_numeric(df_final.hs_mrr) 
    
    return df_final

def model_execution(df_final, loaded_model):
    """
    
    Generate MQLs based on app creator information and activity data.
      
    
    Parameters
    ----------
    
    df_final: dataframe
        Dataframe containing app creator info and activity from Mixpanel to be used in MQL generation.
        
    loaded_model: DecisionTreeClassifier
        ML model used to classify MQLs.
        
    Global Variables
    ----------
    
    loaded_model: DecisionTreeClassifier
        ML model used to classify app creators.
    
    Returns
    ----------
    
    dataframe
        Dataframes containing MQL-classified app creator info, activity from Mixpanel, and MQL reason.
    
    """
    
    #number of employees and mrr thresholds for "Company Size" and "High MRR" MQLs
    employee_threshold = 500
    mrr = 50

    
    ###ABM Generated MQLs###
    print('\033[94m', 'ABM MQLs ', '\033[0m')
    print('')
    print('    Total Number of Contacts: ', len(df_final))

    #contact needs to meet be associated to any of these domains (ABM accounts) to be an ABM MQL
    df_ABM = df_final[df_final['Email'].str.contains('@att.com|@athensservices.com|@belk.com|@bridgewater.com|@costafarms.com|@ebsco.com|@husqvarnagroup.com|@issworld.com|@juul.com|@mortenson.com|@nestle.com|@nike.com|@opusteam.com|@shell.com|@smithgroup.com|@state.co.us|@stryker.com|@unilever.com|@vailresorts.com|@vice.com|@walmart.com|@coca-cola.com|@pepsico.com|@roche.com|@aep.com|@ge.com|@clearlink.com|@amazon.com|@curtisswright.com|@enerflex.com|@eogresources.com|@getcruise.com|@honeywell.com|@jpost.jp|@johnsoncontrols.com|@novonordisk.com|@polycor.com|@riwal.com|@snclavalin.com|@solvay.com|@sossecurity.com|@tarmac.com|@unipart.com|@group.issworld.com|@be.issworld.com|@dk.issworld.com|@us.issworld.com|.issworld.com|@juullabs.com|@ch.nestle.com|@us.nestle.com|@walmartlabs.com|@coca-cola.in|@ccbagroup.com|aia.com|fb.com|whirlpool.com|lime-energy.com|costco.com|riwal.com|disney.com|starbucks.com|parker.com|getcruise.com|homedepot.com|kubota.com|davey.com|oldcastle.com|inspirebrands.com|cbre.com|shell.com|.fb.com|.disney.com|oldcastlematerials.com|crhamericas.com|.shell.com')]
    
    #assign MQL method
    df_ABM['Method'] = 'ABM' 
    
    #generate ABM MQL dataframe
    df_ABM = df_ABM[['user_id', 'Email', 'Year Founded',
           'Number of Employees', 'Original Source Type', 'Number of Pageviews',
           'Method','activity_score', 'auth_source', 'mixpanel_distinct_id', 'last_seen']]

    #only keep parameters used in model and filter out user IDs
    ABM_id_list = df_ABM.user_id.tolist()
    df_not_ABM = df_final[~df_final.user_id.isin(ABM_id_list)]
    df_not_ABM = df_not_ABM[['user_id','Email',
           'Year Founded', 'Number of Employees',
           'Original Source Type', 'Number of Pageviews','activity_score','hs_mrr','sql_auth_info','company_category_industry', 'auth_source','mixpanel_distinct_id', 'last_seen']]

    print('    Number of Contacts Remaining: ', len(df_not_ABM))
    print('')
    print('\033[1m', '   Number of ABM Generated MQLs: ', len(df_ABM), '\033[0m')

    print('')
    print('------------------------------------------------------')
    print('')
    

    ###Automatic Generated MQLs###
    print('\033[94m', 'Automatic MQLs ', '\033[0m')
    print('')

    #contact needs to meet any of these conditions to be an automatic MQL
    df_automatic = df_not_ABM[(df_not_ABM['hs_mrr']>mrr)|(df_not_ABM['Number of Employees']>employee_threshold)|(df_not_ABM['auth_source']=='smartsheet')|
                              (df_not_ABM['sql_auth_info']>0)|(df_not_ABM['company_category_industry']=='Construction & Engineering')|
                              (df_not_ABM['company_category_industry']=='Renewable Electricity')|(df_not_ABM['company_category_industry']=='Electrical Equipment')|
                              (df_not_ABM['company_category_industry']=='Gas Utilities')|(df_not_ABM['company_category_industry']=='Electric Utilities')|
                              (df_not_ABM['company_category_industry']=='Utilities')
                             ]
    
    #assign MQL method
    df_automatic['Method'] = 'Automatic'
    
    #generate automatic MQL dataframe
    df_automatic = df_automatic[['user_id', 'Email', 'Year Founded',
           'Number of Employees', 'Original Source Type', 'Number of Pageviews',
           'Method','activity_score', 'auth_source', 'mixpanel_distinct_id', 'last_seen']]

    #only keep parameters used in model
    auto_id_list = df_automatic.user_id.tolist()
    df_not_automatic = df_not_ABM[~df_not_ABM.user_id.isin(auto_id_list)]
    df_not_automatic = df_not_automatic[['user_id','Email',
           'Year Founded', 'Number of Employees',
           'Original Source Type', 'Number of Pageviews','activity_score', 'auth_source', 'mixpanel_distinct_id', 'last_seen']]

    print('    Number of Contacts Remaining: ', len(df_not_automatic))
    print('')
    print('\033[1m', '   Number of Automatically Generated MQLs: ', len(df_automatic), '\033[0m')

    print('')
    print('------------------------------------------------------')
    print('')


    ###ML Model Generated MQLs###
    print('\033[94m', 'Company Features MQLs ', '\033[0m')
    print('')

    #dropping rows with nulls. This will remove contacts who do not have the attributes to be considered by the model.
    df_model = df_not_automatic.dropna()

    #print number of contacts going through the ML model
    print('    Number of Contacts Qualified for the Company Features Model: ', len(df_model))

    #turn Original Source Type into dummy variable
    df_post_pred = pd.concat([df_model, pd.get_dummies(df_model['Original Source Type'])], axis=1)
    df_post_pred['Direct Traffic'] = np.where(df_post_pred['Original Source Type']=='DIRECT_TRAFFIC', 1, 0)
    df_post_pred['Offline Sources'] = np.where(df_post_pred['Original Source Type']=='OFFLINE', 1, 0)

    #list of features used in model
    features = [
           'Number of Employees', 'Year Founded', 'Number of Pageviews',
           'Direct Traffic', 'Offline Sources']

    #execute ML MQL classifier 
    y_pred = loaded_model.predict(df_post_pred[features])
    df_post_pred['MQL'] = y_pred

    #record MQLs generated by the model
    df_new_model=df_post_pred[df_post_pred['MQL']==1]
    
    #assign MQL method. Calling ML model as "New Model" in method
    df_new_model['Method'] = 'New Model'
    
    #generate ML model MQL dataframe
    df_new_model = df_new_model[['user_id', 'Email','Year Founded',
           'Number of Employees', 'Original Source Type', 'Number of Pageviews',
           'Method','activity_score', 'auth_source', 'mixpanel_distinct_id', 'last_seen']]

    #only include contacts that do not have all the fields needed by the New Model
    company_model_id_list = df_new_model.user_id.tolist()
    df_remaining = df_not_automatic[~df_not_automatic.user_id.isin(company_model_id_list)]
    print('    Number of Contacts Remaining: ', len(df_remaining))
    print('')

    #print number of MQLs generated by the New Model
    print('\033[1m', '   Number of MQLs Generated Based On Company Features: ', len(df_new_model), '\033[0m')
    print('')
    print('------------------------------------------------------')
    print('')

    #MQLs based on ML model
    print('\033[94m', 'Activity Generated MQLs ','\033[0m')
    print('')

    
    ###Activity Generated MQLs###

    #MQL activity thresholds
    Editor_threshold = 138
    MobilePreview_threshold = 14
    SubscribeEmail_threshold = 7
    HomePage_threshold = 8

    #app creators need to meet 3/4 of the conditions to be considered as an activity generated MQL
    df_activity = df_remaining[(df_remaining['activity_score']>=3)]
    
    #generate ML model MQL dataframe
    df_activity['Method'] = 'Activity'
    df_activity = df_activity[[ 'user_id', 'Email', 'Year Founded',
           'Number of Employees', 'Original Source Type', 'Number of Pageviews',
           'Method','activity_score', 'auth_source', 'mixpanel_distinct_id', 'last_seen']]

    #print number of contacts going through random sample
    print('    Number of Contacts Remaining: ', len(df_remaining))
    print('')
    print('\033[1m', '   Number of MQLs Generated Based on User Activity: ', len(df_activity), '\033[0m')
    print('')
    print('------------------------------------------------------')
    print('------------------------------------------------------')
    print('')


    #merge MQLs from all of the methods and app creator activity data
    df_final_MQLs = pd.concat([df_ABM,df_new_model,df_activity,df_automatic])
    df_final_MQLs = pd.merge(df_final_MQLs,df_final[['Email','subscribe_email', 'home_page', 'editor', 'MobilePreview',
           'sql_auth_info', 'Lifecycle Stage', 'company_category_industry', 'hs_mrr']],on='Email',how='left')

    #reorder columns
    df_final_MQLs = df_final_MQLs[['user_id', 'Email','Method','activity_score','auth_source', 'Year Founded', 'Number of Employees', 'Original Source Type',
           'Number of Pageviews', 'subscribe_email',
           'home_page', 'editor', 'MobilePreview', 'sql_auth_info',
           'Lifecycle Stage', 'company_category_industry', 'hs_mrr', 'mixpanel_distinct_id', 'last_seen']]
    
    #remove duplicate contacts
    df_final_MQLs = df_final_MQLs.drop_duplicates('Email')
    print('\033[1m', "   Total Number of MQLs Generated: ", len(df_final_MQLs), '\033[0m')

    return df_final_MQLs


def prep_MQL_data(df_final_MQLs):

    """
    
    Prep MQL data before sending them out to Sales team.
    
       
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL reason/method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
    
    """
    
    #only keep features evaluated before sending to Sales team
    df_final_MQLs = df_final_MQLs[['user_id', 'Email','Method','Lifecycle Stage', 'activity_score', 'Year Founded', 'Number of Employees', 'Original Source Type',
           'Number of Pageviews', 'subscribe_email',
           'home_page', 'editor', 'MobilePreview', 'auth_source', 'sql_auth_info', 'company_category_industry','hs_mrr', 'mixpanel_distinct_id', 'last_seen']]

    #drop any duplicate contacts and filter out app creators who are students and were part of Growth Marketing user interviews
    df_final_MQLs = df_final_MQLs.drop_duplicates('Email')
    df_final_MQLs = df_final_MQLs[~df_final_MQLs.Email.isin(['derek.covey@gmail.com', 'silvia@glmconseil.com','amrutamherbals@gmail.com','kdcurrin308@gmail.com','digitalisystech@gmail.com', 'hannes.sm3@gmail.com', 'myvideoiproductions@gmail.com', 'planw12.61@gmail.com', 'alfaagent.bg@gmail.com', 'crewonsiteroofrats@gmail.com', 'Victoria.Gonzalez@fountainsgroup.co.uk'])]
    df_final_MQLs = df_final_MQLs[~df_final_MQLs.Email.str.contains("student")]
    
    #remove app creators who has an invalid user ID
    df_final_MQLs = df_final_MQLs[df_final_MQLs.user_id!=0]
     
    return df_final_MQLs


def add_contacts_to_hubspot_list(email_list, method):
    
    """
    
    Add app creators to HubSpot lists based on their MQL reasons.
    
    
    Parameters
    ----------
    
    email_list: list
        List of emails.
        
    method: str
        String of MQL reason.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function adds app creators to HubSpot lists based on their MQL reasons.
    
    """
    
    #mapping for reason to Hubspot list
    method_ID_dictionary = {'activity': 1011,
                            'sql': 1009,
                            'company_size': 1010,
                            'company_attributes': 980,
                            'construction' : 1012,
                            'MRR': 1017,
                            'ABM': 1018,
                            'smartsheet': 1046,
                            'renewable_electricity': 1047,
                            'electrical_equipment': 1048,
                            'gas_utilities': 1049,
                            'electric_utilities': 1050,
                            'utilities': 1051            
                           }
    
    #get list ID based on MQL reason
    list_id = method_ID_dictionary[method]
    
    #Update Hubspot list with new MQLs
    url ='https://api.hubapi.com/contacts/v1/lists/' + str(list_id) + '/add?hapikey=' + hubspot_key
    headers = {}
    headers['Content-Type']= 'application/json'
    data=json.dumps({
          "vids": [
          ],
          "emails": email_list
    })
    r = requests.post(data=data, url=url, headers=headers)
    
    
def push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, method):

    """
    
    Push generated MQLs to HubSpot with reason.
    
    
    Parameters
    ----------
    
    email_list: list
        List of emails.
        
    reason_list: list
        List of MQL reasons.
        
    MRR_list: list
        List of app creator MRRs.
        
    mixpanel_distinct_id_list: list
        List of Mixpanel distinct IDs
    
    last_seen_list: list
        List of Mixpanel last seen dates.
        
    method: str
        String of MQL reason. It is the higher level reason than those in reason_list.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function pushes generated MQLs to Hubspot with reason.
    
    """

    #Hubspot authentication
    requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)
    
    
    #push MQLs to HubSpot
    for email, reason, MRR, mixpanel_distinct_id, last_seen in zip(email_list,reason_list, MRR_list,mixpanel_distinct_id_list, last_seen_list):
        
        #generate Mixpanel profile, /manage, and app user domains links
        mixpanel_profile_link = 'https://mixpanel.com/report/487607/explore#user?distinct_id=' + mixpanel_distinct_id
        manage_profile_link = 'https://www.appsheet.com/manage/who?id=' + email
        app_user_domains_link = "https://mixpanel.com/report/487607/insights#~(displayOptions~(chartType~'table~plotStyle~'standard~analysis~'linear~value~'absolute)~isNewQBEnabled~true~sorting~(bar~(sortBy~'column~colSortAttrs~(~(sortBy~'value~sortOrder~'desc)~(sortBy~'value~sortOrder~'desc)))~line~(sortBy~'value~sortOrder~'desc)~table~(sortBy~'column~colSortAttrs~(~(sortBy~'label~sortOrder~'asc)~(sortBy~'label~sortOrder~'asc))))~columnWidths~(bar~())~title~'~querySamplingEnabled~false~sections~(show~(~(dataset~'!mixpanel~value~(name~'!all_people~resourceType~'people)~resourceType~'people~profileType~'people~search~'~dataGroupId~null~math~'total~property~null))~group~(~(dataset~'!mixpanel~value~'user_domains~resourceType~'people~profileType~null~search~'~dataGroupId~null~propertyType~'list~typeCast~null~unit~null))~filter~(clauses~(~(dataset~'!mixpanel~value~'!email~resourceType~'people~profileType~null~search~'~dataGroupId~null~filterType~'string~defaultType~'string~filterOperator~'equals~filterValue~(~'" + email + ")~propertyObjectKey~null))~determiner~'all)~time~(~(dateRangeType~'in*20the*20last~unit~'day~window~(unit~'day~value~30)))))"

        
        #generate HubSpot API call
        url ='http://api.hubapi.com/contacts/v1/contact/email/' + email + '/profile?hapikey=' + hubspot_key
        headers = {}
        headers['Content-Type']= 'application/json'
        data=json.dumps({
          "properties": [
            {
              "property": "lifecyclestage",
              "value": "marketingqualifiedlead"
            },
            {
              "property": "qualification_reason",
              "value": reason
            },
            {
              "property": "lead_source",
              "value": "App Creator"

            },
            {
              "property": "mrr",
              "value": MRR
            },
            {
              "property": "mixpanel_creator_profile",
              "value": mixpanel_profile_link
            },
            {
              "property": "manage_profile",
              "value": manage_profile_link
            },
            {
              "property": "app_user_domains",
              "value": app_user_domains_link
            },
            {
              "property": "mixpanel_last_activity_date",
              "value": last_seen
            }
          ]
        })

        #execute Hubspot API call to update app creator HubSpot profile with MQL information
        r = requests.post(data=data, url=url, headers=headers)
        
        #if app creator does not exist in HubSpot, then generate a new contact with MQL information
        if str(r) == '<Response [404]>': #checking if contact exists in hubspot
            
            #generate new contact Hubspot API call
            endpoint = 'https://api.hubapi.com/contacts/v1/contact/?hapikey=' + hubspot_key
            headers = {}
            headers["Content-Type"]="application/json"
            data = json.dumps({
              "properties": [
                                {
                                  "property": "email",
                                  "value": email
                                },
                                {
                                  "property": "lifecyclestage",
                                  "value": "marketingqualifiedlead"
                                },
                                {
                                  "property": "qualification_reason",
                                  "value": reason
                                },
                                {
                                  "property": "lead_source",
                                  "value": "App Creator"
                                },
                                {
                                  "property": "mrr",
                                  "value": MRR
                                },
                                {
                                  "property": "mixpanel_creator_profile",
                                  "value": mixpanel_profile_link
                                },
                                {
                                  "property": "manage_profile",
                                  "value": manage_profile_link
                                },
                                {
                                  "property": "app_user_domains",
                                  "value": app_user_domains_link
                                },
                                {
                                  "property": "mixpanel_last_activity_date",
                                  "value": last_seen
                                }
                              ]
                            })
            
            #execute Hubspot API call
            r = requests.post( url = endpoint, data = data, headers = headers )
            
        else: 
            print('Contact lifecycle stage successfully changed.')

    #add contact to HubSpot list based on qualification method
    add_contacts_to_hubspot_list(email_list, method)

def push_leads_to_intercom(email_list, method):

    """
    
    Tag generated leads in Intercom.
    
    
    Parameters
    ----------
    
    email_list: list
        List of emails.
        
    method: str
        String of MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function tags leads in Intercom.
    
    """

    #append 'lead'
    method = 'lead_' + method
    
    #push MQLs to HubSpot
    for email in email_list:
        try:
            
            #find user in Intercom
            user = intercom.users.find(email=email)
            
            #check if lead has been qualified. If not, then tag lead.
            if 'lead_tag_date_at' not in user.custom_attributes.keys():
            
                #tag lead
                intercom.tags.tag(name=method, users=[{'email': email}])

                #update lead tagged date
                user.custom_attributes["lead_tag_date_at"] = int(time.mktime(datetime.today().timetuple()))
                intercom.users.save(user)
                
            else:
                print('Lead has been tagged.')
            
        except Exception as e: 
            print(e)

    
def push_ABM_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send ABM MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send ABM MQLs over to HubSpot.
    
    """

    #filter on ABM MQLs
    df_final_MQLs_ABM = df_final_MQLs[df_final_MQLs['Method']=='ABM']
    df_final_MQLs_ABM['reason'] = 'ABM Account - Check Mixpanel user profile.'

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_ABM.Email.tolist()
    reason_list = df_final_MQLs_ABM.reason.tolist()
    MRR_list = df_final_MQLs_ABM.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_ABM.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_ABM.last_seen.tolist()
    
    #push MQLs to hubspot
    push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'ABM')
    print('Total ABM MQLs:', len(email_list))
                                                    
        
def push_construction_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Construction MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Construction MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by construction
    df_final_MQLs_Construction = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Construction & Engineering']
    df_final_MQLs_Construction['reason'] = 'Construction Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Construction = df_final_MQLs_Construction.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Construction.Email.tolist()
    reason_list = df_final_MQLs_Construction.reason.tolist()
    MRR_list = df_final_MQLs_Construction.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Construction.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Construction.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'construction')
    push_leads_to_intercom(email_list, 'construction')
    print('Total Construction MQLs:', len(email_list))
    
    
    
def push_Renewable_Electricity_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Renewable Electricity MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Renewable Electricity MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by Renewable Electricity
    df_final_MQLs_Renewable_Electricity = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Renewable Electricity']
    df_final_MQLs_Renewable_Electricity['reason'] = 'Renewable Electricity Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Renewable_Electricity = df_final_MQLs_Renewable_Electricity.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Renewable_Electricity.Email.tolist()
    reason_list = df_final_MQLs_Renewable_Electricity.reason.tolist()
    MRR_list = df_final_MQLs_Renewable_Electricity.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Renewable_Electricity.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Renewable_Electricity.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'renewable_electricity')
    push_leads_to_intercom(email_list, 'renewable_electricity')
    print('Total Renewable Electricity MQLs:', len(email_list))
    
    
def push_Electrical_Equipment_MQLs(df_final_MQLs):
    
    """ 
    
    Generate reasons and send Electrical Equipment MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Electrical Equipment MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by Electrical Equipment
    df_final_MQLs_Electrical_Equipment = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Electrical Equipment']
    df_final_MQLs_Electrical_Equipment['reason'] = 'Electrical Equipment Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Electrical_Equipment = df_final_MQLs_Electrical_Equipment.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Electrical_Equipment.Email.tolist()
    reason_list = df_final_MQLs_Electrical_Equipment.reason.tolist()
    MRR_list = df_final_MQLs_Electrical_Equipment.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Electrical_Equipment.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Electrical_Equipment.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'electrical_equipment')
    push_leads_to_intercom(email_list, 'electrical_equipment')
    print('Total Electrical Equipment MQLs:', len(email_list))
    
def push_Gas_Utilities_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Gas Utilities MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Gas Utilities MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by Gas_Utilities
    df_final_MQLs_Gas_Utilities = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Gas Utilities']
    df_final_MQLs_Gas_Utilities['reason'] = 'Gas Utilities Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Gas_Utilities = df_final_MQLs_Gas_Utilities.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Gas_Utilities.Email.tolist()
    reason_list = df_final_MQLs_Gas_Utilities.reason.tolist()
    MRR_list = df_final_MQLs_Gas_Utilities.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Gas_Utilities.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Gas_Utilities.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'gas_utilities')
    push_leads_to_intercom(email_list, 'gas_utilities')
    print('Total Gas Utilities MQLs:', len(email_list))

def push_Electric_Utilities_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Electric Utilities MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Electric Utilities MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by Electric_Utilities
    df_final_MQLs_Electric_Utilities = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Electric Utilities']
    df_final_MQLs_Electric_Utilities['reason'] = 'Electric Utilities Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Electric_Utilities = df_final_MQLs_Electric_Utilities.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Electric_Utilities.Email.tolist()
    reason_list = df_final_MQLs_Electric_Utilities.reason.tolist()
    MRR_list = df_final_MQLs_Electric_Utilities.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Electric_Utilities.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Electric_Utilities.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'electric_utilities')
    push_leads_to_intercom(email_list, 'electric_utilities')
    print('Total Electric Utilities MQLs:', len(email_list))
    
def push_Utilities_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Utilities MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Utilities MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by utilities
    df_final_MQLs_Utilities = df_final_MQLs_Auto[df_final_MQLs_Auto['company_category_industry']=='Utilities']
    df_final_MQLs_Utilities['reason'] = 'Utilities Company - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_Utilities = df_final_MQLs_Utilities.head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Utilities.Email.tolist()
    reason_list = df_final_MQLs_Utilities.reason.tolist()
    MRR_list = df_final_MQLs_Utilities.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Utilities.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Utilities.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'utilities')
    push_leads_to_intercom(email_list, 'utilities')
    print('Total Utilities MQLs:', len(email_list))
    
                                                    
def push_mrr_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send MRR MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send MRR MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by MRR that would not have fallen under the other reasons
    df_final_MQLs_MRR = df_final_MQLs_Auto[(df_final_MQLs_Auto['hs_mrr']>100)&(df_final_MQLs_Auto['company_category_industry']!='Construction & Engineering')
                                          &(df_final_MQLs_Auto['company_category_industry']!='Renewable Electricity')
                                          &(df_final_MQLs_Auto['company_category_industry']!='Electrical Equipment')
                                          &(df_final_MQLs_Auto['company_category_industry']!='Gas Utilities')
                                          &(df_final_MQLs_Auto['company_category_industry']!='Electric Utilities')
                                          &(df_final_MQLs_Auto['company_category_industry']!='Utilities')] #increased to 100 from 50
      
                        
                        
    df_final_MQLs_MRR['reason'] = 'High MRR User - Check Manage and Mixpanel activity dashboard.'

    #MQL cap
#     df_final_MQLs_MRR = df_final_MQLs_MRR.sort_values('hs_mrr',ascending=False).head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_MRR.Email.tolist()
    reason_list = df_final_MQLs_MRR.reason.tolist()
    MRR_list = df_final_MQLs_MRR.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_MRR.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_MRR.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'MRR')
    push_leads_to_intercom(email_list, 'MRR')
    print('Total MRR MQLs:', len(email_list))
    

def push_company_size_MQLs(df_final_MQLs):
    
    """ 
    
    Generate reasons and send Company Size MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Company Size MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by company size that would not have fallen under the other reasons
    df_final_MQLs_Company_Size = df_final_MQLs_Auto[(df_final_MQLs_Auto.sql_auth_info.isnull()) & (df_final_MQLs_Auto['hs_mrr']<=100) 
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Construction & Engineering')
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Renewable Electricity')
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Electrical Equipment')
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Gas Utilities')
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Electric Utilities')
                                                    &(df_final_MQLs_Auto['company_category_industry']!='Utilities')
                                                    &(df_final_MQLs_Auto['Number of Employees']>500)] #moved emplyee size from 50 to 5000
                 
    df_final_MQLs_Company_Size['reason'] = 'Company Size - Check Mixpanel user profile.'

    #MQL cap
    df_final_MQLs_Company_Size = df_final_MQLs_Company_Size.sort_values('Number of Employees',ascending=False).head(8) #moved from 15 to 8

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Company_Size.Email.tolist()
    reason_list = df_final_MQLs_Company_Size.reason.tolist()
    MRR_list = df_final_MQLs_Company_Size.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Company_Size.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Company_Size.last_seen.tolist()
    
    #push MQLs to hubspot
    push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'company_size')
    print('Total Company Size MQLs:', len(email_list))

    
def push_smartsheet_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Smartsheet MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Smartsheet MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by smartsheet auth that would not have fallen under the other reasons
    df_final_MQLs_smartsheet = df_final_MQLs_Auto[((df_final_MQLs_Auto['Number of Employees'].isnull()) | (df_final_MQLs_Auto['Number of Employees']<=500)) & (df_final_MQLs_Auto['hs_mrr']<=100)
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Construction & Engineering') 
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Renewable Electricity')
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Electrical Equipment')
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Gas Utilities')
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Electric Utilities')
                                                  &(df_final_MQLs_Auto['company_category_industry']!='Utilities') 
                                                  &(df_final_MQLs_Auto['auth_source']=='smartsheet')]
    
    
    
    df_final_MQLs_smartsheet['reason'] = 'Authenticated with Smartsheet - Check Mixpanel user profile.'

    #MQL cap
#     df_final_MQLs_smartsheet = df_final_MQLs_smartsheet.sort_values('editor',ascending=False).head(2)

    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_smartsheet.Email.tolist()
    reason_list = df_final_MQLs_smartsheet.reason.tolist()
    MRR_list = df_final_MQLs_smartsheet.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_smartsheet.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_smartsheet.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'smartsheet')
    push_leads_to_intercom(email_list, 'smartsheet')
    print('Total Smartsheet MQLs:', len(email_list))
    
def push_sql_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send SQL MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send SQL MQLs over to HubSpot.
    
    """

    #filter on auto MQLs
    df_final_MQLs_Auto = df_final_MQLs[df_final_MQLs['Method']=='Automatic']

    #get contacts who were automatically qualified by sql auth info that would not have fallen under the other reasons
    df_final_MQLs_SQL = df_final_MQLs_Auto[((df_final_MQLs_Auto['Number of Employees'].isnull()) | (df_final_MQLs_Auto['Number of Employees']<=500)) & (df_final_MQLs_Auto['hs_mrr']<=100) 
                                           &(df_final_MQLs_Auto['sql_auth_info']>0)
                                           &(df_final_MQLs_Auto['company_category_industry']!='Construction & Engineering') 
                                           &(df_final_MQLs_Auto['company_category_industry']!='Renewable Electricity')
                                           &(df_final_MQLs_Auto['company_category_industry']!='Electrical Equipment')
                                           &(df_final_MQLs_Auto['company_category_industry']!='Gas Utilities')
                                           &(df_final_MQLs_Auto['company_category_industry']!='Electric Utilities')
                                           &(df_final_MQLs_Auto['company_category_industry']!='Utilities')
                                           &(df_final_MQLs_Auto['auth_source']!='smartsheet')]
    
    df_final_MQLs_SQL['reason'] = 'Has or Attempted SQL Database Integration - Check Mixpanel activity dashboard.'

    #MQL cap
#     df_final_MQLs_SQL = df_final_MQLs_SQL.sort_values('editor',ascending=False).head(2) #move to 7 from 3

    #generate email, reason, mrr, mixpanel id and last seen lists
    email_list = df_final_MQLs_SQL.Email.tolist()
    reason_list = df_final_MQLs_SQL.reason.tolist()
    MRR_list = df_final_MQLs_SQL.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_SQL.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_SQL.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'sql') 
    push_leads_to_intercom(email_list, 'sql')
    print('Total SQL MQLs:', len(email_list))

    
def push_company_attributes_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send ML MQLs over to HubSpot.
    
    
    Parameters
    --------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send ML MQLs over to HubSpot.
    
    """

    #get contacts who were automatically qualified by company attributes
    df_final_MQLs_Company_Attributes = df_final_MQLs[df_final_MQLs['Method']=='New Model']
    df_final_MQLs_Company_Attributes['reason'] = 'Contact Attributes - Check Mixpanel user profile.'

    
    #MQL cap
#     df_final_MQLs_Company_Attributes = df_final_MQLs_Company_Attributes.head(2)
    
    #generate email, reason, mrr, mixpanel id, and last seen lists
    email_list = df_final_MQLs_Company_Attributes.Email.tolist()
    reason_list = df_final_MQLs_Company_Attributes.reason.tolist()
    MRR_list = df_final_MQLs_Company_Attributes.hs_mrr.tolist()
    mixpanel_distinct_id_list = df_final_MQLs_Company_Attributes.mixpanel_distinct_id.tolist()
    last_seen_list = df_final_MQLs_Company_Attributes.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'company_attributes') 
    push_leads_to_intercom(email_list, 'company_attributes')
    print('Total Company Attributes MQLs:', len(email_list))
    
def push_activity_MQLs(df_final_MQLs):
    
    """
    
    Generate reasons and send Activity MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function generates reasons and send Activity MQLs over to HubSpot.
    
    """
    
    #get contacts who were qualified by their activity
    df_final_MQLs_Activity = df_final_MQLs[df_final_MQLs['Method']=='Activity']
    
    #MQL cap
    activity_mql_list = df_final_MQLs_Activity.sort_values('editor',ascending=False)
    
    #create list of features used in model
    editor_list = activity_mql_list.editor.tolist()
    subscribe_email_list = activity_mql_list.subscribe_email.tolist()
    home_page_list = activity_mql_list.home_page.tolist()
    MobilePreview_list = activity_mql_list.MobilePreview.tolist()
    
    #Generate activity reason list
    reason_list = []
    for i in range(len(activity_mql_list)):
        message = 'Significant Activity - '
        activity_list = []
        if editor_list[i] >= 138:
            activity_list.append('Editor Session')
        if MobilePreview_list[i] >= 14:
            activity_list.append('App Preview')
        if subscribe_email_list[i] >= 7:
            activity_list.append('User Invites')
        if home_page_list[i] >= 8:
            activity_list.append('Website Visits')
        activity_list[len(activity_list) - 1] = 'and ' + activity_list[len(activity_list) - 1] + '. Check Mixpanel activity dashboard.'
        message = message + ', '.join(activity_list)
        reason_list.append(message)
    activity_mql_list['reasons'] = reason_list
    
    #generate email, reason, mrr, mixpanel id, and last seen lists
    reason_list = activity_mql_list['reasons'].tolist()
    email_list = activity_mql_list['Email'].tolist()
    MRR_list = activity_mql_list.hs_mrr.tolist()
    mixpanel_distinct_id_list = activity_mql_list.mixpanel_distinct_id.tolist()
    last_seen_list = activity_mql_list.last_seen.tolist()
    
    #push MQLs to hubspot
#     push_mqls_to_hubspot(email_list, reason_list, MRR_list, mixpanel_distinct_id_list, last_seen_list, 'activity') 
    push_leads_to_intercom(email_list, 'activity')
    print('Total Activity MQLs:', len(email_list))

    
def generate_mqls(loaded_model):

    """
    
    Generate and send MQLs over to HubSpot.
    
    
    Parameters
    ----------
    
    loaded_model: DecisionTreeClassifier
        ML model used to classify MQLs
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    df_final_MQLs: dataframe
        Dataframe contains MQL classified app creator info, activity from Mixpanel, and MQL method.
        
    
    """ 
     
    #pull information for all exist app creators in Mixpanel
    df_users = user_pull()
    
    #get date range
    yesterday = datetime.today() - timedelta(days=1)
    past_30_days = yesterday - timedelta(days=30)
    
    #generate MQLs
    df_merge_temp = pull_data(past_30_days,yesterday)
    df_final = pd.merge(df_merge_temp,df_users,on='user_id',how='left')
    df_final = data_prep(df_final)
    df_final_MQLs = model_execution(df_final, loaded_model) #execute model on the data
    df_final_MQLs = prep_MQL_data(df_final_MQLs) #prep MQL data

    #push generated MQLs to hubspot. Remove industry as a qualification reason to reduce volume
    push_ABM_MQLs(df_final_MQLs) #ABM generated MQLs
    push_construction_MQLs(df_final_MQLs) #construction generated MQLs
    push_Renewable_Electricity_MQLs(df_final_MQLs) #renewable electricity generated MQLs
    push_Electrical_Equipment_MQLs(df_final_MQLs) #electrical equipment generated MQLs
    push_Gas_Utilities_MQLs(df_final_MQLs) #gas utilities generated MQLs
    push_Electric_Utilities_MQLs(df_final_MQLs) #electric utilities generated MQLs
    push_Utilities_MQLs(df_final_MQLs) #utilities generated MQLs
    push_mrr_MQLs(df_final_MQLs) #MRR generated MQLs
    push_company_size_MQLs(df_final_MQLs) #company size generated MQLs
    push_smartsheet_MQLs(df_final_MQLs) #smartsheet generated MQLs
    push_sql_MQLs(df_final_MQLs) #sql auth info generated MQLs
    push_company_attributes_MQLs(df_final_MQLs) #company attributes generated MQLs
    push_activity_MQLs(df_final_MQLs) #activity generated MQLs

    return df_final_MQLs
    
    
def main():
    
    """
    Generate and send MQLs to Hubspot.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns 
    ----------
    
    None
        
    
    """

    #send initial email
    send_email("Initiate MQL Generator")
    
    #load MQL classifier model
    loaded_model = pickle.load(open('/home/phuong/automated_jobs/MQL/deal_predictor_decision_tree_final.sav', 'rb'))

    #get yesterday date
    yesterday = date.today() - timedelta(days=1)

    #execute MQL generator
    df_final_MQLs = generate_mqls(loaded_model)
    
    #send completed email
    send_email("MQL Generator Completed")

    return 



#execute update
if __name__ == "__main__":
    main()

