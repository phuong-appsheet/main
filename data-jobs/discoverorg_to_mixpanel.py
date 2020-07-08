#import libraries
from datetime import datetime, date, timedelta
from mixpanel_jql import JQL, Reducer, Events, People
import pandas as pd
import numpy as np
from mixpanel import Mixpanel
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
from pt_utils.utils import send_email

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY')) 
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
GOOGLE_APPLICATION_CREDENTIALS= os.path.expanduser(os.environ.get('BQ_CLIENT_SECRETS'))



def get_new_signup(yesterday):
    
    """
    Get yesterday new signup.
    
    
    Parameters
    ----------
    
    yesterday: date
        Yesterday's date.
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key.
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing user IDs and emails of creators who signed up yesterday.
    
    """

    # New Signup Web
    query = JQL(
                api_creator_secret,
                events=Events({
                    'event_selectors': [{'event': "New Signup Web"}],
                    'from_date': yesterday,
                    'to_date': yesterday
                })
                ,people=People({
                    'user_selectors': [

                    ]
                }),
                join_params={
                    'type': 'full',
                    'selectors': [
                        {'event': "New Signup Web"}
                    ]
                }
            )



    #store email, user id, and sign up events
    email_list = []
    userid_list = []
    for row in query.send():
        if 'user' in list(row.keys()):
            if '$username' in list(row['user']['properties'].keys()):
                userid_list.append(row['user']['properties']['$username'])
            if '$email' in list(row['user']['properties'].keys()):
                email_list.append(row['user']['properties']['$email'])

    #create dataframe 
    data = {'user_id': userid_list,'email':email_list}
    df_new_users = pd.DataFrame(data=data)
    
    return df_new_users


def auth_google(GOOGLE_APPLICATION_CREDENTIALS):

    """
    Authenticate into Google service account.
    
    
    Parameters
    ----------
    
        
        
    Global Variables
    ----------
    
    GOOGLE_APPLICATION_CREDENTIALS: Google app credentials.
        Use to make queries to BigQuery table.
        
    
    Returns
    ----------
    
    BigQuery Client
        Client used to query BigQuery.
    
    """

    #initiate service accounts
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_APPLICATION_CREDENTIALS,
        scopes=["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive"],
    )
    
    
    #initiate Google client
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    
    return client


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
    query_user = JQL(
                api_creator_secret,
                people=People({
                    'user_selectors': [{
                    }

                ]

                })
            ).group_by(
                keys=[
                    "e.properties.$distinct_id",
                    "e.properties.$username"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    distinct_id_list = []
    user_id_list = []
    for row in query_user.send():
        distinct_id_list.append(row['key'][0])
        user_id_list.append(row['key'][1])

    #create dataframe 
    data = {'distinct_id': distinct_id_list, 'user_id': user_id_list}
    df_creators = pd.DataFrame(data=data)
    return df_creators




def execute_query(client, email_domain):
    
    """
    Execute query in BigQuery and enrich Mixpanel profiles.
    
    
    Parameters
    ----------
    
        
        
    Global Variables
    ----------
    
    client: BigQuery Client
        Client used to query BigQuery.
        
    
    Returns
    ----------
    
    """
    
    #generate query
    query = """
        SELECT
          company_email_domain,
          company_name,
          company_website,
          company_phone,
          company_primary_industry,
          company_revenue,
          number_of_employees,
          year_founded,
          hq_country
        FROM 
          discoverorg_company.company
        WHERE
          company_email_domain = '{0}'
        """.format(email_domain)
    
    query_job = client.query(query)

    results = query_job.result().to_dataframe()  # Waits for job to complete.
    return results





def enrich_profiles(client, email_domains_list, distinct_id_list):


    """
    Query company information in DiscoverOrg and update Mixpanel profiles.
    
    
    Parameters
    ----------
    
    client: BigQuery client.
        Client used to query BigQuery.
    
    email_domains_list: list
        List of email domains.
        
    distinct_id_list: list
        List of distinct IDs.
        
        
    Global Variables
    ----------
        
    mp: Mixpanel client.
        Client used to update profiles in Mixpanel.
        
    
    Returns
    ----------
    
    
    """
    
    
    #build out query and extract profile properties.
    for email_domain, distinct_id in zip(email_domains_list, distinct_id_list):
        
        
        #execute query
        df_results = execute_query(client, email_domain)

        if len(df_results)>0:

            print(email_domain)
            print(distinct_id)
        
            #initialize parameters to update in Mixpanel
            params = {}

            #extract parameters from query result
            if df_results['company_name'].tolist()[0] is not None:
                params['Company Name'] = df_results['company_name'].tolist()[0]

            if df_results['company_phone'].tolist()[0] is not None:
                params['company_phone'] = df_results['company_phone'].tolist()[0]

            if df_results['company_revenue'].tolist()[0] is not None:
                params['company_metrics_annualRevenue'] = df_results['company_revenue'].tolist()[0]

            if df_results['number_of_employees'].tolist()[0] is not None:
                params['company_metrics_employees'] = df_results['number_of_employees'].tolist()[0]

            if df_results['hq_country'].tolist()[0] is not None:
                params['company_geo_country'] = df_results['hq_country'].tolist()[0]

            if df_results['company_primary_industry'].tolist()[0] is not None:
                params['company_category_industry'] = df_results['company_primary_industry'].tolist()[0]

            if df_results['year_founded'].tolist()[0] is not None:
                params['company_foundedYear'] = df_results['year_founded'].tolist()[0]


            #update mixpanel profile
            mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})

    return 


def main():
    
    """
    Enrich new Mixpanel profiles.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    #send initial email
    send_email("Initiate DiscoverOrg Enrichment")
    
    #authenticate with Google
    client = auth_google(GOOGLE_APPLICATION_CREDENTIALS)

    #get all users mixpanel and new users from yesterday
    yesterday = date.today() - timedelta(days=1)
    df_new_users = get_new_signup(yesterday)
    df_new_users= df_new_users.drop_duplicates('email', keep='first')
    df_creators = creator_pull()
    df_new_users = pd.merge(df_new_users, df_creators, on='user_id', how='left').drop_duplicates('distinct_id')
    
    #remove contacts with @gmail.com, hotmail.com, yahoo.com, and outlook.com since Clearbit wouldn't be able to enrich them
    df_new_users = df_new_users[~df_new_users['email'].str.contains("gmail.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("hotmail.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("yahoo.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("outlook.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("googlemail.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("hotmail.co.uk")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("outlook.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("aol.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("icloud.com")]
    df_new_users = df_new_users[df_new_users['user_id']!=0]


    #separate out domains
    df_new_users['user_email_domains'] = df_new_users['email'].str.split('@').str[1].fillna('')

    #get user email and ID
    email_domains_list = df_new_users.user_email_domains.tolist()
    distinct_id_list = df_new_users.distinct_id.tolist()
    
    #enrich profiles in Mixpanel
    enrich_profiles(client, email_domains_list, distinct_id_list)
    
    #send email when completed
    send_email("DiscoverOrg Enrichment Completed")
    
    
    return 


 
#execute update
if __name__ == "__main__":
    main()
