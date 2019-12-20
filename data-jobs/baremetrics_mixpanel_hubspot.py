#import libraries
from mixpanel_jql import JQL, Reducer, Events, People
import pandas as pd
import numpy as np
import requests
from mixpanel import Mixpanel
import time
import json
import os
from tqdm import tqdm
from pt_utils.utils import send_email

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
baremetrics_key = os.environ.get('BAREMETRICS_KEY')
stripe_project_id = os.environ.get('STRIPE_PROJECT')
baremetrics_project_id = os.environ.get('BAREMETRICS_PROJECT')



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
                    "e.properties.$username"
                ],
                accumulator=Reducer.count()
            )


    #initialize lists to record app creator information
    email_list = []
    user_id_list = []
    for row in query_user.send():
        email_list.append(row['key'][0])
        user_id_list.append(row['key'][1])
        
    #create dataframe 
    data = {'email':email_list, 'user_id': user_id_list}
    df_users = pd.DataFrame(data=data)
    
    return df_users

def get_stripe_data():
    
    """
    
    Pull app creators Baremetrics data in the Stripe project.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    stripe_project_id: str
        String of Stripe project ID
     
    baremetrics_key: str
        String of API key used to make Baremetrics API calls.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator email and mrr.
    
    """
    
    #generate API call 
    url = 'https://api.baremetrics.com/v1/'+ stripe_project_id +'/customers'
    header = {'Authorization': 'Bearer ' + baremetrics_key} 
    page_value = 0
    querystring = {'page': page_value}

    #initialize lists to store emails and mrrs
    email_list = []
    current_mrr_list = []
    
    #execute first API call
    response = requests.get(url, headers = header, params=querystring).json()
    customer_list = response['customers']
    
    #use pagenation to make API calls and process responses
    page_value = 1  
    while len(customer_list) > 0:
        #get customer email and mrr
        for customer in customer_list:
            email_list.append(customer['email'])
            current_mrr_list.append(customer['current_mrr'])

        try:
            querystring = {'page': page_value}
            response = requests.get(url, headers = header, params=querystring).json()
            customer_list = response['customers']
            page_value += 1
        except Exception as e: 
            print(e)
            time.sleep(30*60)
                
        
    #generate dataframe
    data = {'email': email_list, 'current_mrr': current_mrr_list}
    df_client_mrr_stripe = pd.DataFrame(data)

    return df_client_mrr_stripe

def get_baremetrics_data():
    
    """
    
    Pull app creators Baremetrics data in the Baremetrics project.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    baremetrics_project_id: str
        String of Baremetrics project ID
     
    baremetrics_key: str
        String of API key used to make Baremetrics API calls.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator email and mrr.
    
    """
    
    #generate API call
    url = 'https://api.baremetrics.com/v1/' + baremetrics_project_id + '/customers'
    header = {'Authorization': 'Bearer ' + baremetrics_key}
    page_value = 0
    querystring = {'page': page_value}
    
    #initialize lists to store emails and mrrs
    email_list = []
    current_mrr_list = []

    #execute first API call
    response = requests.get(url, headers = header, params=querystring).json()
    customer_list = response['customers']

    #use pagenation to make API calls and process responses
    page_value = 1 
    while len(customer_list) > 0:
        #get customer email and mrr
        for customer in customer_list:
            email_list.append(customer['email'])
            current_mrr_list.append(customer['current_mrr'])
        
        try:
            querystring = {'page': page_value}
            response = requests.get(url, headers = header, params=querystring).json()
            customer_list = response['customers']
            page_value += 1
        except Exception as e: 
            print(e)
            time.sleep(30*60)
        
    #generate dataframe
    data = {'email': email_list, 'current_mrr': current_mrr_list}
    df_client_mrr_baremetrics = pd.DataFrame(data)
    
    return df_client_mrr_baremetrics

def execute_data_push():
    
    """
    
    Update Mixpanel app creator MRR with Baremetrics data.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    mp: Mixpanel object
        Mixpanel object used to app creator profiles in Mixpanel.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator email and mrr that were updated in Mixpanel.
    
    """

    #pull all Mixpanel app creator profiles and drop any that do not have complete information
    df_users = user_pull().dropna()

    #pull mrr data from Baremetrics
    df_client_mrr_stripe = get_stripe_data()
    df_client_mrr_baremetrics = get_baremetrics_data()

    #join data from Stripe and Baremetrics projects
    frames = [df_client_mrr_stripe, df_client_mrr_baremetrics]
    df_final = pd.concat(frames)

    #sum all mrr under one contract and normalize by dividing by 100
    df_final = df_final.groupby(['email'])['current_mrr'].sum().reset_index()
    df_final['current_mrr'] = df_final['current_mrr']/100

    #lower case 
    df_final.email = df_final.email.str.lower()
    df_users.email = df_users.email.str.lower()

    #remove blank spaces
    df_final.email = df_final.email.str.replace(' ', '')
    df_users.email = df_users.email.str.replace(' ', '')

    #replace certain emails with proper names
    df_final.loc[df_final.email == 'columbiaopsco', 'email'] = 'ben.rankin@columbiaopsco.com'

    #remove any users who do not have a positive mrr and mapping app creators in Baremetrics to profiles in Mixpanel
    df_final = df_final[df_final.current_mrr>0]
    df_user_mrr = pd.merge(df_final, df_users, on='email', how='left')

    #only keep Baremetrics accounts with Mixpanel profiles
    df_user_mrr_with_mixpanel = df_user_mrr[~df_user_mrr.user_id.isnull()]

    #every app creator whose Baremetrics emails are mapped to Mixpanel profiles are self served
    df_user_mrr_with_mixpanel['mrr_channel'] = 'self_serve'
    
    #keeping relevant columns
    df_user_mrr_with_mixpanel = df_user_mrr_with_mixpanel[['email', 'current_mrr', 'user_id', 'mrr_channel']]
    df_users_upload = df_user_mrr_with_mixpanel

    #keep the one associated with highest mrr
    df_users_upload = df_users_upload.sort_values('current_mrr', ascending=False)
    df_users_upload = df_users_upload.drop_duplicates('user_id',keep='first')

    #convert Mixpanel user ID to int
    df_users_upload['user_id'] = df_users_upload.user_id.astype(int)

    #extract user ID, mrr, emails, and channel to lists
    user_id_list = df_users_upload.user_id.tolist()
    mrr_list = df_users_upload.current_mrr.tolist()
    email_list = df_users_upload.email.tolist()
    mrr_channel_list = df_users_upload.mrr_channel.tolist()

    print('Number of users:', len(df_users_upload))
    
    #update Mixpanel app creator profiles
    for i in tqdm(range(len(user_id_list))):
        
        #extract app creator user ID, mrr, and mrr channel
        user_id = user_id_list[i]
        mrr = mrr_list[i]
        mrr_channel = mrr_channel_list[i]
        
        #populate Mixpanel parameters
        params = {'hs_mrr': mrr,
                  'hs_mrr_channel': mrr_channel
                 }
        
        #execute call to update Mixpanel profile
        mp.people_set(user_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
    
    #update mrr property in Hubspot   
    baremetrics_to_hubspot(email_list, mrr_list)

    return df_users_upload


def baremetrics_to_hubspot(email_list, MRR_list):

    """
    
    Update HubSpot profile MRR with Baremetrics data.
    
    
    Parameters
    ----------
    
    email_list: list
        List of emails.
    
    MRR_list: list
        List of MRRs.
    
    
    Global Variables
    ----------
    
    hubspot_key: str
        String of HubSpot API key to make API call
        
    
    Returns
    ----------
    
    None


    """
    
    #HubSpot authentication
    requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)
    
    #update profiles in HubSpot
    for email,MRR in zip(email_list, MRR_list):
        
        #generate API call
        url ='http://api.hubapi.com/contacts/v1/contact/email/' + email + '/profile?hapikey=' + hubspot_key
        headers = {}
        headers['Content-Type']= 'application/json'
        data=json.dumps({
          "properties": [
            {
              "property": "mrr",
              "value": MRR

            }
          ]
        })
        
        
        #execute API call
        r = requests.post(data=data, url=url, headers=headers)
        
    return 


def main():
    
    """
    Update Mixpanel and HubSpot profiles with Baremetrics MRR data.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
    
    """
    
    #send initial email
    send_email("Initiate Baremetrics MRR to Mixpanel & Hubspot")
    
    #execute update
    df_upload = execute_data_push()
    
    #send completed email
    send_email("Baremetrics MRR to Mixpanel & Hubspot Completed")
    
    return 


#execute update
if __name__ == "__main__":
    main()
    


