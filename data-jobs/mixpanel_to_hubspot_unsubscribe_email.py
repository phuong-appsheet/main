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
import requests
import json
import time
from pt_utils.utils import send_email


#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY')) 
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')



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
                    "e.properties.$email",
                    "e.properties.$unsubscribed"],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    unsubscribed_list = []
    for row in query_user.send():
        if ((row['key'][0] is not None) & (row['key'][1] is not None)): #only keep accounts with both email and unsubscribe status
            email_list.append(row['key'][0])
            unsubscribed_list.append(row['key'][1])

    #create dataframe 
    data = {'email': email_list, 'unsubscribed': unsubscribed_list}
    df_creators = pd.DataFrame(data=data)
    
    
    return df_creators

def update_contacts_to_hubspot(email_list):

    """
    
    Push contacts to HubSpot.
    
    
    Parameters
    ----------
    
    email_list: list
        List of emails.
        
        
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        Function update contacts' "Unsubscribed Mixpanel Emails" in HubSpot.
    
    """

    #Hubspot authentication
    requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)
    
    #push MQLs to HubSpot
    i = 0 #initiate counter
    while i < len(email_list):
        
        #get email
        email = email_list[i]
        
        #generate HubSpot API call
        url ='http://api.hubapi.com/contacts/v1/contact/email/' + email + '/profile?hapikey=' + hubspot_key
        headers = {}
        headers['Content-Type']= 'application/json'
        data=json.dumps({
          "properties": [
            {
              "property": "unsubscribed_mixpanel_emails",
              "value": True
            }
          ]
        })

        #execute Hubspot API call to update app creator HubSpot profile with MQL information
        r = requests.post(data=data, url=url, headers=headers)
        
        #we don't need to create new HubSpot contacts if they don't exist

        if str(r) == '<Response [429]>': #checking api rate limit was reached
            print("API rate limit reached. Waiting 10 secs.")
            time.sleep(13)
        else: #if limit was not reached, increase index
            i += 1
            
        if str(r) == '<Response [204]>': #if update was successful, print email
            print(email) 
    return


def main():
    
    """
    
    Update HubSpot contacts with Mixpanel email unsubscribe status.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """

    #sending initating email
    send_email('Initiate Mixpanel to HubSpot Unsubscribe Email Sync')

    #pull list of unsubscribed app creators  
    df_creators = creator_pull()
    df_creators['email'] = df_creators['email'].str.lower() #lowercase all email
    email_list = df_creators.email.tolist()
    
    #update hubspot contacts
    update_contacts_to_hubspot(email_list)

    #send completion email
    send_email('Mixpanel to HubSpot Unsubscribe Email Sync Completed')
 
    
    return



#execute update
if __name__ == '__main__':
    main()
