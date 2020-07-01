#CODE: COVID19
#import libraries
import os
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
import requests
from mixpanel import Mixpanel
from pt_utils.utils import send_email
import json

import stripe
stripe.api_key = os.environ.get('STRIPE_KEY')
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')



def pull_stripe_customers():
    
    """
    
    Pull Stripe customers who have used APAC promo code.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    
    #first call and process
    customers = stripe.Customer.list(limit=100)
    email_list = []
    for customer in customers:
        email = customer['email']
        for subscription in customer['subscriptions']['data']:
            discount = subscription['discount'] #get discount
            if discount is not None:
                if discount['coupon']['id'] == 'appsheet_promo-657': #identify accounts with COVID19 promo
                    print(email)
                    email_list.append(email)

    #get rest of customers using pagination                
    for customer in customers.auto_paging_iter():
        email = customer['email']
        for subscription in customer['subscriptions']['data']:
            discount = subscription['discount'] #get discount

            if discount is not None:
                if discount['coupon']['id'] == 'appsheet_promo-657': #identify accounts with COVID19 promo
                    print(email)
                    email_list.append(email)
                    
                    
    return email_list

def creator_pull():

    
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
                    "e.properties.$distinct_id"
                ],
                accumulator=Reducer.count()
            )


    #store emails, user IDs, and user journey stages in lists
    email_list = []
    distinct_id_list = []
    
    for row in query.send():
        if row['key'][0] is not None:
            email_list.append(row['key'][0])
            distinct_id_list.append(row['key'][1])
            

    #create dataframe 
    data = {'email':email_list, 'distinct_id': distinct_id_list}
    df_creators = pd.DataFrame(data=data)
    
 
    
    return df_creators

def push_contacts_to_hubspot(email_list):

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
        Function pushes contacts to Hubspot.
    
    """

    #Hubspot authentication
    requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)
    
    
    #push MQLs to HubSpot
    for email in email_list:
        
        
        #generate HubSpot API call
        url ='http://api.hubapi.com/contacts/v1/contact/email/' + email + '/profile?hapikey=' + hubspot_key
        headers = {}
        headers['Content-Type']= 'application/json'
        data=json.dumps({
          "properties": [
            {
              "property": "apac_promo",
              "value": True
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
                                  "property": "apac_promo",
                                  "value": True
                                },
                                { #change contact owner to Hyung
                                  "property": "hubspot_owner_id",
                                  "value": 39998583
                                }
                              ]
                            })
            
            #execute Hubspot API call
            r = requests.post( url = endpoint, data = data, headers = headers )
            
        else: 
            print('Contact APAC promo tag successfully changed.')

            
def update_mixpanel_apac_promo_status(df_creators):
    
    #update Mixpanel app creator profiles with use cases
    mixpanel_distinct_id = df_creators.distinct_id.tolist()
    for distinct_id in mixpanel_distinct_id:

        #populate params
        params = {'apac_promo_discount': True}

        #execute update
        mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        
    return





def main():
    
    """
    
    Notify Hyung of APAC Code Users
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """

    #sending initating email
    send_email('Initiate APAC Code Tagging')

        
    email_list = pull_stripe_customers() #get customer with COVID19 discount
    email_list = set(email_list) #remove duplicates from list
    email_list = [email.lower() for email in email_list] #lowercase all emails

    df_creators = creator_pull() #pull creators from Mixpanel
    df_creators['email'] = df_creators['email'].str.lower() #lowercase all email


    #only keep contacts with COVID19 promo
    df_creators = df_creators[df_creators.email.isin(email_list)]

    #update COVID19 status in Mixpanel
    update_mixpanel_apac_promo_status(df_creators)
    push_contacts_to_hubspot(email_list)
    
    #send completion email
    send_email('APAC Code Tagging Completed')
 
    
    return



#execute update
if __name__ == '__main__':
    main()
