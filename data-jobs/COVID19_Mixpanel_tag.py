#pagination to list every customer: https://stackoverflow.com/questions/39522963/list-all-customers-via-stripe-api



#CODE: COVID19
#import libraries
import os
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
import requests
from mixpanel import Mixpanel
from pt_utils.utils import send_email


import stripe
stripe.api_key = os.environ.get('STRIPE_KEY')
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')




def pull_stripe_customers():
    
    
    #first call and process
    customers = stripe.Customer.list(limit=100)
    email_list = []
    for customer in customers:
        email = customer['email']
        for subscription in customer['subscriptions']['data']:
            discount = subscription['discount'] #get discount
            if discount is not None:
                if discount['coupon']['id'] == 'COVID19': #identify accounts with COVID19 promo
                    print(email)
                    email_list.append(email)

    #get rest of customers using pagination                
    for customer in customers.auto_paging_iter():
        email = customer['email']
        for subscription in customer['subscriptions']['data']:
            discount = subscription['discount'] #get discount

            if discount is not None:
                if discount['coupon']['id'] == 'COVID19': #identify accounts with COVID19 promo
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


def update_mixpanel_covid19_status(df_creators):
    
    #update Mixpanel app creator profiles with use cases
    mixpanel_distinct_id = df_creators.distinct_id.tolist()
    for distinct_id in mixpanel_distinct_id:

        #populate params
        params = {'COVID19_discount': True}

        #execute update
        mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        
    return



def main():
    
    """
    
    Update app creator COVID promo status in Mixpanel
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """

    #sending initating email
    send_email('Initiate COVID19 Mixpanel Tagging')

        
    email_list = pull_stripe_customers() #get customer with COVID19 discount
    email_list = set(email_list) #remove duplicates from list
    email_list = [email.lower() for email in email_list] #lowercase all emails

    df_creators = creator_pull() #pull creators from Mixpanel
    df_creators['email'] = df_creators['email'].str.lower() #lowercase all email


    #only keep contacts with COVID19 promo
    df_creators = df_creators[df_creators.email.isin(email_list)]

    #update COVID19 status in Mixpanel
    update_mixpanel_covid19_status(df_creators)
    
    #send completion email
    send_email('COVID19 Mixpanel Tagging Completed')

    
    return



#execute update
if __name__ == '__main__':
    main()
