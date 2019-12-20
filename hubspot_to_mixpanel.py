#import libraries
from datetime import datetime, timedelta
from mixpanel_jql import JQL, Reducer, Events, People
import pandas as pd
import numpy as np
import requests
from mixpanel import Mixpanel
import json 
from multiprocessing.pool import ThreadPool as Pool
import concurrent.futures
from tqdm import tqdm
import os
from pt_utils.utils import send_email

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')
gmail_account = os.environ.get('GMAIL_USERNAME')
gmail_password = os.environ.get('GMAIL_PASSWORD')

def update_fields(user_email,mixpanel_distinct_id):  
    
    
    """
    Update Mixpanel app creator profile with Hubspot data.
    
    
    Parameters
    ----------
    
    user_email: str
        String of app creator email.
        
    mixpanel_distinct_id: str
        String of app creator Mixpanel distinct ID
     
     
    Global Variables
    ----------
    
    hubspot_key: str
        String of HubSpot API key used to make API calls.
    
    Returns 
    ----------
    
    None
        
    
    """
    
    #HubSpot API call to contact profile
    contact_url= 'https://api.hubapi.com/contacts/v1/contact/email/'+ user_email + '/profile?hapikey=' + hubspot_key
    contact_response = requests.get(url=contact_url).json()

    #get contact keys
    contact_response_keys = list(contact_response.keys())
    
    #initialize parameters to update Mixpanel profile
    params = {'hs_utmcampaign': '',
                       'hs_utmcontent': '',
                       'hs_utmmedium': '',
                       'hs_utmsource': '',
                       'hs_utmterm': '',
                       'hs_recent_conversion': '',
                       'hs_lifecycle_stage': '',
                       'hs_lead_status': '',
                       'hs_lead_source': '',
                       'hs_last_modified_date': '',
                       'hs_last_mktg_email_name': '',
                       'hs_mktg_email_last_open_date': '',
                       'hs_mktg_email_last_send_date': '',
                       'hs_last_activity_date': '',
                       'hs_last_contacted': '',
                       'hs_appsheet_plan': '',
                       'hs_owner': '',
                       'hs_became_MQL_date': '',
                       'hs_num_associated_deals': '',
                       'hs_comp_revenue': '',
                       'hs_create_date': '',
                       'hs_zautomationsource': '',
                       'hs_analytics_num_page_views': '',
                       'hs_analytics_source': '',
                       'hs_MQL_reason': '',
                       'hs_remarket_reason': '',
                       'hs_contact_became_MQL_date': '',
                       
             }
   
    #check if contact exists in hubspot
    if 'message' not in contact_response_keys:

    
        #contact properties
        contact_properties = contact_response['properties']
        
       
        #contact became a Marketing Qualified Lead Date
        hs_MQL_date = ''
        if 'hs_lifecyclestage_marketingqualifiedlead_date' in contact_properties:
            timestamp = ''.join(contact_properties['hs_lifecyclestage_marketingqualifiedlead_date']['value'].split())[:-3]
            if timestamp != '':
                hs_MQL_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_became_MQL_date'] = hs_MQL_date
        
        #remarketing reason
        hs_remarket_reason = ''
        if 'remarket_reason' in contact_properties:
            hs_remarket_reason = contact_properties['remarket_reason']['value']
            if hs_remarket_reason != '':
                params['hs_remarket_reason'] = hs_remarket_reason
                
        #qualification reason
        hs_qualification_reason = ''
        if 'qualification_reason' in contact_properties:
            hs_qualification_reason = contact_properties['qualification_reason']['value']
            if hs_qualification_reason != '':
                params['hs_MQL_reason'] = hs_qualification_reason 
                
        #contact original source
        hs_analytics_source = ''
        if 'hs_analytics_source' in contact_properties:
            hs_analytics_source = contact_properties['hs_analytics_source']['value']
            if hs_analytics_source != '':
                params['hs_analytics_source'] = hs_analytics_source 
        
        #contact number of page views
        hs_analytics_num_page_views = ''
        if 'hs_analytics_num_page_views' in contact_properties:
            hs_analytics_num_page_views = contact_properties['hs_analytics_num_page_views']['value']
            if hs_analytics_num_page_views != '':
                params['hs_analytics_num_page_views'] = hs_analytics_num_page_views 
        
        #contact automation source
        hs_zautomationsource = ''
        if 'zautomationsource' in contact_properties:
            hs_zautomationsource = contact_properties['zautomationsource']['value']
            if hs_zautomationsource != '':
                params['hs_zautomationsource'] = hs_zautomationsource
                
        #contact utmcampaign
        hs_utmcampaign = ''
        if 'utmcampaign' in contact_properties:
            hs_utmcampaign = contact_properties['utmcampaign']['value']
            if hs_utmcampaign != '':
                params['hs_utmcampaign'] = hs_utmcampaign                                  

        #contact utmcontent
        hs_utmcontent = ''
        if 'utmcontent' in contact_properties:
            hs_utmcontent = contact_properties['utmcontent']['value']
            if hs_utmcontent != '':
                params['hs_utmcontent'] = hs_utmcontent

        #contact utmmedium 
        hs_utmmedium = ''
        if 'utmmedium' in contact_properties:
            hs_utmmedium = contact_properties['utmmedium']['value']
            if hs_utmmedium != '':
                params['hs_utmmedium'] = hs_utmmedium

        #contact utmsource
        hs_utmsource = ''
        if 'utmsource' in contact_properties:
            hs_utmsource = contact_properties['utmsource']['value']
            if hs_utmsource != '':
                params['hs_utmsource'] = hs_utmsource

        #contact utmterm
        hs_utmterm = ''
        if 'utmterm' in contact_properties:
            hs_utmterm = contact_properties['utmterm']['value']
            if hs_utmterm != '':
                params['hs_utmterm'] = hs_utmterm

        #contact recent conversion
        hs_recent_conversion = ''
        if 'recent_conversion_event_name' in contact_properties:
            hs_recent_conversion = contact_properties['recent_conversion_event_name']['value']
            if hs_recent_conversion != '':
                params['hs_recent_conversion'] = hs_recent_conversion

        #contact life cycle stage
        hs_lifecycle_stage = ''
        if 'lifecyclestage' in contact_properties:
            hs_lifecycle_stage = contact_properties['lifecyclestage']['value']
            if hs_lifecycle_stage != '':
                params['hs_lifecycle_stage'] = hs_lifecycle_stage

        #contact lead status
        hs_lead_status = ''
        if 'hs_lead_status' in contact_properties:
            hs_lead_status = contact_properties['hs_lead_status']['value']
            if hs_lead_status != '':
                params['hs_lead_status'] = hs_lead_status

        #concat lead source
        hs_lead_source = ''
        if 'lead_source' in contact_properties:
            hs_lead_source = contact_properties['lead_source']['value']
            if hs_lead_source != '':
                params['hs_lead_source'] = hs_lead_source

        #contact last modified date
        hs_last_modified_date = ''
        if 'lastmodifieddate' in contact_properties:
            timestamp = ''.join(contact_properties['lastmodifieddate']['value'].split())[:-3]
            if timestamp != '':
                hs_last_modified_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M:%S") #converting time stamp to date
                params['hs_last_modified_date'] = hs_last_modified_date

        #contact last email name
        hs_last_mktg_email_name = ''
        if 'hs_email_last_email_name' in contact_properties:
            hs_last_mktg_email_name = contact_properties['hs_email_last_email_name']['value']
            if hs_last_mktg_email_name != '':
                params['hs_last_mktg_email_name'] = hs_last_mktg_email_name

        #contact last email open date
        hs_mktg_email_last_open_date = ''
        if 'hs_email_last_open_date' in contact_properties:
            timestamp = ''.join(contact_properties['hs_email_last_open_date']['value'].split())[:-3]
            if timestamp != '':
                hs_mktg_email_last_open_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_mktg_email_last_open_date'] = hs_mktg_email_last_open_date

        #contact last email sent
        hs_mktg_email_last_send_date = ''
        if 'hs_email_last_send_date' in contact_properties:
            timestamp = ''.join(contact_properties['hs_email_last_send_date']['value'].split())[:-3]
            if timestamp != '':
                hs_mktg_email_last_send_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_mktg_email_last_send_date'] = hs_mktg_email_last_send_date

        #contact last activity Date
        hs_last_activity_date = ''
        if 'notes_last_updated' in contact_properties:
            timestamp = ''.join(contact_properties['notes_last_updated']['value'].split())[:-3]
            if timestamp != '':
                hs_last_activity_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_last_activity_date'] = hs_last_activity_date

                #contact create date
        hs_create_date = ''
        if 'createdate' in contact_properties:
            timestamp = ''.join(contact_properties['createdate']['value'].split())[:-3]
            if timestamp != '':
                hs_create_date = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_create_date'] = hs_create_date
                
        #contact last contacted
        hs_last_contacted = ''
        if 'notes_last_contacted' in contact_properties:
            timestamp = ''.join(contact_properties['notes_last_contacted']['value'].split())[:-3]
            if timestamp != '':
                hs_last_contacted = (datetime.utcfromtimestamp(int(timestamp)) + timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  #converting time stamp to date
                params['hs_last_contacted'] = hs_last_contacted

        #contact plan on using appsheet
        hs_appsheet_plan = ''
        if 'how_is_your_company_planning_to_utilize_appsheet_as_a_mobile_app_solution_platform_to_customers_' in contact_properties:
            hs_appsheet_plan = contact_properties['how_is_your_company_planning_to_utilize_appsheet_as_a_mobile_app_solution_platform_to_customers_']['value']
            if hs_appsheet_plan != '':
                params['hs_appsheet_plan'] = hs_appsheet_plan
 
        #contact owner
        hs_owner = ''
        if 'hubspot_owner_id' in contact_properties:
            contact_owner_id = contact_properties['hubspot_owner_id']['value']
            if contact_owner_id != '':
                #get contact owner name
                owner_url = 'http://api.hubapi.com/owners/v2/owners/' + contact_owner_id +'?hapikey=' + hubspot_key
                headers = {'Content-Type': "application/json"}
                owner_response = requests.get(url=owner_url).json()
                owner_first_name = owner_response['firstName']
                owner_last_name = owner_response['lastName']

                #concatenate owner name
                hs_owner = owner_first_name + " " + owner_last_name
                params['hs_owner'] = hs_owner


        # contact number of associated Deal
        hs_num_associated_deals = ''
        if 'num_associated_deals' in contact_properties:
            hs_num_associated_deals = contact_properties['num_associated_deals']['value']
            if hs_num_associated_deals != '':
                params['hs_num_associated_deals'] = hs_num_associated_deals


        #get company id
        hs_comp_revenue = ''
        if 'associatedcompanyid' in list(contact_properties.keys()):
            company_id = contact_properties['associatedcompanyid']['value']

            #calling hubspot company api
            company_url = "https://api.hubapi.com/companies/v2/companies/" + company_id
            headers = {'Content-Type': "application/json"}
            querystring = {"hapikey": hubspot_key}
            company_response = requests.get(url=company_url, params = querystring).json()


            #get company keys
            company_properties = company_response['properties']
            company_response_keys = list(company_properties.keys())


            #company total revenue
            if 'total_revenue' in company_properties:
                hs_comp_revenue = company_properties['total_revenue']['value']
                if hs_comp_revenue != '':
                    params['hs_comp_revenue'] = hs_comp_revenue    

        #make api call
        mp.people_set(mixpanel_distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        

        #check to see if there are already Mixpanel and /manage links in HubSpot profiles. Only create links if they do not exist
        if 'mixpanel_creator_profile' not in contact_properties:
        
            #update Hubspot mixpanel, app user domains, and manage links
            mixpanel_profile_link = 'https://mixpanel.com/report/487607/explore#user?distinct_id=' + mixpanel_distinct_id
            manage_profile_link = 'https://www.appsheet.com/manage/who?id=' + user_email
            app_user_domains_link = "https://mixpanel.com/report/487607/insights#~(displayOptions~(chartType~'table~plotStyle~'standard~analysis~'linear~value~'absolute)~isNewQBEnabled~true~sorting~(bar~(sortBy~'column~colSortAttrs~(~(sortBy~'value~sortOrder~'desc)~(sortBy~'value~sortOrder~'desc)))~line~(sortBy~'value~sortOrder~'desc)~table~(sortBy~'column~colSortAttrs~(~(sortBy~'label~sortOrder~'asc)~(sortBy~'label~sortOrder~'asc))))~columnWidths~(bar~())~title~'~querySamplingEnabled~false~sections~(show~(~(dataset~'!mixpanel~value~(name~'!all_people~resourceType~'people)~resourceType~'people~profileType~'people~search~'~dataGroupId~null~math~'total~property~null))~group~(~(dataset~'!mixpanel~value~'user_domains~resourceType~'people~profileType~null~search~'~dataGroupId~null~propertyType~'list~typeCast~null~unit~null))~filter~(clauses~(~(dataset~'!mixpanel~value~'!email~resourceType~'people~profileType~null~search~'~dataGroupId~null~filterType~'string~defaultType~'string~filterOperator~'equals~filterValue~(~'" + user_email + ")~propertyObjectKey~null))~determiner~'all)~time~(~(dateRangeType~'in*20the*20last~unit~'day~window~(unit~'day~value~30)))))"

            #HubSpot authentication
            requests.get('https://api.hubapi.com/contacts/v1/lists/all/contacts/all\?hapikey\=' + hubspot_key)
            
            #generate API call
            url ='http://api.hubapi.com/contacts/v1/contact/email/' + user_email + '/profile?hapikey=' + hubspot_key
            headers = {}
            headers['Content-Type']= 'application/json'
            data=json.dumps({
              "properties": [
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
                }
              ]
            })

            r = requests.post(data=data, url=url, headers=headers)
        
    return 
   
    
    
def pull_creators():
    
    """
    
    Pull app creators Mixpanel information.
    
    
    Parameters
    ----------
    
        
        
    Global Variables
    ----------
    
    api_creator_secret: Mixpanel Creator Project API key
        Use to make API calls to Mixpanel Creator Project.
        
    
    Returns
    ----------
    
    dataframe
        Dataframe containing app creator information from Mixpanel.
    
    """
    
    #generate JQL query
    query_category = JQL(
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


    #initialize lists to store emails and distinct IDs
    email_list = []
    distinct_id_list = []
    
    #process query response
    for row in query_category.send():
        email_list.append(row['key'][0])
        distinct_id_list.append(row['key'][1])

    #create dataframe 
    data = {'email': email_list, 'mixpanel_distinct_id': distinct_id_list}
    df_creators_mixpanel = pd.DataFrame(data=data)

    #remove creators with missing information
    df_creators_mixpanel = df_creators_mixpanel.dropna()
    
    return df_creators_mixpanel




def main():
    
    """
    
    Update Mixpanel profiles with HubSpot specific company and contact data.
    Update HubSpot profiles with Mixpanel specific contact data.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        
    """
    
    
    #send initial email
    send_email('Initiate HubSpot-Mixpanel Update')
    
    #pull app creator information from Mixpanel
    df_creators_mixpanel = pull_creators()

    #email list
    email_list = df_creators_mixpanel.email.tolist()
    mixpanel_distinct_id_list = df_creators_mixpanel.mixpanel_distinct_id.tolist()

    #initialize list to record errors
    errors = []

    #update Mixpanel profiles with HubSpot data and vice versa
    for i in tqdm(range(len(email_list))):

        #get user ID and email
        user_email = email_list[i]
        mixpanel_distinct_id = mixpanel_distinct_id_list[i]

        #update Mixpanel profile
        try: 
            update_fields(user_email,mixpanel_distinct_id)

        except Exception as e: 
            print(e)
            print(user_email)
            errors.append(user_email)  


    #generate dataframes containing contacts who were caused an error
    data = {'emails': errors}
    df_errors = pd.DataFrame(data)
    
    #save dataframe as a csv
    file_path = '/home/phuong/automated_jobs/hubspot_mixpanel/email_errors.csv'
    df_errors.to_csv(file_path, index=False)
    
    #send completed email
    send_email('HubSpot-Mixpanel Update Completed', file_path)
    
    
    return 


#execute update
if __name__ == '__main__':
    main()
    



