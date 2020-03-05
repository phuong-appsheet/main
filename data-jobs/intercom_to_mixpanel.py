#import libraries
from mixpanel_jql import JQL, Reducer, Events
import pandas as pd
import numpy as np
from mixpanel_jql import JQL, Reducer, Events, People
import requests
from mixpanel import Mixpanel
import clearbit
import json 
import requests
import os
from pt_utils.utils import send_email
import time

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY'))
clearbit.key = os.environ.get('CLEARBIT_KEY_APPSHEET')
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')
api_user_secret = os.environ.get('MIXPANEL_USER_KEY')
hubspot_key = os.environ.get('HUBSPOT_KEY')
intercom_key = os.environ.get('INTERCOM_KEY')





def pull_intercom_users():
    
    """
    
    Pull Intercom app creator profiles, associated tags, app use case, and company urls.
    

    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    intercom_key: str
        Client secret used to make calls to Intercom.
        
        
    Returns
    ----------
    
    df_user_tags: dataframe
        Dataframe contains app creator info and associated tags.
    
    df_user_company_url: dataframe
        Dataframe contains app creator info and company url.
    
    df_user_use_case: dataframe
        Dataframe contains app creator info and use case.
           
    """
    
    #initialize lists to store emails and tags
    user_email_list = []
    user_tags_list = []
    
    #initialize lists to store emails and company_urls
    company_url_email_list = []
    company_url_list = []

    #initialize lists to store emails and use cases
    use_case_email_list = []
    use_case_list = []

    #generate first request
    response = requests.get(
        'https://api.intercom.io/users/scroll',
        params={'q': 'requests+language:python'},
        headers={'Accept': 'application/json','Authorization': 'Bearer '+ intercom_key},
    )

    #process results to record emails, tags, company urls, and use cases
    for user in response.json()['users']:
        user_tags = user['tags']['tags']
        if user_tags != []:
            user_tags_list.append(user_tags)
            user_email_list.append(user['email'])
        if 'custom_attributes'in user.keys():
            if 'company_url' in user['custom_attributes'].keys():
                company_url_email_list.append(user['email'])
                company_url_list.append(user['custom_attributes']['company_url'])
            if 'Use_Case' in user['custom_attributes'].keys():
                use_case_email_list.append(user['email'])
                use_case_list.append(user['custom_attributes']['Use_Case'])
 
    #extract scroll parameter
    scroll_param = response.json()['scroll_param']

    
    #use loop to make more requests
    loop = True
    while loop:

        try:
            
            #generate request
            response = requests.get(
                'https://api.intercom.io/users/scroll',
                params={'q': 'requests+language:python','scroll_param': scroll_param},
                headers={'Accept': 'application/json','Authorization': 'Bearer '+ intercom_key},
            )

            #process results to record emails, tags, company urls, and use cases
            if 'users' in response.json().keys():
                for user in response.json()['users']:
                    user_tags = user['tags']['tags']
                    if user_tags != []:
                        user_tags_list.append(user_tags)
                        user_email_list.append(user['email'])
                    if 'custom_attributes'in user.keys():
                        if 'company_url' in user['custom_attributes'].keys():
                            company_url_email_list.append(user['email'])
                            company_url_list.append(user['custom_attributes']['company_url'])
                    if 'Use_Case' in user['custom_attributes'].keys():
                        use_case_email_list.append(user['email'])
                        use_case_list.append(user['custom_attributes']['Use_Case'])

            else:
                loop = False
        except:
            time.sleep(30)

    #generate tag dataframe
    data = {'email': user_email_list, 'user_tags': user_tags_list}
    df_user_tags = pd.DataFrame(data)

    #generate company url dataframe
    data_company_url = {'email': company_url_email_list, 'company_url': company_url_list}
    df_user_company_url = pd.DataFrame(data_company_url)
    
    #generate use case dataframe and only filter out the eligible fields
    data_Use_Case = {'email': use_case_email_list, 'Use_Case': use_case_list}
    df_user_use_case = pd.DataFrame(data_Use_Case)
    eligible_fields = ['Field Work', 'Inventory', 'Inspections / Surveys', 'Projects', 'Human Resources',
             'Education / Training', 'Sales / Marketing', 'Other']
    df_user_use_case = df_user_use_case[df_user_use_case.Use_Case.isin(eligible_fields)]

    return df_user_tags, df_user_company_url, df_user_use_case

def parse_intercom_tag_names(df_user_tags):

    """
    
    Parse tag names for each app creator.
    
    
    Parameters
    ----------
    
    df_user_tags: dataframe
        Dataframe contains app creator info and associated tags.
        
        
        
    Global Variables
    ----------
    
    
    
    Returns
    ----------
    
    dataframe
        Dataframe contains app creator email and associated tag names.
           
           
    """
    
    #intialize lists to store emails and tag names
    user_email_list = []
    user_tag_names_list_final = []

    #extract email and tag list from dataframe
    tag_list_temp = df_user_tags.user_tags.tolist()
    email_list_temp = df_user_tags.email.tolist()

    #extract tag names
    for email, user_tags_list in zip(email_list_temp, tag_list_temp):

        #extract user tag names from json list
        user_tag_names_list = []
        for tag in user_tags_list:
            user_tag_names_list.append(tag['name'])

        #append email and tag name list to final lists
        user_tag_names_list_final.append(user_tag_names_list)
        user_email_list.append(email)

    #generate dataframe
    data = {'email':user_email_list, 'user_tag_names': user_tag_names_list_final}
    df_user_tags = pd.DataFrame(data)

    return df_user_tags

def user_pull():

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
                    "e.properties.company_domain",
                    "e.properties.$distinct_id"
                ],
                accumulator=Reducer.count()
            )


    #initiate list to store emails, user IDs, and company domains
    email_list = []
    company_domain_list = []
    distinct_id_list = []
    
    #process query results
    for row in query.send():
        email_list.append(row['key'][0])
        company_domain_list.append(row['key'][1])
        distinct_id_list.append(row['key'][2])
        
    #create dataframe 
    data = {'email':email_list, 'company_domain': company_domain_list, 'distinct_id' : distinct_id_list}
    df_users = pd.DataFrame(data=data)
    
    return df_users


def update_mixpanel_users(df_users_ps):
    
    """
    
    Update app creator Intercom tag field in Mixpanel.
    
    
    Parameters
    ----------
    
    df_users_ps: dataframe
        Dataframe contains app creators info and associated tags.
        
    
    Global Variables
    ----------
    
    mp: Mixpanel object
        Mixpanel object used to make API calls to Mixpanel Creator Project.
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators info and associated tags.    
        
        
    """
    
 

    #extract user ID and tag lists
    distinct_id_list = df_users_ps.distinct_id.tolist()
    tag_list = df_users_ps.user_tag_names.tolist()
    
    #update Mixpanel app creator profiles
    for distinct_id,tags in zip(distinct_id_list,tag_list):
        
        try:
            
            #populate parameters
            params = {'intercom_tags': tags}
            
            #execute call
            mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
            
        except Exception as e: 
            print(e)
 
    return df_users_ps


def execute_intercom_tag_updates(df_user_tags, df_users):
    
    """
    
    Update intercom_tags field for Mixpanel users.
    
    
    
    Parameters
    ----------
    
    df_user_tags: dataframe
        Dataframe contains app creators email and associated tags.
        
    df_users: dataframe
        Dataframe contains app creators info.
    
    Global Variables
    ----------
        
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators info and associated tags.    
        
        
    """
    

    #parse tag names for each user
    df_user_tags = parse_intercom_tag_names(df_user_tags)

    #only keep relevant columns
    df_users = df_users[['email','distinct_id']]

    #merge Mixpanel app creator info and Intercom app creator info with associated tags
    df_users_ps = pd.merge(df_users, df_user_tags, on='email', how='left' )

    #remove any app creators who do not have any associated Intercom tags
    df_users_ps = df_users_ps.dropna()

    #update Mixpanel app creator profile
    df_users_ps = update_mixpanel_users(df_users_ps)
    
    return df_users_ps



def get_clearbit_data(company_url_list, distinct_id_list):

    """
    
    Enrich app creator profiles with Clearbit data using company urls.
    
    
    Parameters
    ----------
    
    company_url_list: list
        List of company urls.
        
    distinct_id_list: list
        List of distinct IDs.
        
        
    
    Global Variables
    ----------
    
    clearbit: Clearbit object
        Clearbit object to make Clearbit API calls.
        
    mp: Mixpanel object
        Mixpanel object to make Mixpanel API calls.
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators info who were successfully enrich with Clearbit.    
        
        
    """
    
    #initiate lists to store successfully enriched user IDs and domains.
    successful_distinct_id_list = []
    successful_domain_list = []


    #enrich Mixpanel app creator profiles with Clearbit
    for i in range(len(company_url_list)):

        #get user ID and company url
        company_url = company_url_list[i]
        distinct_id = distinct_id_list[i]

        #checking Clearbit enrichment service
        try:
            company = clearbit.Enrichment.find(domain=company_url)
        except:
            company = None

        #check if Clearbit has response
        if company != None:            
            
            #check if status is pending. Pending means that Clearbit is currently in the process of gather info on the company.
            if 'pending' not in list(company.keys()):

                #initiate dictionary to populate parameters
                params = {}

                #record successfully enriched creators
                successful_distinct_id_list.append(distinct_id)
                successful_domain_list.append(company_url)

                #check if nested company key is not None
                if company != None:
                    
                    #string-either education, government, nonprofit, private, public, or personal.
                    company_type = company['type'] 
                    if company_type != None:
                        params['company_type'] = company_type

                    #string-International headquarters phone number 
                    company_phone = company['phone'] 
                    if company_phone != None:
                        params['company_phone'] = company_phone

                    #array-Array of technology tags
                    company_tech = company['tech'] 
                    if company_tech != None:
                        params['company_tech'] = company_tech

                    #string-SRC of company logo.
                    company_logo = company['logo'] 
                    if company_logo != None:
                        params['company_logo'] = company_logo

                    #string-Twitter Bio
                    company_twitter_bio = company['twitter']['bio'] 
                    if company_twitter_bio != None:
                        params['company_twitter_bio'] = company_twitter_bio

                    #string-Company’s Linkedin URL
                    company_linkedin_handle = company['linkedin']['handle'] 
                    if company_linkedin_handle != None:
                        params['company_linkedin_handle'] = company_linkedin_handle

                    #string-Twitter screen name
                    company_twitter_handle = company['twitter']['handle'] 
                    if company_twitter_handle != None:
                        params['company_twitter_handle'] = company_twitter_handle

                    #integer-Annual Revenue (public companies only)
                    company_metrics_annualRevenue = company['metrics']['annualRevenue'] 
                    if company_metrics_annualRevenue != None:
                        params['company_metrics_annualRevenue'] = company_metrics_annualRevenue

                    #integer-Amount of employees
                    company_metrics_employees = company['metrics']['employees'] 
                    if company_metrics_employees != None:
                        params['company_metrics_employees'] = company_metrics_employees


                    #float-Headquarters latitude
                    company_geo_lat = company['geo']['lat'] 
                    if company_geo_lat != None:
                        params['company_geo_lat'] = company_geo_lat


                    #float-Headquarters longitude
                    company_geo_lng = company['geo']['lng'] 
                    if company_geo_lng != None:
                        params['company_geo_lng'] = company_geo_lng


                    #string-Headquarters street number
                    company_geo_streetNumber = company['geo']['streetNumber'] 
                    if company_geo_streetNumber != None:
                        params['company_geo_streetNumber'] = company_geo_streetNumber


                    #string-Headquarters street name
                    company_geo_streetName = company['geo']['streetName'] 
                    if company_geo_streetName != None:
                        params['company_geo_streetName'] = company_geo_streetName


                    #string-Headquarters suite number
                    company_geo_subPremise = company['geo']['subPremise'] 
                    if company_geo_subPremise != None:
                        params['company_geo_subPremise'] = company_geo_subPremise


                    #string-Headquarters city name
                    company_geo_city = company['geo']['city'] 
                    if company_geo_city != None:
                        params['company_geo_city'] = company_geo_city


                    #string-Headquarters state name
                    company_geo_state = company['geo']['state'] 
                    if company_geo_state != None:
                        params['company_geo_state'] = company_geo_state

                    #string-Headquarters two character state code
                    company_geo_stateCode = company['geo']['stateCode'] 
                    if company_geo_stateCode != None: 
                        params['company_geo_stateCode'] = company_geo_stateCode

                    #string-Headquarters postal/zip code
                    company_geo_postalCode = company['geo']['postalCode'] 
                    if company_geo_postalCode != None:
                        params['company_geo_postalCode'] = company_geo_postalCode


                    #string-Headquarters country name
                    company_geo_country = company['geo']['country'] 
                    if company_geo_country != None:
                        params['company_geo_country'] = company_geo_country

                    #string-Broad sector 
                    company_category_sector = company['category']['sector'] 
                    if company_category_sector != None:
                        params['company_category_sector'] = company_category_sector

                    #string-Industry group (see a complete list)
                    company_category_industryGroup = company['category']['industryGroup'] 
                    if company_category_industryGroup != None:
                        params['company_category_industryGroup'] = company_category_industryGroup


                    #string-Industry (see a complete list)
                    company_category_industry = company['category']['industry'] 
                    if company_category_industry != None:
                        params['company_category_industry'] = company_category_industry

    
                    #string-Sub industry (see a complete list)
                    company_category_subIndustry = company['category']['subIndustry'] 
                    if company_category_subIndustry != None:
                        params['company_category_subIndustry'] = company_category_subIndustry


                    #string-Two digit category SIC code
                    company_category_sicCode = company['category']['sicCode'] 
                    if company_category_sicCode != None:
                        params['company_category_sicCode'] = company_category_sicCode

                    #string-Two digit category NAICS code
                    company_category_naicsCode = company['category']['naicsCode'] 
                    if company_category_naicsCode != None:
                        params['company_category_naicsCode'] = company_category_naicsCode

                    #string-Description of the company
                    company_description = company['description'] 
                    if company_description != None:
                        params['company_description'] = company_description

                    #integer-Year company was founded
                    company_foundedYear = company['foundedYear'] 
                    if company_foundedYear != None:
                        params['company_foundedYear'] = company_foundedYear

                    #string-Address of company
                    company_location = company['location'] 
                    if company_location != None:
                        params['company_location'] = company_location


                    #string-The timezone for the company’s location
                    company_timeZone = company['timeZone'] 
                    if company_timeZone != None:
                        params['company_timeZone'] = company_timeZone

                    #string-Domain of company’s website
                    company_domain = company['domain'] 
                    if company_domain != None:
                        params['company_domain'] = company_domain

                    #array-List of domains also used by the company
                    company_domainAliases = company['domainAliases'] 
                    if company_domainAliases != None:
                        params['company_domainAliases'] = str(company_domainAliases)


                    #string-Name of company
                    company_name = company['name'] 
                    if company_name != None:
                        params['company_name'] = company_name

                
                #execute update
                mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
        
    #generate dataframe
    data = {'successful_distinct_id': successful_distinct_id_list, 'successful_domain': successful_domain_list}
    df_clearbit_enriched = pd.DataFrame(data)
    
    return df_clearbit_enriched
            
    
def execute_intercom_company_updates(df_company_url, df_users): 
    
    """
    
    Update app creator Mixpanel profiles with company data from Clearbit.
    
    
    Parameters
    ----------
    
    df_user_company_url: dataframe
        Dataframe contains app creator info and company url.
    
        
    df_users: dataframe
        Dataframe contains app creator info.
    
    
    
    Global Variables
    ----------
    
    
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators who were successfully enriched with Clearbit.    
        
        
    """
    
    
    #merge Intercom company url and app creator Mixpanel profiles
    df_users_intercom_merge = pd.merge(df_users, df_company_url, on='email', how='left')

    #remove app creators without company url
    df_users_intercom_merge = df_users_intercom_merge[~df_users_intercom_merge.company_url.isnull()]

    #keep app creators who currently do not have a company domain in Mixpanel. Not override existing company data in Mixpanel.
    df_users_intercom_merge = df_users_intercom_merge[df_users_intercom_merge.company_domain.isnull()]

    #extract company data from Clearbit and update Mixpanel app creator profiles
    company_urls_final = df_users_intercom_merge['company_url'].tolist()
    distinct_id_final = df_users_intercom_merge['distinct_id'].tolist()
    df_clearbit_enriched_company_url_push = get_clearbit_data(company_urls_final, distinct_id_final)
    
    return df_clearbit_enriched_company_url_push


def execute_intercom_use_case_updates(df_user_use_case, df_users):
    
    """
    
    Update app creator Mixpanel profiles with use case.
    
    
    Parameters
    ----------
    
    df_user_use_case: dataframe
        Dataframe contains app creator info and use case.
        
    df_users: dataframe
        Dataframe contains app creator info.
    
    
    
    Global Variables
    ----------
    
    mp: Mixpanel object
        Mixpanel object used to make Mixpanel API calls
        
    Returns
    ----------
    
    dataframe
        Dataframe contains app creators info and use case.
        
        
    """
    
    
    #merge Intercom use case and app creator profile in Mixpanel
    df_users_intercom_merge = pd.merge(df_users, df_user_use_case, on='email', how='left')

    #remove contacts without use case
    df_users_intercom_merge = df_users_intercom_merge[~df_users_intercom_merge.Use_Case.isnull()]

    #extract user IDs and use cases to list
    Use_Case_final = df_users_intercom_merge['Use_Case'].tolist()
    distinct_id_final = df_users_intercom_merge['distinct_id'].tolist()
    
    #update Mixpanel app creator profiles with use cases
    for distinct_id,use_case in zip(distinct_id_final, Use_Case_final):
        
        #populate params
        params = {'use_case': use_case}
        
        #execute update
        mp.people_set(distinct_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
         
    
    return df_users_intercom_merge


def main():
    
    """
    Update Mixpanel app creator profiles with company data, use case, and tags from Intercom.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
      
    #send initial email
    send_email('Initiate Intercom-Mixpanel Update')
    
    #pull app creator profiles in Mixpanel
    df_users = user_pull()

    #pull app creator Intercom tags, company url, and use case
    df_user_tags, df_user_company_url, df_user_use_case = pull_intercom_users()

    # execute updates
    df_tags_pushed = execute_intercom_tag_updates(df_user_tags, df_users)
    df_clearbit_enriched_company_url_push = execute_intercom_company_updates(df_user_company_url, df_users)
    df_use_case_push = execute_intercom_use_case_updates(df_user_use_case, df_users)



    #send complete email
    send_email('Intercom-Mixpanel Update Completed')
    
    return df_use_case_push





#execute updates
if __name__ == '__main__':
    main()


