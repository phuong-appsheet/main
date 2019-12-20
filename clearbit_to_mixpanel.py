#import libraries
from datetime import datetime, date, timedelta
from mixpanel_jql import JQL, Reducer, Events, People
import pandas as pd
import numpy as np
from mixpanel import Mixpanel
import clearbit
import os
from pt_utils.utils import send_email

#import credentials object
mp = Mixpanel(os.environ.get('MIXPANEL_KEY')) 
clearbit.key = os.environ.get('CLEARBIT_KEY_APPSHEET')
api_creator_secret = os.getenv('MIXPANEL_CREATOR_KEY')

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


def get_clearbit_data(email_list, user_id_list):

    """
    Get Clearbit data and update Mixpanel contact.
    
    
    Parameters
    ----------
    
    email_list : list
        List of creator emails.
        
    user_id_list : list
        List of user IDs.
        
        
    Global Variables
    ----------
    
    clearbit: Clearbit client.
        Use to make API calls to Clearbit to enrich creator profiles.
        
    
    Returns
    ----------
    
    None
        Function will update Mixpanel panel profiles with Clearbit company and contact information.
    
    """
    
    #store user_ids and emails that were successfully enriched
    successful_user_id_list = []
    successful_email_list = []

    
    #loop through user profiles to enrich
    for i in range(len(email_list)):

        #get user ID and email
        user_email = email_list[i]
        user_id = user_id_list[i]

        #check if clearbit has info on contact
        try:
            clearbit_response = clearbit.Enrichment.find(email=user_email)
        except:
            clearbit_response = None

        #check if clearbit has info on contact
        if clearbit_response != None:

            #check if status is pending. Pending means that Clearbit is still in the process of collect data for the contact.
            if 'pending' not in list(clearbit_response.keys()):

                #initialize parameters to update in Mixpanel
                params = {}

                #record succesfully added users
                successful_user_id_list.append(user_id)
                successful_email_list.append(user_email)

                #extract Clearbit company info
                if clearbit_response['company'] != None:
                    company_domain=clearbit_response['company']['domain']

                    if company_domain != None:

                        #company attributes
                        company = clearbit.Company.find(domain=company_domain)

                        if company != None:
                            company_type = company['type'] #string-either education, government, nonprofit, private, public, or personal.
                            if company_type != None:
                                params['company_type'] = company_type


                            company_phone = company['phone'] #string-International headquarters phone number 
                            if company_phone != None:
                                params['company_phone'] = company_phone


                            company_tech = company['tech'] #array-Array of technology tags
                            if company_tech != None:
                                params['company_tech'] = company_tech

                            company_logo = company['logo'] #string-SRC of company logo. Do we really want this?
                            if company_logo != None:
                                params['company_logo'] = company_logo

                            company_twitter_bio = company['twitter']['bio'] #string-Twitter Bio
                            if company_twitter_bio != None:
                                params['company_twitter_bio'] = company_twitter_bio

                            company_linkedin_handle = company['linkedin']['handle'] #string-Company’s Linkedin URL
                            if company_linkedin_handle != None:
                                params['company_linkedin_handle'] = company_linkedin_handle

                            company_twitter_handle = company['twitter']['handle'] #string-Twitter screen name
                            if company_twitter_handle != None:
                                params['company_twitter_handle'] = company_twitter_handle

                            company_metrics_annualRevenue = company['metrics']['annualRevenue'] #integer-Annual Revenue (public companies only)
                            if company_metrics_annualRevenue != None:
                                params['company_metrics_annualRevenue'] = company_metrics_annualRevenue

                            company_metrics_employees = company['metrics']['employees'] #integer-Amount of employees
                            if company_metrics_employees != None:
                                params['company_metrics_employees'] = company_metrics_employees


                            company_geo_lat = company['geo']['lat'] #float-Headquarters latitude
                            if company_geo_lat != None:
                                params['company_geo_lat'] = company_geo_lat


                            company_geo_lng = company['geo']['lng'] #float-Headquarters longitude
                            if company_geo_lng != None:
                                params['company_geo_lng'] = company_geo_lng


                            company_geo_streetNumber = company['geo']['streetNumber'] #string-Headquarters street number
                            if company_geo_streetNumber != None:
                                params['company_geo_streetNumber'] = company_geo_streetNumber


                            company_geo_streetName = company['geo']['streetName'] #string-Headquarters street name
                            if company_geo_streetName != None:
                                params['company_geo_streetName'] = company_geo_streetName


                            company_geo_subPremise = company['geo']['subPremise'] #string-Headquarters suite number
                            if company_geo_subPremise != None:
                                params['company_geo_subPremise'] = company_geo_subPremise


                            company_geo_city = company['geo']['city'] #string-Headquarters city name
                            if company_geo_city != None:
                                params['company_geo_city'] = company_geo_city


                            company_geo_state = company['geo']['state'] #string-Headquarters state name
                            if company_geo_state != None:
                                params['company_geo_state'] = company_geo_state

                            company_geo_stateCode = company['geo']['stateCode'] #string-Headquarters two character state code
                            if company_geo_stateCode != None:
                                params['company_geo_stateCode'] = company_geo_stateCode

                            company_geo_postalCode = company['geo']['postalCode'] #string-Headquarters postal/zip code
                            if company_geo_postalCode != None:
                                params['company_geo_postalCode'] = company_geo_postalCode

                            company_geo_country = company['geo']['country'] #string-Headquarters country name
                            if company_geo_country != None:
                                params['company_geo_country'] = company_geo_country

                            company_category_sector = company['category']['sector'] #string-Broad sector (see a complete list)
                            if company_category_sector != None:
                                params['company_category_sector'] = company_category_sector

                            company_category_industryGroup = company['category']['industryGroup'] #string-Industry group (see a complete list)
                            if company_category_industryGroup != None:
                                params['company_category_industryGroup'] = company_category_industryGroup


                            company_category_industry = company['category']['industry'] #string-Industry (see a complete list)
                            if company_category_industry != None:
                                params['company_category_industry'] = company_category_industry


                            company_category_subIndustry = company['category']['subIndustry'] #string-Sub industry (see a complete list)
                            if company_category_subIndustry != None:
                                params['company_category_subIndustry'] = company_category_subIndustry


                            company_category_sicCode = company['category']['sicCode'] #string-Two digit category SIC code
                            if company_category_sicCode != None:
                                params['company_category_sicCode'] = company_category_sicCode

                            company_category_naicsCode = company['category']['naicsCode'] #string-Two digit category NAICS code
                            if company_category_naicsCode != None:
                                params['company_category_naicsCode'] = company_category_naicsCode

                            company_description = company['description'] #string-Description of the company
                            if company_description != None:
                                params['company_description'] = company_description

                            company_foundedYear = company['foundedYear'] #integer-Year company was founded
                            if company_foundedYear != None:
                                params['company_foundedYear'] = company_foundedYear

                            company_location = company['location'] #string-Address of company
                            if company_location != None:
                                params['company_location'] = company_location


                            company_timeZone = company['timeZone'] #string-The timezone for the company’s location
                            if company_timeZone != None:
                                params['company_timeZone'] = company_timeZone

                            company_domain = company['domain'] #string-Domain of company’s website
                            if company_domain != None:
                                params['company_domain'] = company_domain

                            company_domainAliases = company['domainAliases'] #array-List of domains also used by the company
                            if company_domainAliases != None:
                                params['company_domainAliases'] = str(company_domainAliases)

                            company_name = company['name'] #string-Name of company
                            if company_name != None:
                                params['company_name'] = company_name

                            print(user_email)
                            
                #get contact attributes
                contact = clearbit_response['person']

                #check if contact attributes exist
                if contact!=None:
 
                    #person attributes
                    contact_linkedin_handle = contact['linkedin']['handle'] #string-LinkedIn url (i.e. /pub/alex-maccaw/78/929/ab5)
                    if contact_linkedin_handle != None:
                            params['contact_linkedin_handle'] = contact_linkedin_handle


                    contact_twitter_handle = contact['twitter']['handle'] #string-Twitter screen name
                    if contact_twitter_handle != None:
                        params['contact_twitter_handle'] = contact_twitter_handle


                    contact_employment_seniority = contact['employment']['seniority'] #string-Seniority at Company
                    if contact_employment_seniority != None:
                        params['contact_employment_seniority'] = contact_employment_seniority


                    contact_employment_name = contact['employment']['name'] #string-Company name
                    if contact_employment_name != None:
                        params['contact_employment_name'] = contact_employment_name



                    contact_employment_title = contact['employment']['title'] #string-Title at Company
                    if contact_employment_title != None:
                        params['contact_employment_title'] = contact_employment_title



                    contact_employment_role = contact['employment']['role'] #string-Role at Company
                    if contact_employment_role != None:
                        params['contact_employment_role'] = contact_employment_role

                    contact_geo_city = contact['geo']['city'] #string-Normalized city based on location
                    if contact_geo_city != None:
                        params['contact_geo_city'] = contact_geo_city


                    contact_geo_state = contact['geo']['state'] #string-Normalized state based on location
                    if contact_geo_state != None:
                        params['contact_geo_state'] = contact_geo_state


                    contact_geo_country = contact['geo']['country'] #string-Normalized two letter country code based on location
                    if contact_geo_country != None:
                        params['contact_geo_country'] = contact_geo_country

                    contact_location = contact['location'] #string-The most accurate location we have
                    if contact_location != None:
                        params['contact_location'] = contact_location

                    contact_name_givenName = contact['name']['givenName'] #string-First name of person (if found)
                    if contact_name_givenName != None:
                        params['contact_first_name'] = contact_name_givenName

                    contact_name_familyName = contact['name']['familyName'] #stringLast name of person (if found)
                    if contact_name_familyName != None:
                        params['contact_last_name'] = contact_name_familyName
                    print(user_email)
                mp.people_set(user_id, params, meta = {'$ignore_time' : 'true', '$ip' : 0})
            
            
           
    
def main():
    
    """
    
    Update Mixpanel profiles with Clearbit company and contact data.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None
        
    """
    
    
    #send initial email
    send_email("Initiate Clearbit Enrichment")
    
    #get all users mixpanel and new users from yesterday
    yesterday = date.today() - timedelta(days=1)
    df_new_users = get_new_signup(yesterday)
    df_new_users.drop_duplicates('email', keep='first')

    #remove contacts with @gmail.com, hotmail.com, yahoo.com, and outlook.com since Clearbit wouldn't be able to enrich them
    df_new_users = df_new_users[~df_new_users['email'].str.contains("gmail.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("hotmail.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("yahoo.com")]
    df_new_users = df_new_users[~df_new_users['email'].str.contains("outlook.com")]
    df_new_users = df_new_users[df_new_users['user_id']!=0]

    #get user email and ID
    email_list = df_new_users.email.tolist()
    user_id_list = df_new_users.user_id.tolist()

    #execute data migration
    get_clearbit_data(email_list, user_id_list)
    
    #send email when completed
    send_email("Clearbit Enrichment Completed")

    return


#execute update
if __name__ == "__main__":
    main()


