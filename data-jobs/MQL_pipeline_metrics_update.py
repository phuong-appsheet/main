import requests
import json
import urllib
import os
import time
import pandas as pd
from datetime import date, datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import gspread  
from pt_utils.utils import send_email
from pt_utils import mandrillpt

hubspot_key = os.environ.get('HUBSPOT_KEY') 
google_drive_client_secrets = os.path.expanduser(os.environ.get('GD_CLIENT_SECRETS'))





def pull_contact_forms():
    
    """
    Pull contact forms information submitted in Q2 2020.


    Parameters
    ----------


    Global Variables
    ----------

    hubspot_key: str
        Key used to access HubSpot API.
        

    Returns
    ----------

    df_contact_forms: dataframe
        Dataframe contains contact email and form submit date.


    """
    
    #request HubSpot forms service
    url= 'https://api.hubapi.com/form-integrations/v1/submissions/forms/2cbc2d07-3e1c-4221-abc7-7ccf4c182e78?hapikey=' + hubspot_key + '&limit=50'
    response = requests.get(url=url).json()

    #initiate list to store emails and submit dates
    email_list = []
    submittedat_list = []

    #loop through form responses
    for form in response['results']:

        #extract emails and submit timestamps
        for value in form['values']:
            if value['name'] == 'email':
                
                #extract email
                email = value['value']
                email_list.append(email)  

                #extract submitted timestamp
                timestamp = ''.join(str(form['submittedAt']).split())[:-3]
                submitted_at = (datetime.utcfromtimestamp(int(timestamp)) - timedelta(hours=7))  #converting time stamp to datetime pst
                submittedat_list.append(submitted_at)

    #store pagination parameter to keep looping through form service
    pagination_parameter = response['paging']['next']['after']

    #use pagination to loop through all existing forms
    loop = True
    while loop:

        try:

            #request HubSpot forms service
            url= 'https://api.hubapi.com/form-integrations/v1/submissions/forms/2cbc2d07-3e1c-4221-abc7-7ccf4c182e78?hapikey=' + hubspot_key + '&limit=50&after=' + pagination_parameter 
            response = requests.get(url=url).json()

            #check if there are forms to loop through
            if len(response['results'])>0:

                #loop through form responses
                for form in response['results']:

                    #extract emails and submit dates
                    for value in form['values']:
                        if value['name'] == 'email':
                            
                            #extract email
                            email = value['value']
                            email_list.append(email)

                            #extract submitted timestamp
                            timestamp = ''.join(str(form['submittedAt']).split())[:-3]
                            submitted_at = (datetime.utcfromtimestamp(int(timestamp)) - timedelta(hours=7))  #converting time stamp to datetime pst
                            submittedat_list.append(submitted_at)


                #store pagination parameter to keep looping through form service
                pagination_parameter = response['paging']['next']['after']

            else:
                loop = False

        except Exception as e: 
            print(e)
            time.sleep(30)


    #create dataframe with emails and submit dates
    data = {'email': email_list, 'submittedat': submittedat_list}
    df_contact_forms = pd.DataFrame(data).sort_values('submittedat', ascending=False)

    #sort by submit date and remove duplicate emails
    df_contact_forms = df_contact_forms.sort_values('submittedat', ascending=False)
    df_contact_forms = df_contact_forms.drop_duplicates('email', keep='first')

    #change timestamp value to date value
    df_contact_forms['submittedat'] = df_contact_forms['submittedat'].dt.date

    #only look at forms submitted this quarter
    df_contact_forms = df_contact_forms[df_contact_forms.submittedat >= datetime.strptime('2020-04-01', '%Y-%m-%d').date()]

    return df_contact_forms




def pull_hubspot_contacts(df_contact_forms):
    
    """
    Pull HubSpot contact and associated deals information.


    Parameters
    ----------

    df_contact_forms: dataframe
        Dataframe contains email and submit date of contacts who submitted a form in Q2 2020.
        

    Global Variables
    ----------
    
    hubspot_key: str
        Key used to access HubSpot API.


    Returns
    ----------

    df_associated_deals: dataframe
        Dataframe contains contact email, existing contact indicator (prior to form), deal open date, and deal open indicator.


    """
    
    #initiate lists to store number of deals, email, deal open time, and existing contact indicator
    num_deals_list_final = []
    user_email_list_final = []
    deal_open_timestamp_list_final = []
    existing_contact_list_final = []

    #get list of emails who submitted a form
    user_email_list = df_contact_forms.email.tolist()

    #loop through list of emails and make calls to HubSpot API to extract contact information
    i=0
    while i < len(user_email_list):

        #extract email
        user_email = user_email_list[i]

        try:

            #find contact in HubSpot
            contact_url= 'https://api.hubapi.com/contacts/v1/contact/email/'+ user_email + '/profile?hapikey=' + hubspot_key
            contact_response = requests.get(url=contact_url).json()

            #initiate number of associated deals, deal timestamp, and existing contact indicator
            num_deals = 0
            deal_open_timestamp = None
            existing_contact = 0

            #check if contact exists in HubSpot
            if 'properties' in contact_response.keys(): 

                #get number of deals associated to contact
                if 'num_associated_deals' in contact_response['properties'].keys():

                    #extract number of deals
                    if contact_response['properties']['num_associated_deals']['value'] != '':
                        num_deals_temp = int(contact_response['properties']['num_associated_deals']['value'])

                        #extract time of deal open
                        if num_deals_temp > 0: 
                            timestamp = contact_response['properties']['num_associated_deals']['versions'][0]['timestamp']
                            timestamp = ''.join(str(timestamp).split())[:-3]
                            deal_open_timestamp_temp = datetime.utcfromtimestamp(int(timestamp)) - timedelta(hours=7)  #converting time stamp to datetime pst

                            #only register deals that were opened this quarter
                            if deal_open_timestamp_temp >= datetime.strptime('2020-04-01', '%Y-%m-%d'): 
                                deal_open_timestamp = deal_open_timestamp_temp
                                num_deals = num_deals_temp

                #check if existing account field is available
                if 'do_you_have_an_existing_appsheet_account_' in contact_response['properties'].keys(): 
                    
                    #indicate if contact has an existing account
                    if contact_response['properties']['do_you_have_an_existing_appsheet_account_']['value'] == 'Yes': 
                        existing_contact = 1

                #log contact information
                num_deals_list_final.append(num_deals)
                user_email_list_final.append(user_email)
                deal_open_timestamp_list_final.append(deal_open_timestamp)
                existing_contact_list_final.append(existing_contact)
                i += 1

            #check if rate limit was reached
            elif 'errorType' in contact_response.keys():
                if contact_response['errorType'] == 'RATE_LIMIT': 
                    time.sleep(10)

            #if contact could not be found in HubSpot, still log them as having no associated deal
            else: 
                i += 1
                num_deals_list_final.append(num_deals)
                user_email_list_final.append(user_email)
                deal_open_timestamp_list_final.append(deal_open_timestamp)
                existing_contact_list_final.append(existing_contact)

        except Exception as e:
            print(e)
            time.sleep(10)

    #create dataframe with emails, existing contact indicator, number of associated deals, and first deal open date
    data = {'email': user_email_list_final, 'existing_contact': existing_contact_list_final, 'num_deals': num_deals_list_final, 'deal_open_date': deal_open_timestamp_list_final}
    df_associated_deals = pd.DataFrame(data)

    #indicates whether the contact has a deal open
    df_associated_deals.loc[df_associated_deals.num_deals > 0, 'associated_deals'] = 1
    df_associated_deals.loc[df_associated_deals.num_deals == 0, 'associated_deals'] = 0

    #convert datetime to date for merging purposes
    df_associated_deals['deal_open_date'] = df_associated_deals['deal_open_date'].dt.date
    df_associated_deals = df_associated_deals[['email', 'existing_contact', 'deal_open_date', 'associated_deals']]
    
    return df_associated_deals


def generate_data_range_df(date_beginning_quarter):
    
    """
    Create dataframe of Q2 dates for merging purposes. This will allow us to log dates that had no data for our metrics.


    Parameters
    ----------

    date_beginning_quarter: datetime
        Date representing the beginning of Q2 2020 (4/1/2020).
        

    Global Variables
    ----------
    


    Returns
    ----------

    df_date_range: dataframe
        Dataframe contains Q2 2020 dates.


    """
    
    #generate date range dataframe
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    numdays = (yesterday -  date_beginning_quarter).days + 1
    date_list = [(yesterday - timedelta(days=x)).date() for x in range(numdays)]
    data = {'date': date_list}
    df_date_range = pd.DataFrame(data).sort_values('date', ascending=True)
    
    return df_date_range


def process_data(df_date_range, df_contact_forms, df_hubspot_deals, df_BAP_GCP_hubspot_deals): 
    
    """
    Create dataframe containing quarter-to-date aggregated sums of the metrics.


    Parameters
    ----------

    df_date_range: dataframe
        Dataframe contains Q2 2020 dates.
    
    df_contact_forms: dataframe
        Dataframe contains contact email and form submit date.
    
    df_hubspot_deals: dataframe 
        Dataframe contains contact email, existing contact indicator (prior to form), deal open date, and deal open indicator.
    
    df_BAP_GCP_hubspot_deals
        Dataframe contains dates and number of deals coming from BAP/GCP reps.
        

    Global Variables
    ----------
    

    Returns
    ----------

    df_final_cumsum: dataframe
        Dataframe contains quarter-to-date aggregated sums of the metrics.


    """
    
    #separate those with/without existing accounts
    df_contact_forms_existing_account = df_contact_forms[df_contact_forms.existing_contact==1]
    df_contact_forms_no_existing_account = df_contact_forms[df_contact_forms.existing_contact==0]

    #aggregate count of submit forms and deals open
    df_contact_forms_existing_account_form = df_contact_forms_existing_account.groupby('submittedat').email.count().reset_index()
    df_contact_forms_no_existing_account_form = df_contact_forms_no_existing_account.groupby('submittedat').email.count().reset_index()
    df_contact_forms_existing_account_deal_open = df_contact_forms_existing_account.groupby('deal_open_date').email.count().reset_index()
    df_contact_forms_no_existing_account_deal_open = df_contact_forms_no_existing_account.groupby('deal_open_date').email.count().reset_index()

    #rename date columns to merge data
    df_contact_forms_no_existing_account_form = df_contact_forms_no_existing_account_form.rename(columns={'submittedat': 'date', 'email': 'forms_no_existing_account'})
    df_contact_forms_existing_account_form = df_contact_forms_existing_account_form.rename(columns={'submittedat': 'date', 'email': 'forms_existing_account'})
    df_contact_forms_no_existing_account_deal_open = df_contact_forms_no_existing_account_deal_open.rename(columns={'deal_open_date': 'date', 'email': 'deal_open_no_existing_account'})
    df_contact_forms_existing_account_deal_open= df_contact_forms_existing_account_deal_open.rename(columns={'deal_open_date': 'date', 'email': 'deal_open_existing_account'})

    #merge data
    df_final = pd.merge(df_date_range, df_contact_forms_no_existing_account_form, on='date', how='left' )
    df_final = pd.merge(df_final, df_contact_forms_existing_account_form, on='date', how='left' )
    df_final = pd.merge(df_final, df_contact_forms_no_existing_account_deal_open, on='date', how='left' )
    df_final = pd.merge(df_final, df_contact_forms_existing_account_deal_open, on='date', how='left' )
    df_final = pd.merge(df_final, df_hubspot_deals, on='date', how='left' )
    df_final = pd.merge(df_final, df_BAP_GCP_hubspot_deals, on='date', how='left' )
    
    
    #fill columns null values with 0s
    df_final['deal_open_no_existing_account'] = df_final.deal_open_no_existing_account.fillna(0)
    df_final['deal_open_existing_account'] = df_final.deal_open_existing_account.fillna(0)
    df_final['forms_existing_account'] = df_final.forms_existing_account.fillna(0)
    df_final['forms_no_existing_account'] = df_final.forms_no_existing_account.fillna(0)
    df_final['deal_value'] = df_final.deal_value.fillna(0)
    df_final['deal_BAP_GCP'] = df_final.deal_BAP_GCP.fillna(0)

    #take cumulative sums of the metrics
    df_final_cumsum = df_final[['forms_no_existing_account', 'forms_existing_account', 'deal_open_no_existing_account', 'deal_open_existing_account', 'deal_value', 'deal_BAP_GCP']].cumsum(axis=0)
    df_final_cumsum['date'] = df_final['date']
    df_final_cumsum = df_final_cumsum[['date', 'forms_no_existing_account', 'forms_existing_account', 'deal_open_no_existing_account', 'deal_open_existing_account', 'deal_value', 'deal_BAP_GCP']]

    return df_final_cumsum






def auth_google_services():
    
    """
    Authenticate to Google Drive.
    
    
    Parameters
    ----------
    
        
    Global Variables
    ----------
    
    google_drive_client_secrets: str
        Client secret used to make calls to Google Drive API.
        
    Returns
    ----------
    
    main_sheet: Google Sheet Object. Resource: https://medium.com/@denisluiz/python-with-google-sheets-service-account-step-by-step-8f74c26ed28e
        
    """

    # use credentials to create a client to interact with the Google Drive API
    scope = ['https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(google_drive_client_secrets, scope) #google_drive_client_secrets
    client = gspread.authorize(creds)

    # accessing Google Sheets
    main_sheet = client.open("MQL_pipeline_metrics_dashboard").get_worksheet(0)

    return main_sheet


def update_google_sheet(df_final_cumsum, main_sheet):
    
    """
    
    Update Google Sheet with new data.
    
    Parameters
    ----------
    
    df_final_cumsum: dataframe
        Dataframe with referral signup metric.
    
    main_sheet: Google Sheet object
        Use to access to access referral signup metric Google Sheet.
        
        
    Global Variables
    ----------
    
        
    Returns
    ----------
    
    None
        Function updates MQL pipeline dashboard.
           
    """
    
    #extract data to lists
    forms_no_existing_account_list = df_final_cumsum.forms_no_existing_account.tolist()
    forms_existing_account_list = df_final_cumsum.forms_existing_account.tolist()
    deal_open_no_existing_account_list = df_final_cumsum.deal_open_no_existing_account.tolist()
    deal_open_existing_account_list = df_final_cumsum.deal_open_existing_account.tolist()
    deal_value_list = df_final_cumsum.deal_value.tolist()
    deal_BAP_GCP_list = df_final_cumsum.deal_BAP_GCP.tolist()
    date_list = df_final_cumsum.date.tolist()

    #make updates to Google Sheet
    i = 0 
    while i in range(len(date_list)):
        index = i + 2 #reset index for page
        print(i)
        try:
            #update each metric
            main_sheet.update_cell(index,2,forms_existing_account_list[i])
            main_sheet.update_cell(index,3,forms_no_existing_account_list[i])
            main_sheet.update_cell(index,4,deal_open_existing_account_list[i])
            main_sheet.update_cell(index,5,deal_open_no_existing_account_list[i])
            main_sheet.update_cell(index,6,deal_BAP_GCP_list[i])
            main_sheet.update_cell(index,7,deal_value_list[i])

            i += 1
            time.sleep(5)

        except Exception as e:
            print(e)
            time.sleep(10)
    
    return
    
    
    
   

def get_hubspot_deals():
    
    """
    Pull daily deal open amount and number of BAP/GCP sourced deals.


    Parameters
    ----------


    Global Variables
    ----------

    hubspot_key: str
        Key used to access HubSpot API.
        

    Returns
    ----------

    df_hubspot_deals: dataframe
        Dataframe date and deal amount.

    df_BAP_GCP_hubspot_deals: dataframe
        Dataframe date and number of BAP/GCP sourced deals created.

    """

    #set parameters to call Hubspot deal service 
    limit = 20
    deal_id_list = []
    get_all_deals_url = "https://api.hubapi.com/deals/v1/deal/paged?"
    parameter_dict = {'hapikey': hubspot_key, 'limit': limit}
    headers = {}

    #paginate request using offset
    has_more = True
    while has_more:
        
        #create request
        parameters = urllib.parse.urlencode(parameter_dict)
        get_url = get_all_deals_url + parameters

        try: #maintain loop if hit api limit
            
            #execute API request
            r = requests.get(url= get_url, headers = headers)
            response_dict = json.loads(r.text)
            has_more = response_dict['hasMore']

            #loop through list of return deals
            deals = response_dict['deals']
            for deal in deals:
                deal_id_list.append(deal['dealId'])
                
            #store parameter for pagination
            parameter_dict['offset']= response_dict['offset']
            
        except Exception as e:
            print(e)
            time.sleep

    #initiate deal ID, deal value, date, and BAP/GCP indicator lists
    deal_id_list_final = []
    deal_value_list = []
    time_value_list = []
    deal_BAP_GCP_list = []

    #loop through response to extract deal information
    i=0
    while i < len(deal_id_list):
        
        #extract deal ID
        deal_id = deal_id_list[i]

        try:
            
            #request to deal service
            url = "https://api.hubapi.com/deals/v1/deal/" + str(deal_id) + "?hapikey=" + hubspot_key
            r= requests.get(url = url)
            deal = r.json()

            #deal keys
            deal_keys = deal['properties'].keys()
            
            #initial deal value and BAP/GCP indicator
            deal_value = 0
            deal_BAP_GCP = 0
            
            #extract deal value (Has two fields. amount_in_home_currency is more reliable if available) and BAP/GCP indicator
            if 'hs_closed_amount_in_home_currency' in deal_keys:
                deal_value_temp = float(deal['properties']['hs_closed_amount_in_home_currency']['value'])
                if deal_value_temp>0:
                    deal_value = deal_value_temp
                    
            if ('amount_in_home_currency' in deal_keys) & (deal_value == 0):
                value = deal['properties']['amount_in_home_currency']['value']
                if value!='':
                    deal_value = float(value)
                    
            if 'co_seller' in deal_keys:
                if deal['properties']['co_seller']['value'] != '':
                    deal_BAP_GCP = 1

            #extract deal open timestamp
            timestamp = ''.join(deal['properties']['createdate']['value'].split())[:-3]
            if timestamp != '':
                time_value = (datetime.utcfromtimestamp(int(timestamp)) - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M:%S')  
                time_value = datetime.strptime(time_value[:10], '%Y-%m-%d')

            #check to see if the deal was created in Q2 2020 and is in the Corporate/Enterprise Sales Pipeline (pipeline = "default")
            if (time_value >= datetime.strptime('2020-04-01', '%Y-%m-%d')) & (deal['properties']['pipeline']['value'] == 'default'): 
                deal_id_list_final.append(deal_id)
                deal_value_list.append(deal_value)
                time_value_list.append(time_value)
                deal_BAP_GCP_list.append(deal_BAP_GCP)
            i+=1
            

        except:
            time.sleep(10)

        

    #generate deal value dataframe
    data = {'deal_id': deal_id_list_final, 'time_value': time_value_list, 'deal_value': deal_value_list}
    df_hubspot_deals = pd.DataFrame(data)
    
    #generate deal BAP_GCP dataframe
    data = {'deal_id': deal_id_list_final, 'time_value': time_value_list, 'deal_BAP_GCP': deal_BAP_GCP_list}
    df_BAP_GCP_hubspot_deals = pd.DataFrame(data)
    
    #convert timestamp to datetime
    df_hubspot_deals = df_hubspot_deals.rename(columns={'time_value': 'date'})
    df_hubspot_deals['date'] = df_hubspot_deals['date'].apply(lambda x: x.date())
    df_BAP_GCP_hubspot_deals = df_BAP_GCP_hubspot_deals.rename(columns={'time_value': 'date'})
    df_BAP_GCP_hubspot_deals['date'] = df_BAP_GCP_hubspot_deals['date'].apply(lambda x: x.date())

    #calculate metric values for each date
    df_hubspot_deals = df_hubspot_deals.groupby('date').deal_value.sum().reset_index()
    df_BAP_GCP_hubspot_deals = df_BAP_GCP_hubspot_deals.groupby('date').deal_BAP_GCP.sum().reset_index()

    return df_hubspot_deals, df_BAP_GCP_hubspot_deals
   
    
def main():
    
    """
    Update MQL pipeline metrics dashboards.
    
    
    Parameters
    ----------
    
 
    Global Variables
    ----------
    
    
    Returns
    ----------
    
    None 
    
    """
    
    
    #send initial email
    send_email("Initiate MQL Pipeline Dashboard Update")
    
    #generate date range dataframe
    date_beginning_quarter = datetime.strptime('2020-04-01', '%Y-%m-%d')
    df_date_range = generate_data_range_df(date_beginning_quarter)

    #pull contact form submissions, number of deals associated to contacts, and number of BAP/GCP sourced deals in Q2 2020
    df_contact_forms = pull_contact_forms()
    df_associated_deals = pull_hubspot_contacts(df_contact_forms)
    df_hubspot_deals, df_BAP_GCP_hubspot_deals = get_hubspot_deals()

    #merge contact forms and associated deals contacts
    df_contact_forms = pd.merge(df_contact_forms, df_associated_deals, on='email', how='left')

    #process data to generate df with cumulative sums of all the datapoints
    df_final_cumsum = process_data(df_date_range, df_contact_forms, df_hubspot_deals, df_BAP_GCP_hubspot_deals)

    #authenticate to Google drive API and update spreadsheet
    main_sheet = auth_google_services()
    update_google_sheet(df_final_cumsum, main_sheet)

    #send email when completed
    send_email("MQL Pipeline Dashboard Update Completed")
    
    return 


 
#execute update
if __name__ == "__main__":
    main()
