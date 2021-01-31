import time
from pywikibot.comms.eventstreams import EventStreams
import datetime

def main_function():
    query_timestamp = query_timestamp = (datetime.datetime.utcnow() - datetime.timedelta(minutes=1)).strftime('%Y%m%d%H%M%S')
    now = int(datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S'))
    wiki_data = []
    domain_to_data={}
    stream = EventStreams(streams=['revision-create'],since=query_timestamp)
    while True:
        response=next(iter(stream))
        # checking is user is bot or not , if not bot then only considering the response
        if(response['performer']['user_is_bot']!=True):
            wiki_data.append(response)
            if response['meta']['domain'] in domain_to_data:
                domain_to_data[response['meta']['domain']].append(response)
            else:
                domain_to_data[response['meta']['domain']]=[response]

        res_time = response['rev_timestamp'].split('T')
        res_time =int(''.join(res_time[0].split('-'))+''.join(res_time[1][:-1].split(':')))
        # fetching data since last minute of main_function executed
        if res_time>=now:
            break
            
    # For displaying  Domain Reports
    d_t ={}
    users_data=[]
    pages_count = 0
    for domain in domain_to_data.keys():
        pages = []
        user_list = []
        for reports in domain_to_data[domain]:
            pages.append(reports['page_title'])
            name = reports['performer']['user_text']
            try:
                e_count = reports['performer']['user_edit_count']
            except:
                # if user_edit_count doesn't exist , we counting as 0
                e_count = 0
            user_list.append([name,e_count])      
        users_data.append(user_list)
        d_t[domain]=len(set(pages))
    d_t=sorted(d_t.items(), key=lambda x: x[1],reverse=True) 
    print("Sample Reports")
    print("Total number of Wikipedia Domains Updated ",len(domain_to_data.keys()),'\n')
    for i in d_t:
        print(f'{i[0]}: {i[1]} pages updated ')
    
    # For displaying User report
    k = 0
    print("\n\t\tUSER Report \n")
    for domain_user in users_data:
        print(f'\nUsers who updates to {list(domain_to_data.keys())[k]}\n')
        user_to_count={}
        for user in domain_user:
            if user[0] not in user_to_count:
                user_to_count[user[0]] = user[1]
            else:
                user_to_count[user[0]] =user[1]
        for name,count in user_to_count.items():
            print(f'{name} : {count}')
        k+=1

c = 0

#executing main_function for every one minute
def execute():
    main_function()
#     print('------------------------------------------------------------------')
    global c
    c+=1
    time.sleep(60)


# executing main_function for 10 minutes
while True:
    print('#######################################################################################',c)
    execute()
    if c==10:
        break
