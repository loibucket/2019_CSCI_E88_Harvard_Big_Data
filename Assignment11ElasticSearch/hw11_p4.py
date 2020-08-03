import pandas as pd
import numpy as np
import random

#make data
def generate_df(id_list, timestring, min_interval, max_interval, url_list, user_list, country_list):

    df = pd.DataFrame(id_list)
    df[0] = df[0].astype(str).apply(lambda x: 'id'+x)

    #generate incremental time
    #time_list = [pd.Timestamp('2019-11-22T00Z')]
    time_list = [pd.Timestamp(timestring)]
    for i in range(1,len(df)):
        time_list.append( time_list[-1] + pd.Timedelta(seconds=random.randint(min_interval,max_interval)) )
    df[1] = time_list

    #generate url
    df[2] = np.random.choice(url_list, size=len(df))

    #generate user
    df[3] = np.random.choice(user_list, size=len(df))

    #generate country
    df[4] = np.random.choice(country_list, size=len(df))

    #generate browser
    df[5]  = np.random.choice(['Chrome','Firefox','Edge','IE'], size=len(df))

    #generate OS
    df[6] = np.random.choice(['Mac','Linux','iPhone','Windows'], size=len(df))

    #generate Response
    df[7] = np.random.randint(100,600,size=len(df))

    #generate TTFB
    df[8] = np.random.uniform(0.1,13,size=len(df))
    df[8] = df[8].round(3)
    
    return df

#specify urls, users, countries
url_list = ['http://100.com/100','http://200.com/200','http://300.com/300',\
                'http://400.com/400','http://500.com/500']

user_list = ['user9000','user800','user10','user77','user55']

country_list = ['ER','SJ','MA','GD','ZW']

big_url_list = url_list + ['http://google.com/cow','http://bing.com/cat','http://espn.com/ball',\
                'http://espn.com/ball','http://amazon.com/fire']

big_user_list = user_list + ['bob','carly','moo','bigbig','buck']

big_country_list = country_list + ['AF','AL','AS','AD','AQ']

#make data with variations in users country urls
timestring = '2019-11-22T12Z'
id_list = list(range(0,21))
min_interval = 100
max_interval = 200

df_1 = generate_df(id_list, timestring, min_interval, max_interval, url_list, user_list, country_list)

timestring = '2019-11-22T13Z'
id_list = list(range(21,51))
min_interval = 100
max_interval = 200

df_2 = generate_df(id_list, timestring, min_interval, max_interval, big_url_list, big_user_list, big_country_list)

timestring = '2019-11-22T14Z'
id_list = list(range(51,60))
min_interval = 100
max_interval = 200

df_3 = generate_df(id_list, timestring, min_interval, max_interval, url_list, user_list, country_list)


df = pd.concat([df_1,df_2,df_3])
df = df.reset_index(drop = True)
df = df.rename(columns={0: "logId", 1: "eventTime", 2: "url", 3:"userId", 4:"us_country", 5:"browser", 6:"ua_os", 7:"responseCode", 8:"ttfb"})

#add geolocation
country_location = {}
country_location['ER'] = "15.1794,39.7823"
country_location['SJ'] = "77.5536,23.6703"
country_location['MA'] = "31.7917,7.0926"
country_location['GD'] = "12.1165,61.6790"
country_location['ZW'] = "19.0154,29.1549"
country_location['AF'] = "33.9391,67.7100"
country_location['AL'] = "41.1533,20.1683"
country_location['AS'] = "37.0902,95.7129"
country_location['AD'] = "42.5063,1.5218"
country_location['AQ'] = "-75.250973,-0.071389"

df['location']=df['us_country'].map(country_location)

#write curl commands to file
f = open("curl_p4.sh", "w")
for i in range(len(df)):
    f.write("""curl -XPOST 'localhost:9200/hw11_p2_index/_doc/""" +str(i)+ """?pretty' -H 'Content-Type: application/json' -d '""")
    f.write(df.iloc[i].to_json(orient='index'))
    f.write("""'\n""")
f.close()