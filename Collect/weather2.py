"""
We collect weather data from openweathermap and store to a postgrss database.
The script run cuntinioulsy and collect the weahter data every 15 minutes
The data is collected from openweathermap.org and stored in a postgresql database.
We collect for evry city:temperature,pressure,humidity,windspeed,winddirection,weatherdescription and a timestamp.One
15 minute time
The openweathermap offer the real time weather but every city has different schema , and we have to make
the common fields for now

The script


"""
# importing requests and json
import requests,json
import time
import pandas as pd

from Database.Connector import *
import os
print(os.getcwd())
#import paramiko
import sys
import subprocess

#import sshtunnel
import pandas as pd
import os
from sshtunnel import open_tunnel
from time import sleep


def ssh_r():
       """
Open ssh tunnel with ubuntu server. We can change the post and the ip we want to execute for other machines  and ports.
Also the ueser name and passord we are going to put to an external file.
Before opening thr connector to the database we have execute the function to create a tunnel to iphost:port to a 127.0.0.1:dynamic port
. Always the ip will be 127.0.0.1 but the port it will be dynamic, Finaly we have to inform the settings from open connector the port number
Otupout the server connection and the Port
       """
       with open_tunnel(
    ('160.40.48.128', 22),
    ssh_username=os.environ.get('USER_'),
    ssh_password=os.environ.get('PASSWORD_'),
    remote_bind_address=('127.0.0.1', 44421)
       ) as server:
              port=server.local_bind_port

       return server,port



server,port=ssh_r()
server.start()


#inform the open connector to dynamic port
import configparser
config = configparser.ConfigParser()
config.read('settings/database.ini')
#port = config.get('postgresql','port')
config.set('postgresql', 'port',str(server.local_bind_port))

with open('settings/database.ini', 'w') as configfile:
    config.write(configfile)
print(port)
print(server.local_bind_port)


open_connector()
close_connector()




# base URL
BASE_URL = "https://api.openweathermap.org/data/2.5/weather?"
CITY = "Thessaloniki"
API_KEY = "21cd99dc6132d832aa7eec5f9604ba7b" #georgesidira

API_KEY2="3a6c74b2797ba82a744ab21517b0bed7"

def get_weather(BASE_URL,API_KEY,city):
       """
       get weather data from openweathermap.org
       :param city:
       output:[timestamp,country,CITY,main,wind,weather_description]
              main:'temp,feels_like,temp_min,temp_max,pressure,humidity
       """
       from datetime import date, timedelta,datetime,timezone

       #Generete Timestamp

       # current date and time
       now = datetime.now()
       timestamp = str(datetime.now(timezone.utc))
       #timestamp=datetime.fromtimestamp(timestamp)
       print("timestamp =", timestamp,city)
       url = BASE_URL + "appid=" + API_KEY + "&q=" + city
       try:
       #When we have internet connection then try and ok but when we do not have then the script will crash / we neeed to run even with out a momment with no internet then the data will be 0, ath the database
              response = requests.get(url)
              data = json.loads(response.text)

              CITY = city
       # upadting the URL
              URL = BASE_URL + "q=" + CITY + "&appid=" + API_KEY+"&units=metric"
       # HTTP request
              response = requests.get(URL)
       # checking the status code of the request
              if response.status_code == 200:
       # getting data in the json format
                     data = response.json()
              #getting the main dict block
                     main = data['main']

              # getting temperature
                     temperature = main['temp']
              # getting the humidity
                     humidity = main['humidity']
              # getting the pressure
                     pressure = main['pressure']
                     wind = data['wind']
              # getting the country
                     country = data['sys']['country']
                     temperature=data['main']['temp']
                     feels_like=data['main']['feels_like']
                     temp_min=data['main']['temp_min']
                     temp_max=data['main']['temp_max']
                     pressure=data['main']['pressure']
                     humidity=data['main']['humidity']
                     #grnd_level=data['main']['grnd_level']
                     wind_speed=data['wind']['speed']
                     wind_deg=data['wind']['deg']
                     #wind_gust=data['wind']['gust']
                     sunrise=data['sys']['sunrise']
                     sunset=data['sys']['sunset']
                     weather_description=data['weather'][0]['description']

                     data_w=[timestamp,country,CITY,temperature,feels_like,temp_min,temp_max,pressure,humidity,wind_speed,wind_deg,sunrise,sunset,weather_description]

              else:
                     # showing the error message
                     print("Error in the HTTP request")
                     data_w= [timestamp,'country0',CITY,0,0,0,0,0,0,'winspeed0','wind_deg0','None']


       except:
              data_w=[0,0,0,0,0,0,0,0,0,0,0,0,0,0]
       return data_w

cities_df = pd.read_excel (r'CW_customers_cities.xlsx')

c=["Thessaloniki","Athens","Serres","Patra","Rodos","Kalamata","Alexandroupoli","Drama","Corfu","Stoholm","Amsterdam","London","Paris","Ioannina","Berlin","Rome",
   "Madrid","Budapest","Bucharest","Prague","Vienna","Bern","Dublin","Glasgow","Copenhagen","Boden","Riga","Minsk","Alicante","Bari","Munich","Gothenburg","Kiruna","Umea","Uppsala","Jonkoping","Karlstad",
   "Kalmar","Norrkoping","Danderyd"]

cities=cities_df["city"].unique().tolist()
#zipcode=cities_df["zipcode"].unique().tolist()

while True:
       weather_d=[]
       for i in range(0,len(cities)):
           weather_d.append(get_weather(BASE_URL,API_KEY,cities[i]))
           if (i+1)%60==0:
               open_connector()
               for j in weather_d:
                   connector('weather', 1, j)

               close_connector()
               weather_d = []

               #waitafter 60 cities for 4 or 5 minites all cities are 780 . We need a hour to take all cities. There is restiction 1.000.000 per month
               time.sleep(240)


       open_connector()
       for i in weather_d:
              connector('weather', 1, i)

       close_connector()
server.stop()
server.close()

       


