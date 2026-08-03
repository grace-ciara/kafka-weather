from confluent_kafka import Producer
from read_config import read_config
import requests
import os
from dotenv import load_dotenv
import time 
from datetime import datetime
import json

load_dotenv() 

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
config = read_config()
topic = "weather"
producer = Producer(config)

cities = [("Nairobi", "KE"),
          ("Ottawa", "CA"),
          ("Cairo", "EG"),
          ("Tokyo","JP"),
          ("Paris","FR"),
          ("Berlin","DE"),
          ("London","GB"),]

def fetch_weather(city_name,country_code):
    BASE_URL = f"https://api.openweathermap.org/data/2.5/weather?q={city_name},{country_code}&appid={OPENWEATHER_API_KEY}"

    response = requests.get(BASE_URL)
    if response.status_code == 200:
        return{
            "city": city_name,
            "country": country_code,
            "temperature": round(response.json()["main"]["temp"] - 273.15, 1),
            "feels_like": round(response.json()["main"]["feels_like"] - 273.15, 1),
            "humidity": response.json()["main"]["humidity"],
            "weather": response.json()["weather"][0]["description"],
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        }
    else:
        print(f"Error fetching weather data: {response.status_code}")
        return None

while True:
    for city_name, country_code in cities:
        weather = fetch_weather(city_name, country_code)
        producer.produce(topic, json.dumps(weather).encode("utf-8"), key=weather["city"].encode("utf-8"))
        producer.flush()
        print(f"Produced {weather['city']} weather data to {topic} topic.")
    time.sleep(5) # wait for 5 seconds before fetching the weather data again



