# -*- coding: utf-8 -*-
"""forcast.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1FcNnO4SqqIIM7Oz5ykpnuftsMGUSDSfJ
"""

# Import the requests library
import requests

# Use the provided OpenWeatherMap API key
api_key = "f5875dd3f8485520bc0f3f5ec39eacb8"

# Set coordinates for Portland, OR
lat = 45.51
lon = -122.67

# Build the 3-day forecast API endpoint URL
base_url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&cnt=24&appid={api_key}"

# Make the GET request to the API
response = requests.get(base_url)

# If the response is successful, process the JSON
if response.status_code == 200:
    data = response.json()
    forecast_list = data["list"]

    # Check each forecast entry for rain-related weather
    rain_forecasted = False
    for entry in forecast_list:
        weather_main = entry["weather"][0]["main"].lower()
        if weather_main in ["rain", "drizzle", "thunderstorm"]:
            rain_forecasted = True
            break

    if rain_forecasted:
        print("☔ Yes, it is forecasted to rain in Portland within the next three days.")
    else:
        print("🌤 No, there is no rain in the forecast for Portland in the next three days.")
else:
    print(f"API Error: {response.status_code}")
    print("Details:", response.json())