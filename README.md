## Real-Time Weather Data Pipeline with Kafka & Confluent

A real-time data streaming pipeline that fetches live weather metrics for global cities via the OpenWeather API, packages the data into structured JSON payloads, and streams them into an Apache Kafka topic hosted on Confluent Cloud.

### Project Structure

```text
├── src/                      # Source code directory
│   └── __init__.py           # Package initializer
├── weather_producer.py       # Fetches weather data and produces to Kafka
├── weather_consumer.py       # Consumes weather data from Kafka
├── read_config.py            # Parses client.properties configuration
├── main.py                   # Simple script initialization entry point
├── client.properties.example # Template for Confluent Cloud credentials
├── requirements.txt          # Python project dependencies
└── README.md                 # Project documentation
```


### Setup Instructions

#### 1. Install Dependencies
This project uses **uv** for fast package management. 
```bash
uv pip install -r requirements.txt
```

#### 2. Configure Credentials
* Duplicate `client.properties.example` and rename it to `client.properties`. Add your **Confluent Cloud** keys inside it.
* Create a `.env` file in the root directory and add your **OpenWeather API key**:
  ```text
  OPENWEATHER_API_KEY=your_api_key_here
  ```

### How to Run

#### Start the Producer
```bash
uv run weather_producer.py
```

#### Start the Consumer
In a new terminal window, run:
```bash
uv run weather_consumer.py
```

### Data Output Sample
```json
{
  "city": "Nairobi",
  "country": "KE",
  "temperature": 15.8,
  "feels_like": 15.5,
  "humidity": 76,
  "weather": "broken clouds",
  "time": "2026-08-03 22:55:52"
}
```













