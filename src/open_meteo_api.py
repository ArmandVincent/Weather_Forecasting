import requests
import pandas as pd

class OpenMeteoHistoricalAPI:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

    HOURLY_VARIABLES = {
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
        "pressure_msl", "surface_pressure", "precipitation", "rain", "snowfall",
        "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
        "shortwave_radiation", "direct_radiation", "direct_normal_irradiance",
        "diffuse_radiation", "global_tilted_irradiance", "sunshine_duration",
        "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m",
        "wind_gusts_10m", "et0_fao_evapotranspiration", "weather_code", "snow_depth",
        "vapour_pressure_deficit", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm",
        "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm",
        "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm",
        "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm"
    }

    DAILY_VARIABLES = {
        "weather_code", "temperature_2m_max", "temperature_2m_min",
        "apparent_temperature_max", "apparent_temperature_min",
        "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
        "sunrise", "sunset", "sunshine_duration", "daylight_duration",
        "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
        "shortwave_radiation_sum", "et0_fao_evapotranspiration"
    }

    def __init__(
        self,
        latitude: float,
        longitude: float,
        variables: list,
        start_date: str,
        end_date: str,
        timezone: str = "auto",
        temperature_unit: str = "celsius",
        wind_speed_unit: str = "kmh",
        precipitation_unit: str = "mm",
        timeformat: str = "iso8601",
        model: str = None,
        elevation: float = None,
        cell_selection: str = "land"
    ):
        self.latitude = latitude
        self.longitude = longitude
        self.variables = variables
        self.start_date = start_date
        self.end_date = end_date
        self.timezone = timezone
        self.temperature_unit = temperature_unit
        self.wind_speed_unit = wind_speed_unit
        self.precipitation_unit = precipitation_unit
        self.timeformat = timeformat
        self.model = model
        self.elevation = elevation
        self.cell_selection = cell_selection

        self.hourly_vars = [v for v in variables if v in self.HOURLY_VARIABLES]
        self.daily_vars = [v for v in variables if v in self.DAILY_VARIABLES]

        if not self.hourly_vars and not self.daily_vars:
            raise ValueError("No valid variables found in hourly or daily categories.")

    def _build_params(self):
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "timezone": self.timezone,
            "temperature_unit": self.temperature_unit,
            "wind_speed_unit": self.wind_speed_unit,
            "precipitation_unit": self.precipitation_unit,
            "timeformat": self.timeformat,
            "cell_selection": self.cell_selection
        }

        if self.hourly_vars:
            params["hourly"] = ",".join(self.hourly_vars)
        if self.daily_vars:
            params["daily"] = ",".join(self.daily_vars)
        if self.model:
            params["model"] = self.model
        if self.elevation is not None:
            params["elevation"] = self.elevation

        return params

    def fetch(self):
        params = self._build_params()
        try:
            response = requests.get(self.BASE_URL, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API request error: {e}")
            return None

    def to_dataframe(self):
        """Fetches data and returns one or two DataFrames depending on available frequency."""
        data = self.fetch()
        if data is None:
            raise ValueError("No data fetched from API.")

        dfs = {}

        if "hourly" in data:
            hourly_df = pd.DataFrame(data["hourly"])
            hourly_df["time"] = pd.to_datetime(hourly_df["time"])
            hourly_df.set_index("time", inplace=True)
            dfs["hourly"] = hourly_df

        if "daily" in data:
            daily_df = pd.DataFrame(data["daily"])
            daily_df["time"] = pd.to_datetime(daily_df["time"])
            daily_df.set_index("time", inplace=True)
            dfs["daily"] = daily_df

        if not dfs:
            raise ValueError("No 'hourly' or 'daily' data found in API response.")

        return dfs["hourly"] if len(dfs) == 1 else dfs
    