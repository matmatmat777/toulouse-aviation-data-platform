import requests
import os

app_env = os.environ.get("APP_ENV", "dev")

print("Toulouse Aviation Data Platform")
print(f"Requests version: {requests.__version__}")
print("Build Docker v2")
print(f"Environment: {app_env}")