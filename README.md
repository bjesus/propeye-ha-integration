# Propeye Home Assistant Integration

This custom component integrates [Propeye](https://propeye.se/) electricity consumption data into Home Assistant.

## Features
- **Energy Dashboard Compatible**: Adds a sensor that tracks daily energy usage, fully compatible with the Energy Dashboard.
- **Hourly Updates**: Fetches data every 15 minutes to capture the latest hourly readings (with ~1 hour delay from source).
- **Secure**: Uses your Propeye email and password to authenticate and retrieve a secure token.

## Installation

### Option 1: HACS (Recommended)
1. Open HACS in Home Assistant.
2. Go to "Integrations" > Top right menu > "Custom repositories".
3. Add the URL of this repository.
4. Select "Integration" as the category.
5. Click "Add" and then install "Propeye".
6. Restart Home Assistant.

### Option 2: Manual
1. Copy the `custom_components/propeye` folder to your Home Assistant `config/custom_components/` directory.
2. Restart Home Assistant.

## Configuration
1. Go to **Settings** > **Devices & Services**.
2. Click **Add Integration**.
3. Search for **Propeye**.
4. Enter your **Email** and **Password**.

## Sensors
- `sensor.electricity_consumption`: Shows the total energy consumed today (kWh).
