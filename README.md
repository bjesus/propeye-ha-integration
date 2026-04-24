# Propeye Home Assistant Integration

This custom component integrates [Propeye](https://propeye.se/) electricity consumption data into Home Assistant.

## Features
- **Accurate hourly data** for the Energy Dashboard: the integration pushes the API's per-hour kWh values straight into Home Assistant's long-term statistics, so each hour shows the value Propeye reports rather than whatever happens to fall between polls.
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

## What you get

### Sensor
- `sensor.electricity_consumption` — today's cumulative consumption in kWh.
  This value resets at midnight and is convenient for dashboards or
  automations that want a live "consumption so far today" figure.

### Long-term statistic (use this for the Energy Dashboard)
- `propeye:electricity_consumption_<slugified_email>` — an external
  long-term statistic with one row per hour, mirroring the Propeye
  API's own hourly breakdown.

**To use in the Energy Dashboard:**

1. Go to **Settings** → **Dashboards** → **Energy**.
2. Under **Electricity grid** click **Add consumption**.
3. Select the statistic whose id starts with `propeye:electricity_consumption_…`.

Using this external statistic (rather than `sensor.electricity_consumption`)
is strongly recommended because the Propeye API occasionally consolidates
multiple hours of consumption into a single data point. If the Energy
Dashboard is bound to the raw sensor, HA will see the state "jump" when
the consolidated value arrives and show some hours as 0 kWh followed by
a large spike. The external statistic avoids this: on every poll we
re-fetch the last several days of hourly data from the API and update
the statistics table in place, so each hour always shows exactly what
Propeye says.
