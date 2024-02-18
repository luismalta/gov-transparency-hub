# Gov Transparency Hub

Gov Transparency Hub is a data pipeline project built using Dagster to collect information from transparency portals of multiple cities. It focuses on gathering data like revenue and expenses, and aims to create a consolidated database that can be accessed via an API by third-party applications.

## Getting started

To set up the project, make sure you have Poetry and Docker installed.

1. Install dependencies using Poetry
    ```bash
    poetry install
    ```
2. Adjust service environment variables in .env.
It is possible to launch services locally using Docker Compose present in the project

3. Launch Dagster
   ```bash
   dagster dev
   ```