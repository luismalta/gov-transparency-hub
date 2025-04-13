# Gov Transparency Hub

Gov Transparency Hub is a data pipeline project built using Dagster to collect information from transparency portals of multiple cities. It focuses on gathering data like revenue and expenses, aiming to create a consolidated database that can be accessed via an API by third-party applications.

## Getting Started

To set up the project, ensure you have `uv` and Docker installed.

1. Install dependencies using `uv`:
    ```bash
    uv install
    ```

2. Configure the service environment variables in the `.env` file. You can find a sample `.env.example` file in the project directory to guide your setup.

3. Launch the required services locally using Docker Compose:
    ```bash
    docker-compose up
    ```

4. Start Dagster for development:
    ```bash
    uv run dagster dev
    ```

## Technologies Used for ETL

The project leverages the following technologies to implement the ETL (Extract, Transform, Load) process:

- **Dagster**: Orchestrates the ETL pipelines, defining jobs, schedules, and sensors to automate data workflows.
- **DBT (Data Build Tool)**: Handles data transformations and modeling, ensuring data consistency and quality in the target database.
- **DLT (Data Load Tool)**: Simplifies the process of extracting and loading data into the target database, providing a framework for incremental data loading.
- **PostgreSQL**: Serves as the primary database for storing consolidated data, including raw and processed datasets.
- **Python**: Provides the core programming language for implementing custom logic, scrapers, and utility functions.
- **BeautifulSoup**: Used for web scraping to extract data from transparency portals.
- **Pandas**: Facilitates data manipulation and cleaning during the transformation phase.
- **Docker**: Ensures consistent development and deployment environments for the ETL pipelines.
- **Minio**: Used for storing intermediate or large datasets during the ETL process.

## Project Structure

The project is organized as follows:

- **`gov_transparency_hub/assets/`**: Contains Dagster assets for processing city revenue and expenses, as well as utility functions and constants.
  - `city_expenses.py`: Handles expense-related data processing.
  - `city_revenue.py`: Handles revenue-related data processing.
  - `utils.py`: Utility functions for formatting and processing.
- **`gov_transparency_hub/dbt_pipelines/`**: Contains DBT project files for transforming and modeling data.
  - `models/`: SQL models for data transformations.
  - `tests/`: Tests for DBT models.
  - `profiles.yml`: Configuration for DBT connections.
- **`gov_transparency_hub/jobs/`**: Dagster job definitions for running pipelines.
- **`gov_transparency_hub/partitions/`**: Partition definitions for Dagster assets.
- **`gov_transparency_hub/resources/`**: Custom resources for interacting with external systems like databases and S3.
  - `PortalTransparenciaScrapper.py`: Scraper for fetching data from transparency portals.
  - `dbt_connection.py`: DBT connection resource.
  - `constants.py`: Shared constants for resources.
- **`gov_transparency_hub/schedules/`**: Dagster schedules for automating pipeline execution.
- **`gov_transparency_hub/sensors/`**: Dagster sensors for triggering pipelines based on external events.
- **`tests/`**: Unit and integration tests to ensure the reliability of the pipelines and utilities.


## Contributing

Contributions are welcome! Please follow the guidelines in `CONTRIBUTING.md` and ensure all changes are tested before submitting a pull request. Additionally, make sure your code adheres to the project's coding standards.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Additional Resources

- **Dagster Documentation**: [https://docs.dagster.io](https://docs.dagster.io)
- **Docker Documentation**: [https://docs.docker.com](https://docs.docker.com)
- **uv Documentation**: [https://github.com/your-uv-repo](https://github.com/your-uv-repo) (replace with the actual link if available)
