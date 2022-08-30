# Air Quality ETL
> Binar Business Intelligence Engineer Course Platinum Challenge

This is an ETL (extract, transform, load) project to complete Binar BIE Platinum level. All data are retrieved from [Weatherbit.io](https://www.weatherbit.io/api/airquality-history), including the cities and states data ([Weatherbit Metadata](https://www.weatherbit.io/api/meta)). The AQI breakpoint refers to data from the [US-EPA](https://aqs.epa.gov/aqsweb/documents/codetables/aqi_breakpoints.html).

To run this project, follow these steps:
1. Install all dependencies.
    ```bash
    pip install -r requirements.txt
    ```
2. Duplicate `.env.example` flie and rename it to `.env`.
3. Fill the variables in `.env` file.
4. Run the program.
    ```bash
    python main.py
    ```