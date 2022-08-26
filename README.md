# Air Quality ETL
> Binar Platinum Challenge
> Bussiness Intelligence Engineer Course

This is an ETL (extract, transform, load) project to complete Binar BIE Platinum level. All data is retrieved from [Weatherbit.io]([https://link](https://www.weatherbit.io/api/airquality-history)), including the cities and states data ([Weatherbit Metadata](https://www.weatherbit.io/api/meta)).

To run this project, follow these steps:
1. Install all dependencies
    ```bash
    pip install -r requirements.txt
    ```
2. Duplicate `.env.example` flie and rename it to `.env`.
3. Fill the variables in `.env` file.
4. Run the program
    ```bash
    python main.py
    ```