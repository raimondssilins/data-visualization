import token
from flask import Flask, jsonify
from pyspark.sql import SparkSession
import snowflake.connector
from sqlalchemy import create_engine
import matplotlib
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

matplotlib.use('Agg')

# Snowflake connection parameters
snowflake_config = {
    'user': 'raimondss1',
    'password': 'hacruf-qodvar-sEvwy3',
    'account': 'zodwsgw-kw81190',
    'warehouse': 'FINAL_PROJECT',
    'database': 'COVID19_EPIDEMIOLOGICAL_DATA',
    'schema': 'public'  # Adjust the schema if necessary, in perfect case 
    # I would move it to global variable so it is not read public, but this is just an example
}

# Snowflake connection
conn = snowflake.connector.connect(**snowflake_config)
cur = conn.cursor()

spark = SparkSession.builder.appName("ProjectApp").getOrCreate()
# Created a dictionary to store data as cache
cache = {}

def execute_spark_query(query, selected_columns, filter_condition):
    cur.execute(query)

    # Fetch data
    result = cur.fetchall()

    # Convert result to a list of dictionaries
    column_names = [desc[0] for desc in cur.description]
    data = [dict(zip(column_names, row)) for row in result]

    df = spark.createDataFrame(data)

    # Cache the entire DataFrame
    df.persist()

    # Selecting specific columns from the Spark DataFrame
    selected_data = df.select(selected_columns).where(filter_condition).orderBy(selected_columns[1], ascending=False)

    return selected_data

@app.route('/get_data/<string:query>', methods=['GET']) 
def get_data(query):
    try:
        # An example of how it would be possible token query snowflake
        # Execute your Snowflake query
        query = query
        
        cur.execute(query)

        # Fetch data
        result = cur.fetchall()

        # Convert result to a list of dictionaries
        column_names = [desc[0] for desc in cur.description]
        data = [dict(zip(column_names, row)) for row in result]

        df = spark.createDataFrame(data)

        return jsonify({'data': df})
    
    except Exception as e:
        return jsonify({'error': str(e)})
    
# @app.route('/get_comment/<int::selectedChart>', methods=['GET'])
# def get_comment(selectedChart):
#     try:
#         #I started the mongoDB part a bit late, but would implement it by using the mongo-actions, by getting the onClick information
            # from the react to the python part
#         return jsonify({'comment': 'comment'})

# Define your route
# Define your route
@app.route('/get_transformed_data/<int:selectedChart>/<int:selectedAmount>', methods=['GET'])
def get_transformed_data(selectedChart, selectedAmount):
    try:
        if selectedChart == 1:
            query = "SELECT COUNTRY_REGION, SUM(deaths) AS total_deaths, MAX(POPULATION) - SUM(deaths) AS remaining_population, ROUND((SUM(deaths) / MAX(POPULATION)) * 100, 3) AS percentage_died FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION ORDER BY percentage_died DESC "
            selected_columns = ['COUNTRY_REGION', 'PERCENTAGE_DIED']
            filter_condition = 'PERCENTAGE_DIED > 0'
        elif selectedChart == 2:
            query = "SELECT COUNTRY_REGION, MAX(PEOPLE_FULLY_VACCINATED) as FULLY_VACCINATED, MAX(LAST_OBSERVATION_DATE) FROM OWID_VACCINATIONS GROUP BY COUNTRY_REGION"
            selected_columns = ['COUNTRY_REGION', 'FULLY_VACCINATED']
            filter_condition = 'FULLY_VACCINATED > 0'
        elif selectedChart == 3:
            query = """
                SELECT
                    A.COUNTRY_REGION,
                    B.HDI,
                    A.percentage_died,
                    ROUND((A.percentage_died / B.HDI) * 100, 3) AS ratio
                FROM (
                    SELECT
                        COUNTRY_REGION,
                        SUM(deaths) AS total_deaths,
                        MAX(POPULATION) - SUM(deaths) AS remaining_population,
                        ROUND((SUM(deaths) / MAX(POPULATION)) * 100, 3) AS percentage_died
                    FROM
                    COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL
                    GROUP BY
                        COUNTRY_REGION
                    HAVING
                        percentage_died > 0
                ) A
                JOIN (
                    SELECT DISTINCT
                        COUNTRY,
                        HDI
                    FROM
                        FINAL_PROJECT.PUBLIC.ECONOMIC_DATA
                    WHERE
                        HDI > 0
                ) B
                ON
                    A.COUNTRY_REGION = B.COUNTRY
                ORDER BY
                    ratio DESC;
                """
            selected_columns = ['COUNTRY_REGION', 'ratio']
            filter_condition = 'ratio > 0'
        else:
            return jsonify({'error': 'Invalid selectedChart value'})

        # Check if the DataFrame is already in cache
        if selectedChart not in cache:
            # Execute the Spark query and store the result in cache
            cache[selectedChart] = execute_spark_query(query, selected_columns, filter_condition)

        # Retrieve the cached DataFrame
        selected_data = cache[selectedChart].limit(selectedAmount).toPandas()

        # Convert Pandas DataFrame to a dictionary for the chart
        data_dict = dict(zip(selected_data[selected_columns[0]], selected_data[selected_columns[1]]))

        # Return the result
        return jsonify({'data': data_dict})

    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)