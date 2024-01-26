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
    'schema': 'public'  # Adjust the schema if necessary
}

# Snowflake connection
conn = snowflake.connector.connect(**snowflake_config)
cur = conn.cursor()

spark = SparkSession.builder.appName("ProjectApp").getOrCreate()

@app.route('/get_data/<string:query>', methods=['GET'])
def get_data(query):
    try:
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
#         #I need to connect to mongoDB database
        

#         return jsonify({'comment': 'comment'})

@app.route('/get_transformed_data/<int:selectedChart>/<int:selectedAmount>', methods=['GET'])
def get_transformed_data(selectedChart, selectedAmount):
    try:
        # Execute your Snowflake query
        if(selectedChart == 1):
            query = "SELECT COUNTRY_REGION, SUM(deaths) AS total_deaths, MAX(POPULATION) - SUM(deaths) AS remaining_population, ROUND((SUM(deaths) / MAX(POPULATION)) * 100, 3) AS percentage_died FROM ECDC_GLOBAL GROUP BY COUNTRY_REGION ORDER BY percentage_died DESC "
        
        if(selectedChart == 2):
            query = "SELECT COUNTRY_REGION, MAX(PEOPLE_FULLY_VACCINATED) as FULLY_VACCINATED, MAX(LAST_OBSERVATION_DATE) FROM OWID_VACCINATIONS GROUP BY COUNTRY_REGION"

        if(selectedChart == 3):
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

        cur.execute(query)

        # Fetch data
        result = cur.fetchall()

        # Convert result to a list of dictionaries
        column_names = [desc[0] for desc in cur.description]
        data = [dict(zip(column_names, row)) for row in result]

        df = spark.createDataFrame(data)

        # Selecting specific columns from the Spark DataFrame
        if(selectedChart == 1):
            selected_columns = ['COUNTRY_REGION', 'PERCENTAGE_DIED']
            selected_data = df.select(selected_columns).where('PERCENTAGE_DIED > 0').orderBy('PERCENTAGE_DIED', ascending=False).limit(selectedAmount).toPandas()

            # Convert Pandas DataFrame to a dictionary with 'COUNTRY_REGION' as key and 'PERCENTAGE_DIED' as value
            data_dict = dict(zip(selected_data['COUNTRY_REGION'], selected_data['PERCENTAGE_DIED']))
            return jsonify({'data': data_dict})


        if(selectedChart == 2):
            selected_columns = ['COUNTRY_REGION', 'FULLY_VACCINATED']
            selected_data = df.select(selected_columns).where('FULLY_VACCINATED > 0').orderBy('FULLY_VACCINATED', ascending=False).limit(selectedAmount).toPandas()

            data_dict = dict(zip(selected_data['COUNTRY_REGION'], selected_data['FULLY_VACCINATED']))
            return jsonify({'data': data_dict})
        
        if(selectedChart == 3):
            selected_columns = ['COUNTRY_REGION', 'ratio']
            selected_data = df.select(selected_columns).where('ratio > 0').orderBy('ratio', ascending=False).limit(selectedAmount).toPandas()

            data_dict = dict(zip(selected_data['COUNTRY_REGION'], selected_data['ratio']))
            return jsonify({'data': data_dict})

    
    except Exception as e:
        return jsonify({'error': str(e)})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)