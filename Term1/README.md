# Data Engineering I. - Term Project I. Documentation

**Author:** Péter Bence Török

**Email:** Toeroek_Peter-Bence@student.ceu.edu

**Program:** Central European University (CEU) - Business Analytics Msc 2024/25

**Date of submission:** 03.11.2024

## Table of Contents
1. [EXECUTIVE SUMMARY](#executive-summary)
2. [DATASET DESCRIPTION](#dataset-description)
3. [OPERATIONAL LAYER (OLTP)](#operational-layer)
4. [ANALYTICAL PLAN](#analytical-plan)
5. [ANALYTICAL LAYER](#analytical-layer)
6. [ETL PIPELINE](#etl-pipeline)
7. [DATA MARTS](#data-marts)
8. [OPPORTUNITIES FOR FUTURE DEVELOPMENT](#opportunities-for-future-development)

## 1. EXECUTIVE SUMMARY

For my Data Engineering I. first term project, I utilized a Formula 1  relational dataset sourced from [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) to analyze race performance, drivers, constructors, and circuit data. The dataset, containing historical F1 data between 1950 and 2024, allowed for the creation of a comprehensive data pipeline and the exploration of key insights into Formula 1 racing.

The project began by designing and implementing a relational database schema in MySQL, where I created normalized tables to store information about races, drivers, constructors, and results. Relationships between entities were established through primary and foreign keys to ensure data integrity.

Next, I performed data transformation and denormalization tasks to build optimized structures for analytical queries. This included calculating derived fields such as driver age at the time of each race and aggregating race statistics to gain performance insights.

Additionally, I developed an ETL pipeline that automates data loading and transformation using SQL event triggers. This pipeline ensures that new data is processed efficiently and made available for reporting. Data marts and views were created to simplify access to important metrics, such as average points per race by driver age group.

In conclusion, this project demonstrates the application of data engineering principles—such as database design, ETL pipelines, and data aggregation—using real-world Formula 1 data. In this way, this exercise is a comprehensive presentation of what we have learned in the first half of the course and I have tried to present it with a topic that is close to me personally.

## 2. DATASET DESCRICPTION

### 2.1 Context
As a Formula 1 enthusiast, I have always been intrigued by the complexity of the sport and the critical role that data analysis plays in optimizing race performance. From understanding driver performance trends to analyzing constructors’ strategies, the ability to derive insights from data has become central to F1 success. 

This project leverages a Formula 1 dataset sourced from [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) to explore key aspects of the sport through data engineering techniques.

### 2.2 Structure of the data

The dataset I used consists of six tables, each representing a critical component of the Formula 1 ecosystem:

1.	**t_drivers** – Contains driver information such as name, date of birth, nationality, and driver ID.
  	
2.	**t_constructors** – Includes constructor details like name, nationality, and constructor ID.

3.	**t_races** – Holds data about each race, including race name, date, circuit ID, and race ID.

4.	**t_circuits** – Stores information about the tracks where the races take place, such as location, country, and circuit ID.

5.	**t_race_results** – Contains the outcome of each race for each driver, including position, points, fastest lap data and race result status.

6.	**t_status_codes** – Contains the mapping of the specific status codes with the regarding status name (e.g., finished)

## 2.3 Variable description
The variables for each table and their description are grouped in the following matrix:

| Table            | Variables            | Description                                                                 |
|------------------|----------------------|-----------------------------------------------------------------------------|
| **t_drivers**      | driver_id            | Unique identifier for each driver (primary key).                            |
|                  | first_name           | The first name of the driver.                                               |
|                  | last_name            | The last name of the driver.                                                |
|                  | date_of_birth        | The driver's birthdate.                                                     |
|                  | driver_country       | The nationality of the driver.                                              |
| **t_constructors** | constructor_id       | Unique identifier for each constructor (primary key).                       |
|                  | constructor_name     | The name of the constructor (team).                                         |
|                  | constructor_country  | The country where the constructor is based.                                 |
| **t_races**        | race_id              | Unique identifier for each race (primary key).                              |
|                  | season               | The year of the race season.                                                |
|                  | season_round         | The round number within the season.                                         |
|                  | circuit_id           | Foreign key linking to the circuit where the race occurred.                 |
|                  | gp_name              | The official name of the Grand Prix.                                        |
|                  | gp_date              | The date when the race took place.                                          |
| **t_race_results** | result_id            | Unique identifier for each race result (primary key).                       |
|                  | race_id              | Foreign key linking to the race in which the result occurred.               |
|                  | driver_id            | Foreign key linking to the driver who competed.                             |
|                  | constructor_id       | Foreign key linking to the constructor (team) of the driver.                |
|                  | grid                 | The starting grid position of the driver.                                   |
|                  | position             | The final position of the driver in the race.                               |
|                  | points               | The points awarded to the driver for their performance.                     |
|                  | laps                 | The number of laps completed by the driver.                                 |
|                  | fastest_lap          | Indicates if the driver achieved the fastest lap in the race.               |
|                  | lap_rank             | Rank of the driver's fastest lap time in the race.                          |
|                  | fastest_lap_time     | The time of the driver's fastest lap.                                       |
|                  | fastest_lap_speed    | The speed achieved during the fastest lap.                                  |
|                  | status_id            | Foreign key linking to the driver's race status (e.g., finished, retired).  |
| **t_circuits**     | circuit_id           | Unique identifier for each circuit.                                         |
|                  | circuit_name         | The name of the circuit.                                                    |
|                  | location             | The location (city or region) where the circuit is located.                 |
|                  | country              | The country where the circuit is located.                                   |
|                  | number_of_turns      | The total number of turns in the circuit.                                   |
|                  | length_in_km         | The total length of the circuit in kilometers.                              |
| **t_status_codes** | status_id            | Unique identifier for each race status.                                     |
|                  | status_name          | The name or description of the race status (e.g., finished, retired).       |
|                  | status_category      | Category of the status (e.g., technical failure, collision, driver omission).|


These tables are interconnected through a set of primary and foreign key relationships. For instance, the Results table connects to the Drivers, Races, and Constructors tables via foreign keys to ensure data consistency across entities. The Races table is also linked to the Circuits table, allowing for a comprehensive view of race locations.

This relational structure enables rich data queries and analysis, allowing me to uncover patterns and insights related to driver performance, constructor success, and race outcomes.

> [!Note]
> The original dataset contained 14 tables, including data on qualifications, championships, and other aspects of Formula 1. However, for the purposes of this research, the focus has been limited to races only. As a result, the additional tables related to qualifying sessions, championship standings, and other data were not utilized in the analysis.


> [!Note]
> The load of race_results table may take a couple of minutes as it includes 26,519 records but the command should work properly.

## 3. OPERATIONAL LAYER (OLTP)

The operational layer of this project is designed to establish the foundational data structure for storing and managing Formula 1 race data. This part of the project sets up six essential tables, which will be populated with data imported from external CSV files. These tables include race_results, drivers, constructors, circuits, races, and status_codes. Each table is carefully constructed to align with the structure of the incoming data and ensure data consistency.

The operational layer begins by creating the following six tables:

1. race_results: Stores detailed results for each race, including race ID, driver ID, constructor ID, grid position, final position, points, and other performance metrics.

2. drivers: Contains driver information such as driver ID, forename, surname, date of birth, and nationality.

3. constructors: Holds constructor details, including constructor ID, name, and country of origin.

4. circuits: Stores information about racing circuits, including circuit ID, name, location, country, number of turns, and length.

5. races: Provides metadata for each race, including race ID, season, round number, circuit ID, Grand Prix name, and date.

6. status_codes: Includes status identifiers and descriptions, which indicate race outcomes (e.g., finished, retired).

Once the tables are set up, data is imported using the LOAD DATA INFILE command. This command loads data from CSV files directly into the respective tables, streamlining the process of populating the operational layer with raw data. The LOAD DATA INFILE statements specify the file paths, delimiters, and any necessary configuration to match the structure of the source data.

This approach ensures that the operational layer is prepared to hold comprehensive, organized data that forms the basis for further transformations and analysis in subsequent parts of the data pipeline. The structured setup of these six tables, followed by data importation, provides a reliable foundation for the ETL processes and analytical insights that follow.

> [!Note]
> Regarding the process of loading data into the previously created tables, please ensure that you adjust the file path to the correct location where the source data files are stored.

![OLTP diagram](/Term1/Resources/OLTP_diagram.png)

In the operational layer of the project, relationships between the tables are established to ensure data integrity and enforce referential constraints. This step involves adding foreign keys that link the primary keys of related tables, thereby creating a structured and interconnected data schema.

The relationships between the tables are essential for maintaining data consistency and enabling efficient querying. Here’s how the relationships are set up:

race_results Table:
• The raceID column is linked to the raceID in the races table through a foreign key. This ensures that each entry in race_results corresponds to an existing race.

• The driverID column is linked to the driverID in the drivers table, associating each race result with a specific driver.

• The constructorID column is linked to the constructorID in the constructors table, connecting race results with their corresponding constructors.

• The statusID column is linked to the statusID in the status_codes table, ensuring that each result has a valid status.

races Table:

• The circuitID column is linked to the circuitID in the circuits table, establishing the relationship between each race and the circuit it takes place on.

Foreign key constraints are added to enforce these relationships and ensure that data inserted into the tables adheres to the established schema. This prevents the creation of orphaned records and maintains data consistency across the database.

## 4. ANALYTICAL PLAN

Based on available variables and data quality, in the process of developing the research focus for my project, I structured my analysis around three key pillars:

- Driver Performance

- Constructor Performance

- Mechanical/Engineering Dimension

### 4.1 Driver Performance 

Focuses on the skills and consistency of individual drivers, exploring how various factors like age, experience, and starting position influence their results:

1. At which stage of a race are drivers most prone to making mistakes that lead to disqualifications or accidents?
2. How has the frequency of crashes or driver retirements changed over time? Have races become safer as the sport has evolved?
3. Which country has produced the most Formula 1 drivers, and which has produced the best performers?
4. What is the probability of the pole position driver winning the race based on historical data, and how does this vary across different circuits?
5. Is there a correlation between a driver's age and their performance? At what age do drivers typically reach their peak?

### 4.2 Constructor Performance
Analysing how teams, through strategic decisions and resource allocation, impact their performance in races and over seasons:

6. Do constructors show a preference for hiring drivers from their home country, and if so, how has this trend evolved over time?
7. On average, how many years does it take for a team to win its first Constructors’ Championship after debuting in Formula 1?
8. Which long-standing constructors (with more than 100 races) have shown the most consistency in terms of points gained over time, as measured by the lowest standard deviation in performance?
9. On average, how many race wins per season are required for a team to secure the Constructors’ Championship?
10. Which constructors have been the most dominant in the post-2010 era, measured by the number of 1-2 finishes they have achieved?

### 4.3 Mechanical/Engineering Dimension
Investigates how technology, car design, and other engineering related aspects contribute to race outcomes and team success:

11. Is there a correlation between specific technical failures (e.g., gearbox, engine issues) and particular race tracks? Are certain faults more common on specific circuits due to track characteristics such as layout, surface, or elevation changes?
12. What are the most frequent mechanical failures for each constructor?
13. Which constructors prioritize maximum straight-line speed during car development (based on maximum speed data)?
14. Do constructors design their cars more for high cornering performance (to win on tracks with more turns) or for higher top speeds on straights (based on maximum speed data from fastest laps)?
15. Which driver experienced the most mechanical failures in proportion to the number of races they participated in?

The analytical_layer provides a comprehensive dataset that includes all necessary variables and records, enabling in-depth analyses across various aspects of Formula 1 performance. This consolidated table integrates data from multiple sources, such as race results, driver details, constructors, and circuit information, to ensure that all essential metrics are readily available for analysis. With the support of a well-designed ETL pipeline, the analytical_layer remains accurate and up-to-date. The ETL pipeline automates data extraction, transformation, and loading, ensuring that new data is seamlessly incorporated and any changes in the source tables are reflected promptly. This setup guarantees that analysts have reliable, current data for conducting meaningful and detailed analyses, facilitating insights that drive strategic decisions.

> [!Note]
> The listed analytical questions are part of a theoretical research that will not be answered in this assignments. Although the materialized views and the simple views will answer 5 of these questions partiallly, further investigation and more in-depth analyses would be needed with more sophisticated tools (e.g., with python, R etc.).

## 5. ANALYTICAL LAYER

As for the data warehouse, I created a comprehensive table named analytical_layer that consolidates all the necessary columns and required information for analysis. This table integrates data from various operational tables, including race results, drivers, constructors, circuits, and status codes, to provide a complete view of each race’s outcome and context. By including enriched fields such as driver age at the time of the race and detailed race information (e.g., circuit name and constructor details), the analytical_layer facilitates seamless, efficient querying and serves as a robust foundation for further analysis and reporting.

![OLTP diagram](/Term1/Resources/Datawarehouse_dimensions.png)

The creation of the analytical_layer table was accomplished through a stored procedure that goes beyond simply joining data from different source tables. This procedure also incorporates data transformations to derive new columns that add valuable context to the dataset. For example, it calculates the Age column by computing the difference between a driver’s date of birth and the race date, providing insight into the age of the driver at each race. These transformations ensure that the table is enriched with detailed and meaningful data, supporting more efficient and insightful analysis without the need for additional processing at query time.

## 6. ETL PIPELINE

The ETL pipeline for this project was created using a stored procedure that automates the integration and transformation of data from the operational layer into the analytical_layer table. This stored procedure handles data extraction from multiple source tables, including race_results, drivers, constructors, circuits, and status_codes, ensuring that all relevant data points are combined seamlessly.

Beyond simply joining these tables, the procedure applies necessary transformations to enrich the dataset. For instance, it calculates derived columns such as Age, which is computed by finding the difference between a driver’s date of birth and the race date. This transformation adds valuable context, enabling more granular analysis and supporting advanced reporting needs.

The ETL process ensures that the analytical_layer table is always populated with up-to-date, integrated, and transformed data, facilitating efficient data queries and eliminating the need for complex on-the-fly data manipulations. The pipeline not only standardizes the data preparation but also enhances consistency and reliability, supporting data-driven insights and business decision-making with a streamlined and automated approach.

The ETL pipeline created for this project with a stored procedure efficiently applies all three dimensions of the ETL process: Extraction, Transformation, and Loading.

1. Extraction: The stored procedure begins by pulling data from multiple source tables within the operational layer, such as race_results, drivers, constructors, circuits, and status_codes. This ensures that all relevant raw data is retrieved and consolidated for further processing.

2. Transformation: The pipeline goes beyond simple data merging by applying various transformations to enrich the data. This includes calculating new columns such as Age, derived from the difference between the driver’s date of birth and the race date, adding a deeper level of analysis. The transformation step also standardizes and formats data to ensure consistency and accuracy across the dataset.

3. Loading: After the data has been extracted and transformed, it is loaded into the analytical_layer table, which serves as the comprehensive repository for all integrated and processed data. This table is structured to support efficient querying and reporting, eliminating the need for repeated data processing during analysis.

By incorporating all three dimensions of ETL, the stored procedure ensures that the data pipeline is robust, automated, and capable of delivering consistent, high-quality data to facilitate insights and business decision-making.

## 7. DATA MARTS

My assignment includes the development of 5 data marts, composed of 3 materialized views and 2 simple views, to support comprehensive analytics and provide valuable insights for potential research. These data marts are designed to align with the questions outlined in the analytical plan, ensuring that the analyses are targeted and relevant. Each data mart serves a specific purpose, facilitating easy access to critical metrics and trends for deeper exploration. Below is a description of these 5 data marts:

**Materialized Views:** 

mv_age_group_avg_points: This view calculates the average points scored by drivers in different age groups. It helps analyze performance trends based on driver age, giving insights into how experience and age might correlate with race outcomes.

mv_constructors_technical_failures: This data mart identifies the most frequently occurring technical failures for each constructor, along with the percentage of each issue relative to their total technical failures. It helps highlight reliability concerns and potential areas for engineering improvements.

mv_circuit_technical_failures: This data mart focuses on the most common mechanical issues at different circuits, showing the frequency and proportion of each problem in relation to all technical failures at those circuits. It provides insights into circuit-specific challenges that teams face.

**"Simple" Views:** 

pole_position_win_probability: This view displays the probability of a pole position driver (starting from Grid position 1) winning the race, broken down by circuit. It enables analysis of how circuit characteristics affect the likelihood of converting pole positions into race wins.

country_driver_statistics: This view shows which countries have produced the most Formula 1 drivers and which have the best-performing drivers based on total points. It offers a geographical perspective on driver representation and success.

These data marts are aligned with the analytical plan’s questions, providing targeted answers and supporting deeper research into performance patterns, reliability issues, and regional trends in Formula 1 racing. This structured approach ensures that the insights gained are both relevant and comprehensive, facilitating well-informed research and strategic decision-making.

## 8. Opportunities for future development

This assignment was developed in alignment with the requirements of this course, focusing on building a robust data infrastructure and data marts to support potential analyses. While it establishes a strong foundation by creating an analytical_layer and five data marts (three materialized views and two simple views), it does not delve into in-depth analysis. There are numerous opportunities for further development in the analytical component to comprehensively answer the research questions outlined in the analytical plan.

To fully leverage the data and gain deeper insights, the analytical part could be expanded to include more sophisticated analyses. This would involve using advanced statistical and data visualization tools such as Python or R. These programming environments offer powerful libraries for data manipulation, statistical analysis, machine learning, and visualization, enabling a more nuanced exploration of performance trends, reliability issues, and regional patterns in Formula 1 racing.

Enhancing the analytical component would allow for thorough examination of correlations, predictive modeling, and hypothesis testing, providing more conclusive and actionable insights. For instance, Python’s pandas and scikit-learn libraries or R’s tidyverse and caret packages could be utilized to perform regression analysis, clustering, and machine learning algorithms, effectively answering the research questions and supporting data-driven decision-making.

In summary, while this assignment meets the structural requirements of the course and establishes the groundwork for research, the next phase of development would be to incorporate advanced analytical techniques using Python or R to fully realize the potential of the data and answer each research question with greater depth and precision.
