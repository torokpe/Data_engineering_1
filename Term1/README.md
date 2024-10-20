# Data Engineering I. - Term Project I.

**Author:** Péter Bence Török

**Email:** Toeroek_Peter-Bence@student.ceu.edu

**Program:** Central European University (CEU) - Business Analytics Msc 2024/25

**Date of submission:** 03.11.2024

## Table of Contents
1. [EXECUTIVE SUMMARY](#executive-summary)
2. [DATASET DESCRIPTION](#dataset-description)
3. [OPERATIONAL LAYER](#operational-layer)
4. [ANALYTICAL PLAN](#analytical-plan)
5. [ANALYTICAL LAYER](#analytical-layer)
6. [ETL PIPELINE](#etl-pipeline)
7. [DATA MARTS](#data-marts)
8. [OPPORTUNITIES FOR FUTURE DEVELOPMENT](#opportunities-for-future-development)

## 1. EXECUTIVE SUMMARY

For my Data Engineering I. first term project, I utilized a Formula 1 (also  relational dataset sourced from [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) to analyze race performance, drivers, constructors, and circuit data. The dataset, containing historical F1 data, allowed for the creation of a comprehensive data pipeline and the exploration of key insights into Formula 1 racing.

The project began by designing and implementing a relational database schema in MySQL, where I created normalized tables to store information about races, drivers, constructors, and results. Relationships between entities were established through primary and foreign keys to ensure data integrity.

Next, I performed data transformation and denormalization tasks to build optimized structures for analytical queries. This included calculating derived fields such as driver age at the time of each race and aggregating race statistics to gain performance insights.

Additionally, I developed an ETL pipeline that automates data loading and transformation using SQL event triggers. This pipeline ensures that new data is processed efficiently and made available for reporting. Data marts and views were created to simplify access to important metrics, such as average points per race by driver age group.

Throughout the project, various testing strategies were employed to ensure the accuracy and performance of the database, including unit testing for database operations and performance testing for queries.

In conclusion, this project demonstrates the application of data engineering principles—such as database design, ETL pipelines, and data aggregation—using real-world Formula 1 data. It highlights the power of data engineering in turning raw datasets into meaningful insights that can be utilized for performance analysis in sports.

## 2. DATASET DESCRICPTION

### 2.1 Context
As a Formula 1 enthusiast, I have always been intrigued by the complexity of the sport and the critical role that data analysis plays in optimizing race performance. From understanding driver performance trends to analyzing constructors’ strategies, the ability to derive insights from data has become central to F1 success. 

This project leverages a Formula 1 dataset sourced from [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020) to explore key aspects of the sport through data engineering techniques.

### 2.2 Structure of the data

The dataset I used consists of six tables, each representing a critical component of the Formula 1 ecosystem:

1.	**drivers** – Contains driver information such as name, date of birth, nationality, and driver ID.
  	
2.	**constructors** – Includes constructor details like name, nationality, and constructor ID.

3.	**races** – Holds data about each race, including race name, date, circuit ID, and race ID.

4.	**circuits** – Stores information about the tracks where the races take place, such as location, country, and circuit ID.

5.	**race_results** – Contains the outcome of each race for each driver, including position, points, fastest lap data and race result status.

6.	**status_codes** – Contains the mapping of the specific status codes with the regarding status name (e.g., finished)

## 2.3 Variable description

The variables for each table and their description are grouped in the following matrix:

| Table            | Variables            | Description                                                                 |
|------------------|----------------------|-----------------------------------------------------------------------------|
| **drivers**       | driver_id            | Unique identifier for each driver (primary key).                            |
|                  | first_name           | The first name of the driver.                                               |
|                  | last_name            | The last name of the driver.                                                |
|                  | date_of_birth        | The driver's birthdate.                                                     |
|                  | driver_country       | The nationality of the driver.                                              |
| **constructors**  | constructor_id       | Unique identifier for each constructor (primary key).                       |
|                  | constructor_name     | The name of the constructor (team).                                         |
|                  | constructor_country  | The country where the constructor is based.                                 |
| **races**        | race_id              | Unique identifier for each race (primary key).                              |
|                  | season               | The year of the race season.                                                |
|                  | season_round         | The round number within the season.                                         |
|                  | circuit_id           | Foreign key linking to the circuit where the race occurred.                 |
|                  | gp_name              | The official name of the Grand Prix.                                        |
|                  | gp_date              | The date when the race took place.                                          |
| **race_results** | result_id            | Unique identifier for each race result (primary key).                       |
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
| **circuits**     | circuit_id           | Unique identifier for each circuit.                                         |
|                  | circuit_name         | The name of the circuit.                                                    |
|                  | location             | The location (city or region) where the circuit is located.                 |
|                  | country              | The country where the circuit is located.                                   |
|                  | number_of_turns      | The total number of turns in the circuit.                                   |
|                  | length_in_km         | The total length of the circuit in kilometers.                              |
| **status_codes** | status_id            | Unique identifier for each race status.                                     |
|                  | status_name          | The name or description of the race status (e.g., finished, retired).       |
|                  | status_category      | Category of the status (e.g., technical failure, collision, driver omission).|


These tables are interconnected through a set of primary and foreign key relationships. For instance, the Results table connects to the Drivers, Races, and Constructors tables via foreign keys to ensure data consistency across entities. The Races table is also linked to the Circuits table, allowing for a comprehensive view of race locations.

This relational structure enables rich data queries and analysis, allowing me to uncover patterns and insights related to driver performance, constructor success, and race outcomes. By analyzing this data, I aim to highlight the importance of data-driven decisions in the fast-paced and highly competitive world of Formula 1.

This description ties in your interest in F1 with a detailed explanation of the dataset structure and its relationships. Let me know if you’d like to tweak anything!

> [!Note]
> The original dataset contained 14 tables, including data on qualifications, championships, and other aspects of Formula 1. However, for the purposes of this research, the focus has been limited to races only. As a result, the additional tables related to qualifying sessions, championship standings, and other data were not utilized in the analysis.

## 3. OPERATIONAL LAYER

## 4. ANALYTICAL PLAN

In developing the research focus for my project, I structured my analysis around three key pillars: 

### 4.1 Driver Performance 
Focuses on the skills and consistency of individual drivers, exploring how various factors like age, experience, and starting position influence their results:
1. At which stage of the race do riders most often make a mistake that can lead to a disqualification or accident?
2. 


### 4.2 Constructor Performance
Analysing how teams, through strategic decisions and resource allocation, impact their performance in races and over seasons.

### 4.3 Mechanical/Engineering Dimension
Investigates how technology, car components, and upgrades contribute to race outcomes and team success:
1.	Is there a correlation between specific technical faults (e.g., gearbox failures, engine issues) and particular race tracks? In other words, are certain faults more likely to occur on specific tracks compared to others, possibly due to track characteristics such as layout, surface, or elevation changes?

These pillars represent the core aspects of Formula 1, where the synergy between human skill, team strategy, and mechanical reliability determines success on the track. By basing my analysis on these three elements, I aim to provide a comprehensive understanding of how individual and team efforts combine with engineering advancements to shape race outcomes.
## 5. ANALYTICAL LAYER

## 6. ETL PIPELINE

## 7. DATA MARTS




