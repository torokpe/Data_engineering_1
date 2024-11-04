/*
*********************************************************************
Central European University Business Analytics MSC
Data Engineering 1: Term_project_1
*********************************************************************
Author: Péter Bence Török
Email: Toeroek_Peter-Bence@student.ceu.edu
*********************************************************************
*/

-- CREATING OPERATIONAL LAYER

-- Creating schema and setting it as the current schema
DROP SCHEMA IF EXISTS Formula1;  -- Drop existing schema if it exists to start fresh
CREATE SCHEMA Formula1;  -- Create a new schema named 'Formula1'
USE Formula1;  -- Set 'Formula1' as the active schema


-- Creating table for storing messages related to data events
CREATE TABLE messages 
(
    messageID INTEGER NOT NULL AUTO_INCREMENT,  -- Unique identifier for each message
    message VARCHAR(100) NOT NULL,  -- The content of the message
    date_of_event DATE NOT NULL,  -- The date of the event related to the message
    PRIMARY KEY(messageID)  -- Primary key on messageID
);

-- Creating the race_results table and setting resultID as the primary key
DROP TABLE IF EXISTS t_race_results;  -- Ensure the table doesn't already exist
CREATE TABLE t_race_results 
(
    resultID INTEGER NOT NULL,  -- Unique identifier for each race result
    raceID INTEGER NOT NULL,  -- Identifier for the race
    driverID INTEGER NOT NULL,  -- Identifier for the driver
    constructorID INTEGER NOT NULL,  -- Identifier for the constructor
    grid INTEGER,  -- Starting position on the grid
    position INTEGER,  -- Final race position
    points INTEGER,  -- Points earned in the race
    laps INTEGER,  -- Number of laps completed
    fastest_lap INTEGER,  -- Fastest lap number
    lap_rank INTEGER,  -- Rank of the fastest lap
    fastest_lap_time DOUBLE,  -- Time of the fastest lap
    fastest_lap_speed DOUBLE,  -- Speed of the fastest lap
    statusID INTEGER NOT NULL,  -- Status identifier (e.g., finished, retired)
    PRIMARY KEY(resultID)  -- Primary key on resultID
);

-- Creating the drivers table with driverID as the primary key
DROP TABLE IF EXISTS t_drivers;
CREATE TABLE t_drivers 
(
    driverID INTEGER NOT NULL,  -- Unique identifier for the driver
    forename VARCHAR(100) NOT NULL,  -- Driver's forename
    surname VARCHAR(100) NOT NULL,  -- Driver's surname
    date_of_birth DATE NOT NULL,  -- Driver's date of birth
    driver_country VARCHAR(100) NOT NULL,  -- Country of the driver
    PRIMARY KEY(driverID)  -- Primary key on driverID
);

-- Creating the constructors table with constructorID as the primary key
DROP TABLE IF EXISTS t_constructors;
CREATE TABLE t_constructors
(
    constructorID INTEGER NOT NULL,  -- Unique identifier for the constructor
    constructor_name VARCHAR(100) NOT NULL,  -- Name of the constructor
    constructor_country VARCHAR(100) NOT NULL,  -- Country of the constructor
    PRIMARY KEY(constructorID)  -- Primary key on constructorID
);

-- Creating the circuits table with circuitID as the primary key
DROP TABLE IF EXISTS t_circuits;
CREATE TABLE t_circuits
(
    circuitID INTEGER NOT NULL,  -- Unique identifier for the circuit
    circuit_name VARCHAR(100) NOT NULL,  -- Name of the circuit
    location VARCHAR(100) NOT NULL,  -- Location of the circuit
    country VARCHAR(100) NOT NULL,  -- Country where the circuit is located
    number_of_turns INTEGER NOT NULL,  -- Number of turns in the circuit
    length DOUBLE NOT NULL,  -- Length of the circuit
    PRIMARY KEY(circuitID)  -- Primary key on circuitID
);

-- Creating the status_codes table with statusID as the primary key
DROP TABLE IF EXISTS t_status_codes;
CREATE TABLE t_status_codes
(
    statusID INTEGER NOT NULL,  -- Unique identifier for the status
    status_name VARCHAR(100) NOT NULL,  -- Name of the status (e.g., finished, retired)
    status_category VARCHAR(100) NOT NULL,  -- Category of the status (e.g., completed, not completed)
    PRIMARY KEY(statusID)  -- Primary key on statusID
);

-- Creating the races table with raceID as the primary key
DROP TABLE IF EXISTS t_races;
CREATE TABLE t_races
(
    raceID INTEGER NOT NULL,  -- Unique identifier for the race
    season INTEGER NOT NULL,  -- Season of the race
    season_round INTEGER NOT NULL,  -- Round number of the race in the season
    circuitID INTEGER NOT NULL,  -- Identifier for the circuit
    gp_name VARCHAR(100) NOT NULL,  -- Name of the Grand Prix
    gp_date DATE NOT NULL,  -- Date of the Grand Prix
    PRIMARY KEY(raceID)  -- Primary key on raceID
);


-- Setting up the environment for secure file handling and data loading
SET GLOBAL local_infile=ON;  -- Enable local file loading
SHOW VARIABLES LIKE "secure_file_priv";  -- Check secure file location path
SHOW VARIABLES LIKE "local_infile";  -- Check if local infile is enabled


-- Loading data into tables from CSV files
-- Note: Ensure the paths provided have proper permissions for reading files
LOAD DATA INFILE '/path/to/race_results.csv' 
INTO TABLE t_race_results 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(ResultID, RaceID, DriverID, ConstructorID, Grid, Position, Points, Laps, Fastest_lap, Lap_rank, Fastest_lap_time, Fastest_lap_speed, StatusID);

LOAD DATA INFILE '/path/to/drivers.csv'
INTO TABLE t_drivers 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(DriverID, Forename, Surname, Date_of_birth, Driver_country);

LOAD DATA INFILE '/path/to/constructors.csv'
INTO TABLE t_constructors 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(ConstructorID, Constructor_name, Constructor_country);

LOAD DATA INFILE '/path/to/circuits.csv'
INTO TABLE t_circuits 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(CircuitID, Circuit_name, Location, Country, Number_of_turns, Circuit_length);

LOAD DATA INFILE '/path/to/status_codes.csv'
INTO TABLE t_status_codes 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(StatusID, Status_name, Status_category);

LOAD DATA INFILE '/path/to/races.csv'
INTO TABLE t_races 
FIELDS TERMINATED BY ';' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES 
(RaceID, Season, Season_round, CircuitID, GP_name, GP_date);


-- Disabling checks temporarily for adding foreign keys
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- Adding indexes for optimizing joins and queries
ALTER TABLE `Formula1`.`t_race_results` 
ADD INDEX `raceID_idx` (`raceID` ASC) VISIBLE,
ADD INDEX `driverID_idx` (`driverID` ASC) VISIBLE,
ADD INDEX `statusID_idx` (`statusID` ASC) VISIBLE,
ADD INDEX `constructorID_idx` (`constructorID` ASC) VISIBLE;

ALTER TABLE `Formula1`.`t_races` 
ADD INDEX `circuitID_idx` (`circuitID` ASC) VISIBLE;

-- Adding foreign key constraints to ensure data consistency across related tables
ALTER TABLE `Formula1`.`t_race_results` 
ADD CONSTRAINT `raceID`
  FOREIGN KEY (`raceID`)
  REFERENCES `Formula1`.`t_races` (`raceID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `driverID`
  FOREIGN KEY (`driverID`)
  REFERENCES `Formula1`.`t_drivers` (`driverID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `statusID`
  FOREIGN KEY (`statusID`)
  REFERENCES `Formula1`.`t_status_codes` (`statusID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION,
ADD CONSTRAINT `constructorID`
  FOREIGN KEY (`constructorID`)
  REFERENCES `Formula1`.`t_constructors` (`constructorID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;

ALTER TABLE `Formula1`.`t_races` 
ADD CONSTRAINT `circuitID`
  FOREIGN KEY (`circuitID`)
  REFERENCES `Formula1`.`t_circuits` (`circuitID`)
  ON DELETE NO ACTION
  ON UPDATE NO ACTION;

-- Restoring the original environment settings
SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


-- CREATING DENORMALIZED DATA TABLE
-- This stored procedure creates a denormalized table for easier analysis
DROP PROCEDURE IF EXISTS Create_Analytical_Layer;  -- Remove the procedure if it already exists

DELIMITER //

CREATE PROCEDURE Create_Analytical_Layer()
BEGIN
    -- Drop the existing analytical layer table to ensure a fresh start
    DROP TABLE IF EXISTS analytical_layer;

    -- Create a new denormalized table with aggregated data from related tables
    CREATE TABLE analytical_layer AS
    SELECT 
        t_race_results.resultID AS ResultID,  -- Unique race result identifier
        t_races.gp_name AS GP_Name,  -- Name of the Grand Prix
        t_races.season AS Season,  -- Season year of the race
        t_races.gp_date AS GP_Date,  -- Date of the race
        t_circuits.circuit_name AS Circuit,  -- Circuit name
        t_circuits.country AS Location,  -- Circuit location (country)
        CONCAT(t_drivers.forename, ' ', t_drivers.surname) AS Driver,  -- Full driver name
        t_drivers.date_of_birth AS Date_of_birth,  -- Driver's date of birth
        TIMESTAMPDIFF(YEAR, t_drivers.date_of_birth, t_races.gp_date) AS Age,  -- Age of the driver at the time of the race
        t_drivers.driver_country AS Nationality,  -- Driver's nationality
        t_constructors.constructor_name AS Constructor,  -- Name of the constructor
        t_constructors.constructor_country AS Country_of_origin,  -- Constructor's country of origin
        t_race_results.grid AS Grid_position,  -- Starting grid position
        t_race_results.position AS Position,  -- Final race position
        t_race_results.points AS Points,  -- Points earned in the race
        t_status_codes.status_name AS Status,  -- Status of the race result (e.g., finished, retired)
        t_status_codes.status_category AS Status_category  -- Category of the race status (e.g., completed, not completed)
    FROM
        t_race_results
    INNER JOIN
        t_races USING (raceID)  -- Join with races table
    INNER JOIN
        t_circuits USING (circuitID)  -- Join with circuits table
    INNER JOIN
        t_drivers USING (driverID)  -- Join with drivers table
    INNER JOIN
        t_constructors USING (constructorID)  -- Join with constructors table
    INNER JOIN
        t_status_codes USING (statusID)  -- Join with status_codes table
    ORDER BY
        ResultID;  -- Order the results by ResultID for better readability

END //
DELIMITER ;

-- Call the procedure to create the denormalized analytical layer table
CALL Create_Analytical_Layer();


-- CREATING ETL PIPELINE USING TRIGGER
-- This trigger automatically updates the analytical layer table when a new record is inserted into race_results
DROP TRIGGER IF EXISTS Update_Analytical_Layer;  -- Remove the trigger if it already exists

DELIMITER $$

CREATE TRIGGER Update_Analytical_Layer
AFTER INSERT ON t_race_results  -- Trigger fires after a new row is inserted into race_results
FOR EACH ROW
BEGIN
    -- Log a message indicating a new result has been added
    INSERT INTO messages (message)
    SELECT CONCAT('New ResultID: ', NEW.resultID);

    -- Insert the new data into the analytical layer table
    INSERT INTO analytical_layer
        (ResultID, GP_Name, Season, GP_Date, Circuit, Location, Driver, Date_of_birth, Age, Nationality, Constructor, Country_of_origin, Grid_position, Position, Points, Status, Status_category)
    SELECT 
        NEW.resultID AS ResultID, 
        t_races.gp_name AS GP_Name, 
        t_races.season AS Season,
        t_races.gp_date AS GP_Date,
        t_circuits.circuit_name AS Circuit,
        t_circuits.country AS Location,
        CONCAT(t_drivers.forename, ' ', t_drivers.surname) AS Driver,
        t_drivers.date_of_birth AS Date_of_birth,
        TIMESTAMPDIFF(YEAR, t_drivers.date_of_birth, t_races.gp_date) AS Age,
        t_drivers.driver_country AS Nationality,
        t_constructors.constructor_name AS Constructor,
        t_constructors.constructor_country AS Country_of_origin,
        NEW.grid AS Grid_position,
        NEW.position AS Position,
        NEW.points AS Points,
        t_status_codes.status_name AS Status,
        t_status_codes.status_category AS Status_category
    FROM
        t_races
    INNER JOIN
        t_circuits ON t_races.circuitID = t_circuits.circuitID
    INNER JOIN
        t_drivers ON NEW.driverID = drivers.driverID
    INNER JOIN
        t_constructors ON NEW.constructorID = t_constructors.constructorID
    INNER JOIN
        t_status_codes ON NEW.statusID = status_codes.statusID
    WHERE NEW.raceID = t_races.raceID;  -- Match the raceID for the newly inserted row
END $$

DELIMITER ;


-- DATA MARTS WITH MATERIALIZED VIEWS
-- Creating a table to analyze average points by age group and setting up an event to refresh it periodically
CREATE TABLE mv_age_group_avg_points AS
SELECT 
    CASE 
        WHEN Age BETWEEN 18 AND 20 THEN '18-20'
        WHEN Age BETWEEN 21 AND 25 THEN '21-25'
        WHEN Age BETWEEN 26 AND 30 THEN '26-30'
        WHEN Age BETWEEN 31 AND 35 THEN '31-35'
        WHEN Age BETWEEN 36 AND 40 THEN '36-40'
        WHEN Age BETWEEN 41 AND 45 THEN '41-45'
        ELSE '45+' 
    END AS age_group,  -- Categorize drivers into age groups
    AVG(Points) AS avg_points  -- Calculate the average points for each age group
FROM 
    analytical_layer
GROUP BY
    age_group  -- Group by age group
ORDER BY 
    age_group;  -- Order the results by age group


-- Creating an event to refresh the mv_age_group_avg_points table every 12 hours
SET GLOBAL event_scheduler = ON; -- Enabling event scheduler


DELIMITER //

CREATE EVENT refresh_mv_age_group_avg_points
ON SCHEDULE EVERY 12 HOUR  -- Schedule the event to run every 12 hours
DO
BEGIN
    -- Clear the table data
    TRUNCATE TABLE mv_age_group_avg_points; 
    
    -- Insert new data into the table
    INSERT INTO mv_age_group_avg_points
    SELECT 
        CASE 
            WHEN Age BETWEEN 18 AND 20 THEN '18-20'
            WHEN Age BETWEEN 21 AND 25 THEN '21-25'
            WHEN Age BETWEEN 26 AND 30 THEN '26-30'
            WHEN Age BETWEEN 31 AND 35 THEN '31-35'
            WHEN Age BETWEEN 36 AND 40 THEN '36-40'
            WHEN Age BETWEEN 41 AND 45 THEN '41-45'
            ELSE '45+'  
        END AS age_group,  -- Categorize drivers into age groups
        AVG(Points) AS avg_points  -- Calculate the average points for each age group
    FROM 
        analytical_layer
    GROUP BY
        age_group  -- Group by age group
    ORDER BY 
        age_group;  -- Order the results by age group
END;
//

DELIMITER ;


-- Create a materialized view to see the most common mechanical issues by circuit from the analytical_layer table

-- Create a table to show only the most frequent technical failure for each circuit and its percentage of total failures
CREATE TABLE mv_circuit_technical_failures AS
SELECT 
    Circuit AS circuit_name,
    Status AS most_frequent_mechanical_issue,
    issue_count,
    ROUND((issue_count * 100.0 / total_failures), 2) AS percentage_of_total_technical_failures
FROM (
    SELECT 
        Circuit,
        Status,
        COUNT(*) AS issue_count,
        SUM(COUNT(*)) OVER (PARTITION BY Circuit) AS total_failures,
        ROW_NUMBER() OVER (PARTITION BY Circuit ORDER BY COUNT(*) DESC) AS ranking
    FROM 
        analytical_layer
    WHERE 
        Status_category = 'Technical failure'
    GROUP BY 
        Circuit, Status
) AS ranked_issues
WHERE 
    ranking = 1  -- Only include the most frequent issue for each circuit
ORDER BY 
    Circuit, issue_count DESC;
 
-- Creating an event to refresh the mv_circuit_technical_failures table every 12 hours
DELIMITER //

CREATE EVENT refresh_mv_circuit_technical_failures
ON SCHEDULE EVERY 12 HOUR  -- Schedule the event to run every 12 hours
DO
BEGIN
  
TRUNCATE TABLE mv_circuit_technical_failures; -- Clear the table data

INSERT INTO mv_circuit_technical_failures -- Insert new data into the table

SELECT 
    Circuit AS circuit_name,
    Status AS most_frequent_mechanical_issue,
    issue_count,
    ROUND((issue_count * 100.0 / total_failures), 2) AS percentage_of_total_technical_failures
FROM (
    SELECT 
        Circuit,
        Status,
        COUNT(*) AS issue_count,
        SUM(COUNT(*)) OVER (PARTITION BY Circuit) AS total_failures,
        ROW_NUMBER() OVER (PARTITION BY Circuit ORDER BY COUNT(*) DESC) AS ranking
    FROM 
        analytical_layer
    WHERE 
        Status_category = 'Technical failure'
    GROUP BY 
        Circuit, Status
) AS ranked_issues
WHERE 
    ranking = 1  -- Only include the most frequent issue for each circuit
ORDER BY 
    Circuit, issue_count DESC;
END;
//

DELIMITER ;


-- Create a materialized view to see the most common mechanical issues by constructors from the analytical_layer table

-- Create a table to show only the most frequent technical failure for each constructor and its percentage of total failures
CREATE TABLE mv_constructors_technical_failures AS
SELECT 
    Constructor AS Constructor_name,
    Status AS most_frequent_mechanical_issue,
    issue_count,
    ROUND((issue_count * 100.0 / total_failures), 2) AS percentage_of_total_technical_failures
FROM (
    SELECT 
        Constructor,
        Status,
        COUNT(*) AS issue_count,
        SUM(COUNT(*)) OVER (PARTITION BY Constructor) AS total_failures,
        ROW_NUMBER() OVER (PARTITION BY Constructor ORDER BY COUNT(*) DESC) AS ranking
    FROM 
        analytical_layer
    WHERE 
        Status_category = 'Technical failure'
    GROUP BY 
        Constructor, Status
) AS ranked_issues
WHERE 
    ranking = 1  -- Only include the most frequent issue for each constructor
ORDER BY 
    Constructor, issue_count DESC;


-- Creating an event to refresh the mv_constructors_technical_failures table every 12 hours
DELIMITER //

CREATE EVENT refresh_mv_constructors_technical_failures
ON SCHEDULE EVERY 12 HOUR  -- Schedule the event to run every 12 hours
DO
BEGIN
  
TRUNCATE TABLE mv_constructors_technical_failures; -- Clear the table data

INSERT INTO mv_constructors_technical_failures -- Insert new data into the table

SELECT 
    Constructor AS Constructor_name,
    Status AS most_frequent_mechanical_issue,
    issue_count,
    ROUND((issue_count * 100.0 / total_failures), 2) AS percentage_of_total_technical_failures
FROM (
    SELECT 
        Constructor,
        Status,
        COUNT(*) AS issue_count,
        SUM(COUNT(*)) OVER (PARTITION BY Constructor) AS total_failures,
        ROW_NUMBER() OVER (PARTITION BY Constructor ORDER BY COUNT(*) DESC) AS ranking
    FROM 
        analytical_layer
    WHERE 
        Status_category = 'Technical failure'
    GROUP BY 
        Constructor, Status
) AS ranked_issues
WHERE 
    ranking = 1  -- Only include the most frequent issue for each constructor
ORDER BY 
    Constructor, issue_count DESC;
END;
//

DELIMITER ;


-- Creating a view to see what is the probability of the pole position driver winning at different circuits
-- Create a view to show the probability of a pole position driver winning the race for each circuit
CREATE VIEW pole_position_win_probability AS
SELECT 
    Circuit,
    (COUNT(CASE WHEN Grid_position = 1 AND Position = 1 THEN 1 END) * 100.0) / 
    COUNT(CASE WHEN Grid_position = 1 THEN 1 END) AS win_probability_percentage
FROM 
    analytical_layer
GROUP BY 
    Circuit
ORDER BY 
    win_probability_percentage DESC;


-- Creating a view to see which country has produced the most Formula 1 drivers and average points by countries
CREATE VIEW country_driver_statistics AS
SELECT 
    Nationality AS country,
    COUNT(DISTINCT Driver) AS total_drivers_produced,
    SUM(Points) AS total_points_scored,
    AVG(Points) AS average_points_per_driver
FROM 
    analytical_layer
GROUP BY 
    Nationality
ORDER BY 
    total_drivers_produced DESC, total_points_scored DESC;
    
-- Displaying key tables and views to see result
select * from analytical_layer;
select * from mv_age_group_avg_points;
select * from mv_circuit_technical_failures;
select * from mv_constructors_technical_failures;
select * from pole_position_win_probability;
select * from country_driver_statistics;