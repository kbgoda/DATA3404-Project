CREATE TABLE ontimeperformance_flights_tiny_use (
	flight_id VARCHAR(100) DEFAULT NULL,
	carrier_code VARCHAR(50) DEFAULT NULL, 
--	REFERENCES ontimeperformance_airlines_use(carrier_code),
	flight_number INTEGER DEFAULT NULL, 
	flight_date DATE DEFAULT NULL, 	origin VARCHAR(50) DEFAULT NULL, 	destination VARCHAR(50) DEFAULT NULL, 	tail_number VARCHAR(50) DEFAULT NULL, 
	scheduled_departure_time TIME DEFAULT NULL, 
	scheduled_arrival_time TIME DEFAULT NULL,
	actual_departure_time TIME DEFAULT NULL,
	actual_arrival_time TIME DEFAULT NULL,
	distance INTEGER DEFAULT NULL	
);

CREATE TABLE ontimeperformance_airlines_use (
	carrier_code VARCHAR(50) DEFAULT NULL,
	name VARCHAR(100) DEFAULT NULL,
	country VARCHAR(50) DEFAULT NULL
);

DROP TABLE ontimeperformance_flights_tiny_use;
DROP TABLE ontimeperformance_airlines_use;


-- Query to check the joined table's delays
SELECT *, (F.actual_departure_time - F.scheduled_departure_time) AS delays 
FROM ontimeperformance_flights_tiny_use F
INNER JOIN ontimeperformance_airlines_use A ON F.carrier_code = A.carrier_code 
WHERE F.actual_departure_time > F.scheduled_departure_time 
AND EXTRACT(year FROM flight_date) = 2008
ORDER BY delays ASC;

-- Query to check flights
-- 010100011110
SELECT F.carrier_code, F.flight_date, F.scheduled_departure_time, F.scheduled_arrival_time, F.actual_departure_time, F.actual_arrival_time
FROM ontimeperformance_flights_tiny_use F
LIMIT 10;

-- Query to check airline names
SELECT A.carrier_code, A.name, A.country
FROM ontimeperformance_airlines_use A 
WHERE A.country = 'United States';

-- Query for flight delays and year filter
SELECT F.scheduled_departure_time, F.actual_departure_time 
FROM ontimeperformance_flights_tiny_use F
WHERE EXTRACT(year FROM flight_date) = 2007 AND 
F.actual_departure_time = F.scheduled_departure_time;
AND (actual_departure_time IS NOT NULL OR actual_arrival_time IS NOT NULL);

-- Query to check the joined table
SELECT * 
FROM ontimeperformance_flights_tiny_use F
INNER JOIN ontimeperformance_airlines_use A ON F.carrier_code = A.carrier_code 
WHERE A.name = 'Comair' 
AND F.actual_departure_time IS NOT NULL;

-- Query to convert time into minutes
-- Query to check the joined table
SELECT 
scheduled_departure_time, 
EXTRACT(hours from scheduled_departure_time * 60) + EXTRACT(minutes from scheduled_departure_time) AS minutes_format
FROM ontimeperformance_flights_tiny_use F
INNER JOIN ontimeperformance_airlines_use A ON F.carrier_code = A.carrier_code
LIMIT 5;

--Task 2: Average Departure Delay
-- Convert the select stuff to minutes left
SELECT 
A.name, 
COUNT(*) AS DELAYS, 
AVG(F.actual_departure_time - F.scheduled_departure_time) AS avg_delay, 
MIN(F.actual_departure_time - F.scheduled_departure_time) AS min_delay, 
MAX(F.actual_departure_time - F.scheduled_departure_time) AS max_delay
FROM ontimeperformance_flights_tiny_use F 
INNER JOIN ontimeperformance_airlines_use A ON F.carrier_code = A.carrier_code 
WHERE A.country = 'United States' AND EXTRACT(year FROM flight_date) = 2007 AND 
F.actual_departure_time > F.scheduled_departure_time
AND (actual_departure_time IS NOT NULL OR actual_arrival_time IS NOT NULL)
GROUP BY A.name;

--Task 2: Average Departure Delay
-- Final
SELECT 
A.name, 
COUNT(*) AS DELAYS, 

-- 18 mins
EXTRACT(hours from AVG(F.actual_departure_time - F.scheduled_departure_time) * 60) AS avg_delay,

-- 1 min
EXTRACT(hours from MIN(F.actual_departure_time - F.scheduled_departure_time)) + 
EXTRACT(minutes from MIN(F.actual_departure_time - F.scheduled_departure_time)) AS min_delay, 

-- 313 mins
EXTRACT(hours from MAX(F.actual_departure_time - F.scheduled_departure_time) * 60) AS max_delay

FROM ontimeperformance_flights_tiny_use F 
INNER JOIN ontimeperformance_airlines_use A ON F.carrier_code = A.carrier_code 
WHERE 
A.country = 'United States' AND EXTRACT(year FROM flight_date) = 2007 
AND F.actual_departure_time > F.scheduled_departure_time 
AND (actual_departure_time IS NOT NULL OR actual_arrival_time IS NOT NULL)
GROUP BY A.name;