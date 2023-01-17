-- Q1

SELECT VendorID, PULocationID, tpep_pickup_datetime, Trip_distance, Total_amount, MAX(Tip_amount) as max_tip_amount
FROM Taxi_Trips
WHERE DOLocationID == BatteryParkID

-- Q2

SELECT MONTH(tpep_pickup_datetime) as trip_month, VendorID, PULocationID, tpep_pickup_datetime, Trip_distance, Total_amount, Tip_amount, MAX(Tolls_amount) as max_toll_amount
FROM Taxi_Trips
GROUP BY MONTH(tpep_pickup_datetime)
HAVING Tolls_amount > 0

-- Q3

CREATE FUNCTION HALF_OF_THE_MONTH(@DateValue as DATETIME) RETURNS INT AS 
    BEGIN
        IF DAY(@DateValue) > 15
            BEGIN
                SET @result 2
            END
        ELSE
            BEGIN
                SET @result 1
            END
        RETURN @result
    END



SELECT MONTH(tpep_pickup_datetime) as trip_month, HALF_OF_THE_MONTH(tpep_pickup_datetime) as half_of_the_month, AVERAGE(Trip_distance) as average_trip_distance
FROM Taxi_Trips
GROUP BY MONTH(tpep_pickup_datetime), HALF_OF_THE_MONTH(tpep_pickup_datetime)
HAVING PULocationID != DOLocationID

--  Q4
