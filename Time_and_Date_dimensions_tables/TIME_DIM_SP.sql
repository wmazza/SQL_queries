CREATE OR REPLACE PROCEDURE populate_time_dim(IN date_value_IN timestamp, IN timezone VARCHAR(50), IN DATE_DIM_FK BIGINT, IN DAY_TYPE VARCHAR(7), IN updated_by VARCHAR(100))
LANGUAGE plpgsql
AS $$
DECLARE
  	-- VARIABLES
    -- NOTE: date_value is the real value converted based on the timezone; all the columns are computed from it.
	date_value timestamp := (date_value_IN::timestamptz(0) at time zone timezone); 
	time_value time := date_value::time;
  	minute_value INTEGER := date_part('minute', date_value); 
	hour_value INTEGER := date_part('hour', date_value); 
	
	minute_value_first_digit_integer INTEGER := minute_value / 10;
	minute_value_second_digit_integer INTEGER := minute_value % 10;
	
	
	
	-- TABLE COLUMNS
    date_calendar_tz date := date_value::date;
	timestamp_second_tz timestamp := date_value;
	timestamp_minute_tz timestamp := date_trunc('minute', date_value);  
	timestamp_hour_tz timestamp := date_trunc('hour', date_value); 
	timestamp_5_minute_tz timestamp; 
	timestamp_15_minute_tz timestamp;
	timestamp_half_hour_tz timestamp;

	-- FOREIGN KEYS
	DAY_PART_DIM_FK integer;
	DAYPART_NAME varchar(20);
	
BEGIN

	SELECT DAYPART_DIM_PK, DAYPART_NAME
	INTO DAY_PART_DIM_FK, DAYPART_NAME
	FROM DAYPART_DIM
	WHERE 1=1
		AND time_value >= DAYPART_START_TIME 
		AND time_value <= DAYPART_END_TIME
                                

	-- COMPUTE timestamp_5_minute_tz
	CASE 
		WHEN minute_value_second_digit_integer < 5 THEN timestamp_5_minute_tz := date_trunc('minute', (date_value - minute_value_second_digit_integer * interval '1 minute'));
		WHEN minute_value_second_digit_integer > 5 THEN timestamp_5_minute_tz := date_trunc('minute', (date_value - (minute_value_second_digit_integer - 5) * interval '1 minute'));
		WHEN minute_value_second_digit_integer = 5 THEN timestamp_5_minute_tz := timestamp_minute_tz;
	END CASE;		
		
	-- COMPUTE timestamp_15_minute_tz
	CASE 
		WHEN minute_value >= 0 AND minute_value <= 14 THEN timestamp_15_minute_tz := date_value::date + timestamp_hour_tz::time;
		WHEN minute_value >= 15 AND minute_value <= 29 THEN timestamp_15_minute_tz := date_value::date + (timestamp_hour_tz::time + interval '15 minute');
		WHEN minute_value >= 30 AND minute_value <= 44 THEN timestamp_15_minute_tz := date_value::date + (timestamp_hour_tz::time + interval '30 minute');
		WHEN minute_value >= 45 AND minute_value <= 59 THEN timestamp_15_minute_tz := date_value::date + (timestamp_hour_tz::time + interval '45 minute');

	END CASE;	 

	-- COMPUTE timestamp_half_hour_tz
	CASE
		WHEN minute_value <= 29 THEN timestamp_half_hour_tz := date_value::date + timestamp_hour_tz::time;
		WHEN minute_value >= 30 THEN timestamp_half_hour_tz := date_value::date + (timestamp_hour_tz::time + interval '30 minute');
	END CASE;  

	INSERT INTO TIME_DIM (
		 TIMESTAMP_GMT --TIMESTAMP -- nat key
		,TIMEZONE --VARCHAR(50) -- 'Americas/Los_Angeles' define data type na
		,DATE_DIM_FK --BIGINT FOREIGN KEY REFERENCES DATE_DIM(DATE_DIM_PK)
		,DATE_CALENDAR_TZ --DATE
		,DAY_PART_DIM_FK --INTEGER FOREIGN KEY REFERENCES DAYPART_DIM(DAYPART_DIM_PK)
		,DAYPART_NAME  
		,DAY_TYPE
		,TIMESTAMP_SECOND_TZ --TIMESTAMP 
		,TIMESTAMP_MINUTE_TZ --TIMESTAMP 
		,TIMESTAMP_5_MINUTE_TZ --TIMESTAMP 
		,TIMESTAMP_15_MINUTE_TZ --TIMESTAMP
		,TIMESTAMP_HALF_HOUR_TZ --TIMESTAMP
		,TIMESTAMP_HOUR_TZ --TIMESTAMP
		--,TIME_DIM_ETL_CREATED_DATE TIMESTAMP DEFAULT GETDATE()
		--TIME_DIM_ETL_UPDATED_DATE TIMESTAMP
		,TIME_DIM_ETL_UPDATED_BY --VARCHAR(100)
		--TIME_DIM_ETL_BATCH_ID BIGINT
	)
	VALUES (
		 date_value_in
		,timezone
		,DATE_DIM_FK
		,date_calendar_tz
		,DAY_PART_DIM_FK
		,DAYPART_NAME  
		,DAY_TYPE
		,timestamp_second_tz
		,timestamp_minute_tz
		,timestamp_5_minute_tz
		,timestamp_15_minute_tz
		,timestamp_half_hour_tz
		,timestamp_hour_tz
		,updated_by
	);
		
END;
$$;