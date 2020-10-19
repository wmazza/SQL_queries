CREATE OR REPLACE PROCEDURE populate_date_dim(IN start_timestamp timestamp, IN end_timestamp timestamp, updated_by VARCHAR(100))
language plpgsql
as $$
declare
   current_timestamp_value timestamp := start_timestamp;
   current_date_dim_pk bigint := -1;
   current_week_dim_pk bigint := -1;

   current_timestamp_value_date date := '1970-01-01 00:00:00';
   current_timestamp_value_week integer := -1;
   current_timestamp_value_weekyear integer := -1;
   current_timestamp_day_type varchar(7);
   
begin
	loop 
		exit when current_timestamp_value = end_timestamp;  
						
		-- INSERT IN DATE_DIM AND GET BACK DATE_DIM_PK GENERATED (ONLY IF NEW DAY)
		if current_timestamp_value_date != current_timestamp_value::date then

			current_timestamp_value_date := current_timestamp_value::date;

			-- INSERT IN WEEK_DIM AND GET BACK WEEK_DIM_PK GENERATED (ONLY IF NEW WEEK)
			if extract('week' from current_timestamp_value)::integer != current_timestamp_value_week then

				current_timestamp_value_week := extract('week' from current_timestamp_value)::integer;
				current_timestamp_value_weekyear := concat(extract('year' from current_timestamp_value), current_timestamp_value_week)::integer;

				insert into week_dim (
								 week_number   
								,year_week
								,week_dim_etl_updated_by
							) values 
							(
								 current_timestamp_value_week
								,current_timestamp_value_weekyear
								,updated_by
							)
							returning week_dim_pk into current_week_dim_pk;			
				COMMIT;
			end if;


			current_timestamp_day_type := (CASE WHEN EXTRACT(DOW FROM current_timestamp_value) IN (0,6)
												THEN 'WEEKEND'
												ELSE 'WEEKDAY'
										   END);

			insert into date_dim (
							 date_calendar
							,year_calendar
							,year_month
							,day_of_week
							,day_type
							,week_dim_fk 
							,week_number 
							,year_week 
							,date_dim_etl_updated_by
						) values 
						(
							 date_trunc('DAY', current_timestamp_value)
							,extract('YEAR' from current_timestamp_value)
							,extract('MONTH' from current_timestamp_value)
							,to_char(current_timestamp_value, 'DAY')
							,current_timestamp_day_type
							,current_week_dim_pk
							,current_timestamp_value_week
							,current_timestamp_value_weekyear
							,updated_by
						) 
						returning date_dim_pk into current_date_dim_pk;
			COMMIT;
		end if;

					
		-- CALL THE FUNCTION TO COMPUTE VALUES FOR TIME_DIM TABLE AND INSERT THEM
        call populate_time_dim(current_timestamp_value, 'Universal', current_date_dim_pk, current_timestamp_day_type, updated_by);
        call populate_time_dim(current_timestamp_value, 'America/Los_Angeles', current_date_dim_pk, current_timestamp_day_type, updated_by);
        call populate_time_dim(current_timestamp_value, 'America/New_York', current_date_dim_pk, current_timestamp_day_type, updated_by);
        call populate_time_dim(current_timestamp_value, 'America/Chicago', current_date_dim_pk, current_timestamp_day_type, updated_by);
		
		
		current_timestamp_value := current_timestamp_value + interval '1 second';
	end loop; 
end; 
$$;


