/****** Object:  StoredProcedure [PII_SEARCH_VALUE] ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

/*
-- =============================================
-- Author:		<Mazza, William Andrea>
-- Create date: <2020-06-08>
-- Description:	<Procedure to search databases tables and columns for customer PII requests>
-- =============================================
*/

CREATE PROCEDURE [dbo].[PII_SEARCH_VALUE]

	-- Add the parameters for the stored procedure here
	 @inpExecMode AS VARCHAR(50)
	,@inpName AS VARCHAR(100)
	,@inpEmail AS VARCHAR(100)
	,@inpPhone AS VARCHAR(20)

AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

    -- Insert statements for procedure here
	DECLARE @NameCheck CHAR(1) = '0'
	       ,@EmailCheck CHAR(1) = '0'
		   ,@PhoneCheck CHAR(1) = '0'

		   ,@PII_DATA_FLAG NVARCHAR(100)
		   ,@TotalCursorRows NUMERIC(18,0)

		   ,@PII_Table_Columns_Pk NUMERIC(10,0)
		   ,@PII_Database_Name NVARCHAR(50)
		   ,@PII_Table_Name NVARCHAR(50)
		   ,@PII_Column_Name NVARCHAR(50)
		   ,@PII_Column_Data_Flag CHAR(1)
		   ,@PII_Column_Data_Flag_Subtype NVARCHAR(10)
		   ,@PII_Has_Customer_Data CHAR(1)
		   ,@PII_Select_Record_SQL_Statement NVARCHAR(4000)


	--Check NAME
	IF @inpName IS NOT NULL AND @inpName != ''
	BEGIN
	   SET @NameCheck = '1'
	END

	--Check EMAIL
	IF @inpEmail IS NOT NULL AND @inpEmail != ''
	BEGIN
	   SET @EmailCheck = '1'
	END 

	--Check PHONE
	IF @inpPhone IS NOT NULL AND @inpPhone != ''
	BEGIN
	   SET @PhoneCheck = '1'
	END 

	-- Create Cursor
	DECLARE @Cursor as CURSOR;
	
	BEGIN
		SET @Cursor = CURSOR FOR
		SELECT 
		   [PII_TABLE_COLUMNS_PK]
		  ,[PII_DATABASE_NAME]
		  ,[PII_TABLE_NAME]
		  ,[PII_COLUMN_NAME]
		  ,[PII_COLUMN_DATA_FLAG]
		  ,[PII_COLUMN_DATA_FLAG_SUBTYPE]
		  ,[PII_HAS_CUSTOMER_DATA]
		FROM dbo.PII_TABLE_COLUMNS
		WHERE 1=1
			AND PII_HAS_CUSTOMER_DATA = 'Y';
		  

		OPEN @Cursor;
		SET @TotalCursorRows = @@CURSOR_ROWS;

		FETCH NEXT FROM @Cursor INTO @PII_Table_Columns_Pk, @PII_Database_Name, @PII_Table_Name, @PII_Column_Name, @PII_Column_Data_Flag, @PII_Column_Data_Flag_Subtype, @PII_Has_Customer_Data;

		DECLARE  
				--Search variables
				 @SQL_Search_Statement NVARCHAR(4000)
				,@SearchCountHits INT
				,@ParmDefinition NVARCHAR(500)
				,@SearchFlag CHAR(1) = 'N'

				--Update variables
				,@SQL_Update_Statement NVARCHAR(4000)
				,@UpdatedDate NVARCHAR(20) = ''

		
		WHILE @@FETCH_STATUS = 0
		BEGIN
		
			IF @NameCheck = '1' AND @PII_Column_Data_Flag_Subtype != 'FIRST_NAME'
			BEGIN
				
				IF @PII_Column_Data_Flag_Subtype = 'FULL_NAME'
				BEGIN
					
					SET @SQL_Search_Statement = N'SELECT @COUNT_OUT = COUNT(*)' +
											      ' FROM [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']' +
											     ' WHERE ' + @PII_Column_Name + ' = @NAME'+ ';'
					
					SET @PII_Select_Record_SQL_Statement = N'SELECT *' +
											      ' FROM ' + @PII_Database_Name + '.dbo.' + @PII_Table_Name + 
											      ' WHERE ' + @PII_Column_Name + ' = ' + CHAR(39) + @inpName + CHAR(39) + ';'
				
				END
				ELSE IF @PII_Column_Data_Flag_Subtype = 'LAST_NAME'
				BEGIN
					
					SET @SQL_Search_Statement = N'SELECT @COUNT_OUT = COUNT(*) ' +
											      'FROM [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']
											      WHERE CONCAT(' + @PII_Column_Name + ',' + char(39) + char(32) + char(39) + ', ' + REPLACE(@PII_Column_Name, 'LAST_NAME', 'FIRST_NAME') + ') = @NAME'+ ';'

					SET @PII_Select_Record_SQL_Statement = N'SELECT *' +
											      ' FROM ' + @PII_Database_Name + '.dbo.' + @PII_Table_Name + 
											      ' WHERE CONCAT(' + @PII_Column_Name + ',' + char(39) + char(32) + char(39) + ', ' + REPLACE(@PII_Column_Name, 'LAST_NAME', 'FIRST_NAME') + ') = ' + CHAR(39) + @inpName + CHAR(39) + ';'
				END

				SET @ParmDefinition = N'@COUNT_OUT INT OUTPUT
									   ,@NAME VARCHAR(100)'										

				EXEC Sp_executesql 
					 @SQL_Search_Statement
					,@ParmDefinition
					,@NAME = @inpName
					,@COUNT_OUT = @SearchCountHits OUTPUT
				
				IF(@SearchCountHits > 0) 
				BEGIN
					
					SET @SearchFlag = 'Y'
				
					INSERT INTO [ETL_Stage].[dbo].[PII_SEARCH_RESULTS] (
													--PII_SEARCH_RESULTS_PK       --BIGINT IDENTITY(1,1) PRIMARY KEY
													--,PII_SEARCH_DATE            --DATETIME2(0) DEFAULT(GETDATE())
													 PII_TABLE_COLUMNS_FK         --BIGINT FOREIGN KEY REFERENCES dbo.PII_TABLE_COLUMNS(PII_TABLE_COLUMNS_PK)                                
													,PII_SEARCH_VALUE             --NVARCHAR(255)
													,PII_SEARCH_RESULT_FLAG       --CHAR(1) -- Y, search found, N value not found
													,PII_SEARCH_RECORDS_FOUND     --INT -- count of records found matching search value
													,PII_SEARCH_SQL_STATEMENT     --NVARCHAR(4000)
													--,PII_UPDATE_SQL_STATEMENT   --NVARCHAR(4000)
													--,PII_COLUMN_UPDATED_FLAG      --CHAR(1) DEFAULT('N') -- Y updated
													,PII_SEARCH_RESULTS_MODIFIED_DATE  --DATETIME2(0)
													,PII_SELECT_RECORD_SQL_STATEMENT -- NVARCHAR(4000) 

													) VALUES (
													 @PII_Table_Columns_Pk
													,@inpName
													,@SearchFlag
													,@SearchCountHits
													,@SQL_Search_Statement
													,GETDATE()
													,@PII_Select_Record_SQL_Statement
													)	
				END															  				
			END
															  	

			IF @EmailCheck = '1'  
			BEGIN
				
				SET @SQL_Search_Statement = N'SELECT @COUNT_OUT = COUNT(*)
											  FROM [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']
											  WHERE ' + @PII_Column_Name + ' = @EMAIL'+ ';'
			
				SET @ParmDefinition = N'@COUNT_OUT INT OUTPUT										
									   ,@EMAIL VARCHAR(100)'

				EXEC Sp_executesql 
					 @SQL_Search_Statement
					,@ParmDefinition
					,@EMAIL = @inpEmail
					,@COUNT_OUT = @SearchCountHits OUTPUT
				
				IF(@SearchCountHits > 0) 
				BEGIN
					SET @SearchFlag = 'Y'
					SET @SQL_Update_Statement = N'UPDATE [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']
											      SET ' + @PII_Column_Name + ' = ' + char(39) + 'PII_' + CAST(CAST(GETDATE() AS DATE) AS VARCHAR(10)) + char(39) + '
										          WHERE ' + @PII_Column_Name + ' = ' + char(39) + @inpEmail + char(39) + ';'
				
					INSERT INTO [ETL_Stage].[dbo].[PII_SEARCH_RESULTS] (
													--PII_SEARCH_RESULTS_PK       --BIGINT IDENTITY(1,1) PRIMARY KEY
													--,PII_SEARCH_DATE            --DATETIME2(0) DEFAULT(GETDATE())
													 PII_TABLE_COLUMNS_FK         --BIGINT FOREIGN KEY REFERENCES dbo.PII_TABLE_COLUMNS(PII_TABLE_COLUMNS_PK)                                
													,PII_SEARCH_VALUE             --NVARCHAR(255)
													,PII_SEARCH_RESULT_FLAG       --CHAR(1) -- Y, search found, N value not found
													,PII_SEARCH_RECORDS_FOUND     --INT -- count of records found matching search value
													,PII_SEARCH_SQL_STATEMENT     --NVARCHAR(4000)
													,PII_UPDATE_SQL_STATEMENT     --NVARCHAR(4000)
													--,PII_COLUMN_UPDATED_FLAG      --CHAR(1) DEFAULT('N') -- Y updated
													,PII_SEARCH_RESULTS_MODIFIED_DATE  --DATETIME2(0)
													,PII_SELECT_RECORD_SQL_STATEMENT -- NVARCHAR(4000) 

													) VALUES (
													 @PII_Table_Columns_Pk
													,@inpEmail
													,@SearchFlag
													,@SearchCountHits
													,@SQL_Search_Statement
													,@SQL_Update_Statement
													,GETDATE()
													,@PII_Select_Record_SQL_Statement 
													)
				END			
			END
																	  									  
													 
			IF @PhoneCheck = '1' 
			BEGIN
				SET @SQL_Search_Statement = N'SELECT @COUNT_OUT = COUNT(*)
												  FROM [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']
												  WHERE ' + @PII_Column_Name + ' = @PHONE'+ ';'
			
                SET @ParmDefinition = N'@COUNT_OUT INT OUTPUT										
                                        ,@PHONE VARCHAR(100)'

                EXEC Sp_executesql 
                        @SQL_Search_Statement
                    ,@ParmDefinition
                    ,@PHONE = @inpPhone
                    ,@COUNT_OUT = @SearchCountHits OUTPUT
            
                IF(@SearchCountHits > 0) 
                BEGIN
                    

                    SET @SearchFlag = 'Y'	
                    IF COL_LENGTH('[' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']', @PII_Table_Name + '_ETL_UPDATED_DATE') IS NOT NULL
                    BEGIN
                        SET @UpdatedDate = ', ' + @PII_Column_Name + ']_ETL_UPDATED_DATE = ' + char(39) + CAST(CAST(GETDATE() AS DATE) AS VARCHAR(10)) + char(39)
                    END		
                    

                    SET @SQL_Update_Statement = N'UPDATE [' + @PII_Database_Name + '].[dbo].[' + @PII_Table_Name + ']
                                                SET ' + @PII_Column_Name + ' = ' + char(39) + 'PII_' + CAST(CAST(GETDATE() AS DATE) AS VARCHAR(10)) + char(39) + 
                                                + @UpdatedDate + 
                                                'WHERE ' + @PII_Column_Name + ' = ' + char(39) + @inpPhone + char(39) + ';' 
            
                    INSERT INTO [ETL_Stage].[dbo].[PII_SEARCH_RESULTS] (
                                                    --PII_SEARCH_RESULTS_PK       --BIGINT IDENTITY(1,1) PRIMARY KEY
                                                    --,PII_SEARCH_DATE            --DATETIME2(0) DEFAULT(GETDATE())
                                                     PII_TABLE_COLUMNS_FK         --BIGINT FOREIGN KEY REFERENCES dbo.PII_TABLE_COLUMNS(PII_TABLE_COLUMNS_PK)                                
                                                    ,PII_SEARCH_VALUE             --NVARCHAR(255)
                                                    ,PII_SEARCH_RESULT_FLAG       --CHAR(1) -- Y, search found, N value not found
                                                    ,PII_SEARCH_RECORDS_FOUND     --INT -- count of records found matching search value
                                                    ,PII_SEARCH_SQL_STATEMENT     --NVARCHAR(4000)
                                                    ,PII_UPDATE_SQL_STATEMENT     --NVARCHAR(4000)
                                                    --,PII_COLUMN_UPDATED_FLAG      --CHAR(1) DEFAULT('N') -- Y updated
                                                    ,PII_SEARCH_RESULTS_MODIFIED_DATE  --DATETIME2(0)
                                                    ,PII_SELECT_RECORD_SQL_STATEMENT -- NVARCHAR(4000) MSH 07/22/2020

                                                    ) VALUES (
                                                     @PII_Table_Columns_Pk
                                                    ,@inpPhone
                                                    ,@SearchFlag
                                                    ,@SearchCountHits
                                                    ,@SQL_Search_Statement
                                                    ,@SQL_Update_Statement
                                                    ,GETDATE()
                                                    ,@PII_Select_Record_SQL_Statement -- MSH 07/22/2020
                                                    )
				END
			END													

			FETCH NEXT FROM @Cursor INTO @PII_Table_Columns_Pk, @PII_Database_Name, @PII_Table_Name, @PII_Column_Name, @PII_Column_Data_Flag, @PII_Column_Data_Flag_Subtype, @PII_Has_Customer_Data;

		END

		CLOSE @Cursor;
		DEALLOCATE @Cursor;

	END
END
GO
