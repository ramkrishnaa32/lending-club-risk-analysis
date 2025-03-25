import constants, helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("SHOW DATABASES").show()
spark.sql("DESCRIBE DATABASE EXTENDED lending_club").show(truncate=False)
spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

spark.sql("select * from lending_club.loans_defaulters_details").show(10, truncate=False)
