"""
Query external tables
"""

import helperFunctions

print("Initiating spark session")
spark = helperFunctions.initialize_spark_session("lending_club_risk_analysis")

spark.sql("USE lending_club")
spark.sql("SHOW TABLES").show(truncate=False)

spark.sql("SELECT * FROM score").show()