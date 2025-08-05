import sys
from lib import DataLoader
from lib import Utils
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    # Create Spark session
    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    enable_hive = False  # Set to True if using Hive
    hive_db = "your_hive_db"

    logger.info("Finished creating Spark Session")

    # ✅ Call the read_parties function
    parties_df = DataLoader.read_parties(spark, job_run_env, enable_hive, hive_db)
    parties_df.show(5)

    spark.stop()
