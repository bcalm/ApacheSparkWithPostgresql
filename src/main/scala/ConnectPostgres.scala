import org.apache.spark.sql.SparkSession

object ConnectPostgres {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("connect postgresql")
      .master("local")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:apachedemo")
      .option("dbtable", "persons")
      .option("user", "vikram")
      .option("password", "mypassword")
      .load()

    jdbcDF.show()


  }
}