import org.apache.spark.sql.SparkSession

import java.util.Properties

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

    val connectionProperties = new Properties()
    connectionProperties.put("user", "vikram")
    connectionProperties.put("password", "mypassword")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:postgresql:apachedemo", "persons", connectionProperties)

    jdbcDF2.show()

    val jdbcDF3 = spark.read
      .jdbc("jdbc:postgresql:apachedemo", "persons", connectionProperties)

    jdbcDF3.show()
  }
}