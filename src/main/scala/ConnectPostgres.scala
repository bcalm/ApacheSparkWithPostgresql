import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import java.util.Properties

object ConnectPostgres {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .appName("connect postgresql")
      .master("local[3]")
      .getOrCreate()

    val orders = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:orders")
      .option("dbtable", "orders")
      .option("user", "vikram")
      .option("password", "mypassword")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "vikram")
    connectionProperties.put("password", "mypassword")
    val ordersItems = spark.read
      .jdbc("jdbc:postgresql:orderitem", "orderitems", connectionProperties)

    val customers = spark.read
      .jdbc("jdbc:postgresql:customers", "customers", connectionProperties)

    val filteredOrders = orders.filter("order_status IN ('COMPLETE', 'CLOSED', 'PENDING')")


    val joinResult = filteredOrders
      .join(ordersItems, ordersItems("order_item_order_id") === orders("order_id"))
      .join(customers, customers("customer_id") === orders("order_customer_id"))
      .select("customer_fname", "customer_lname", "order_date", "order_item_subtotal")


    import spark.implicits._
    val concattedCustomerName = joinResult
      .select(
        concat($"customer_fname", lit(" "), $"customer_lname").as("name"),
        date_format($"order_date", "yyyyMM").as("month"),
        $"order_item_subtotal"
      )

    val result = concattedCustomerName.groupBy("month", "name")
      .agg(round(sum("order_item_subtotal"), 2).as("revenue"))
      .orderBy(asc("month"), desc("revenue"))


    result.write
      .jdbc("jdbc:postgresql:apachedemo", "monthlyExpense", connectionProperties)

  }
}