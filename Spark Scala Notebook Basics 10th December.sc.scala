// Databricks notebook source
// MAGIC %fs ls /FileStore/tables/reatail_db/orders

// COMMAND ----------

spark

// COMMAND ----------

val orders=spark.read.format("csv")
.option("inferSchema","true")
.load("/FileStore/tables/reatail_db/orders")
.toDF("order_id","order_date","order_cust_id","order_status")   

// COMMAND ----------

orders.printSchema
orders.show

// COMMAND ----------

val orderItems = spark.read.format("csv")
.schema("order_item_id INT, order_item_order_id INT, order_item_product_id INT, order_item_quantity INT, order_item_subtotal FLOAT, order_item_product_price FLOAT")
.load("/FileStore/tables/reatail_db/order_items")

// COMMAND ----------

val customers = spark.read.format("csv")
.schema("customer_id INT, customer_fname STRING, customer_lname STRING, customer_email STRING, cust_pwd STRING, cust_street STRING, cust_city STRING, cust_state STRING, cust_zip STRING")
.load("/FileStore/tables/reatail_db/customers")

customers.show

// COMMAND ----------

val products = spark.read.format("csv")
.schema("product_id INT, product_category_id INT, product_name STRING, product_description STRING, product_price FLOAT, product_image STRING")
.load("/FileStore/tables/reatail_db/products")

products.printSchema
products.show

// COMMAND ----------

// Get distinct order statuses

orders.select("order_status").distinct.show

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// get order status count

val orderStatusCount= orders.
  groupBy("order_status").
  agg(count(lit(1)).alias("order_count"))

orderStatusCount.orderBy("order_count").show

// COMMAND ----------

display(orderStatusCount)

// COMMAND ----------

// filter orders by complete and closed orders

val ordersCompleted = orders.filter("order_status IN ('COMPLETE', 'CLOSED')")
ordersCompleted.count


// COMMAND ----------

// filter orders by complete and closed orders -- Method 2
import org.apache.spark.sql.functions._
val ordersCompleted2 = orders.filter(col("order_status")==="COMPLETE" || col("order_status")==="CLOSED")
ordersCompleted2.show

// COMMAND ----------

// join on orders

val joinResults = ordersCompleted.join(orderItems, ordersCompleted("order_id")===orderItems("order_item_id")).
  join(products, products("product_id")===orderItems("order_item_product_id")).select("order_date", "product_name", "order_item_subtotal")

joinResults.show(false)  

// COMMAND ----------

// Daily product revenue for each product

val dailyRevenue= joinResults.groupBy("order_date","product_name").agg(round(sum("order_item_subtotal"),2).alias("revenue"))

dailyRevenue.show

// COMMAND ----------

display(dailyRevenue)

// COMMAND ----------

// used to import $. (If col names are passed in "" then they are string, but if we need them as columns we should use $ or col)
import spark.implicits._

val dailyRevenueOrdered = dailyRevenue.orderBy($"order_date".desc, col("revenue").desc)
dailyRevenueOrdered.show(false)

// COMMAND ----------

// write dataframes into csv
// use mode as append if you want to append
dailyRevenueOrdered.write.mode("overwrite").csv("/FileStore/tables/reatail_db/daily_product_revenue")

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/reatail_db/daily_product_revenue

// COMMAND ----------

// getting monthly revenue for each customer

val totalCompleteFailedPendingOrders= orders.filter(col("order_status")==="COMPLETE" || col("order_status")==="CLOSED" || col("order_status")==="PENDING")
totalCompleteFailedPendingOrders.count


// COMMAND ----------

spark.conf.set("spark.sql.analyzer.failAmbiguousSelfJoin", "false")

// COMMAND ----------

// join on customers to get customer names

val joinResults2 = ordersCompleted.join(orderItems, ordersCompleted("order_id")===orderItems("order_item_id")).
  join(customers, customers("customer_id")===orders("order_cust_id")).
  select(orders("order_date"), customers("customer_fname"),customers("customer_lname"), orderItems("order_item_subtotal"))

joinResults2.show(false) 

// COMMAND ----------

val joinResults3=joinResults2.withColumn("customer_name", concat($"customer_fname", lit(" "), $"customer_lname"))
  .withColumn("order_month", date_format($"order_date","yyyyMM")).select("order_month", "customer_name", "order_item_subtotal")

joinResults3.show 
joinResults3.distinct.show

// COMMAND ----------

val customerMonthlyRevenue =joinResults3.groupBy("order_month","customer_name").agg(round(sum("order_item_subtotal"),2).alias("final_revenue")).orderBy($"order_month", $"final_revenue".desc)
customerMonthlyRevenue.show

// COMMAND ----------

customerMonthlyRevenue.write.mode("overwrite").csv("/FileStore/tables/reatail_db/mothly_customer_revenue")

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/reatail_db/mothly_customer_revenue

// COMMAND ----------


