import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FinancialPipeline {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Credit Card Fraud Detection")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/creditcard.csv")

    println(s"Total transacciones: ${df.count()}")

    val fraudes = df.filter(col("Class") === 1)
    val normales = df.filter(col("Class") === 0)

    println(s"Normales:     ${normales.count()}")
    println(s"Fraudulentas: ${fraudes.count()}")

    println("\nEstadisticas de importe en fraudes:")
    fraudes.select("Amount").describe().show()

    println("Fraudes por rango de importe:")
    fraudes.withColumn("rango",
      when(col("Amount") > 1000, "alto")
      .when(col("Amount") > 100, "medio")
      .otherwise("bajo")
    ).groupBy("rango")
      .agg(count("*").alias("num"), round(avg("Amount"), 2).alias("importe_medio"))
      .orderBy(desc("num"))
      .show()

    fraudes.write.mode("overwrite").parquet("output/fraudes")
    println("Guardado en output/fraudes")

    spark.stop()
  }
}
