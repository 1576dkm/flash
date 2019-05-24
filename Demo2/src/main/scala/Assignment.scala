import org.apache.spark.sql.SparkSession

object Assignment {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Creating Parquit file!!!")
      .master("local")
      .getOrCreate()

    val df = spark.read.option("header","true").csv("data.csv")
    df.write.parquet("csv_to_paraquet")
    val df_1 = spark.read.option("header","true").parquet("csv_to_paraquet")
      
      
      
      
    /*val csvRDD = spark.sparkContext.textFile("data.csv")
    val header = csvRDD.first()
    val csvRDDWithoutHeader = csvRDD.filter(_ != header)
    csvRDDWithoutHeader.take(15).foreach(println)
    //val modifiedRDD = csvRDDWithoutHeader.map(line => {
      val colArray = line.split(",")
      var col_0 = Array(colArray(0)).mkString(" ")
      var col_1 = Array(colArray(1)).mkString(" ")
    }).take(15).foreach(println)*/
  }
}