package FixedWidthFileToJSON

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class ToJSONClass (val inputPath: String,
                   val outputPath: String,
                   val fixedWidths: Array[Int],
                   var schemaString: String = "",
                   val header : Boolean = false,
                   val comment: String = "#"
                  ) extends Serializable {

  def execute() : Unit = {
    // Define spark seesion and set spark parameters
    val spark = SparkSession.
    builder.
    appName ("Flat File To JSON").
    master ("local[*]").
    getOrCreate()

    spark.sparkContext.setLogLevel ("ERROR")
    spark.conf.set ("spark.sql.shuffle.partitions", "2")

    // Define accumulator to count exceptions
    val accum  = spark.sparkContext.longAccumulator ("faulty_lines")
    val accum1 = spark.sparkContext.longAccumulator ("comments")
    accum1.add(spark.sparkContext.textFile(inputPath).filter( r => r.startsWith(comment)).count())

    // Function to split strings
    def splitAtWidth(line: String): Array[String] = {

    var noOfColumns = fixedWidths.length
    var col = new Array[String] (noOfColumns)
    var width = 0

    if (line.length == fixedWidths.sum) {
    for (i <- 0 to noOfColumns - 1) {
    col (i) = line.slice (width, width + fixedWidths (i) ).trim
    width += fixedWidths (i)
  }
  } else {
    accum.add (1)
    for (i <- 0 to noOfColumns - 1) {
    col (i) = "Fail"
  }
  }

    col
  }



    // Define Schema programmatically
    val schema = if (!header) {
      new StructType (schemaString.split (",").
        map (fieldName => StructField (fieldName, StringType, true) ) )
    } else {
      val headerLine = spark.sparkContext.textFile (inputPath).filter( r => !r.startsWith(comment)).first ()
      schemaString = splitAtWidth (headerLine).mkString (",")
      new StructType (schemaString.split (",").
        map (fieldName => StructField (fieldName, StringType, true) ) )
    }


    // Check if header exist
    val file = if (header) {
    val fileWithHeader = spark.sparkContext.textFile (inputPath).filter( r => !r.startsWith(comment))
    val headerLine = spark.sparkContext.textFile (inputPath).filter( r => !r.startsWith(comment)).first ()
    fileWithHeader.filter (line => line != headerLine)
  } else {
    spark.sparkContext.textFile (inputPath).filter( r => !r.startsWith(comment))
  }



    val rdd = file.map (line => Row.fromSeq (splitAtWidth (line) ) )
    val df = spark.createDataFrame (rdd, schema)
    df.show

    df.coalesce (1).write.format ("json").mode ("overwrite").save (outputPath)

    println (s"No of faulty lines is ${accum.value}, No of comments is ${accum1.value} and is removed from resulting JSON file")
    spark.stop ()
  }

}
