package FixedWidthFileToJSON

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ToJSON_Final_With_Class {

  def main(args: Array[String]): Unit = {

    // First File - Regular fixed width file
    // Define Input Path, Output Path, fixedWidths and Schema String
    val inputPath1: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\input\\fruit__noheader.txt"
    val outputPath1: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\output\\file1\\"
    val fixedWidths1 = Array(3, 10, 5, 4)
    var schemaString1: String = "Col1,COl2,COl3,COl4"

    val file1 = new ToJSONClass(inputPath = inputPath1,
      outputPath = outputPath1, fixedWidths = fixedWidths1, schemaString = schemaString1
    )
    file1.execute()

    // Second File - Fixed width file with header
    // Define Input Path, Output Path, fixedWidths and Schema String
    val inputPath2: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\input\\fruit__withheader.txt"
    val outputPath2: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\output\\file2\\"
    val fixedWidths2 = Array(3, 10, 5, 4)
    val header2 : Boolean = true

    val file2 = new ToJSONClass(inputPath = inputPath2,
      outputPath = outputPath2, fixedWidths = fixedWidths2, header = header2
    )
    file2.execute()


    // Third File - Fixed width file with comments marked with '/' and header
    // Define Input Path, Output Path, fixedWidths and Schema String
    val inputPath3: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\input\\fruit_withcomments.txt"
    val outputPath3: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\output\\file3\\"
    val fixedWidths3 = Array(3, 10, 5, 4)
    val comment3 : String = "/"
    val header3 : Boolean = true

    val file3 = new ToJSONClass(inputPath = inputPath3,
      outputPath = outputPath3, fixedWidths = fixedWidths3, header = header3, comment = comment3
    )
    file3.execute()
  }

}
