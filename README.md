# flatWidthtoJSON_spark

This program allows reading fixed-width files in local or distributed filesystem as JSON files. The program accepts the following options:

* inputpath (REQUIRED): location of input files. 
* outputpath (REQUIRED): location of output files. 
* fixedWidths (REQUIRED): Int array of the fixed widths of the source file(s)
* Schema to be specified by one the below methods <br>
a) schemaString: Column names seperated by ','. All types assumed as string <br>
b) useHeader: when set to true the first line of files will be used to name columns. All types will be assumed string. Default value is false. Will also skip commented lines when selecting the header line.
* comment: skip lines beginning with this character. Default is "#". Disable comments by setting this to null.

## Initialisation of job
* Jobs can initialized from object [ToJSON_Final_With_Class](https://github.com/tonybastin/fixedWidthtoJSON_spark/blob/master/src/main/scala/FixedWidthFileToJSON/ToJSON_Final_With_Class.scala)
* Sample Input files can be seen in [input folder](https://github.com/tonybastin/fixedWidthtoJSON_spark/tree/master/src/main/scala/FixedWidthFileToJSON/input)
* Sample Output files can be seen in [output folder](https://github.com/tonybastin/fixedWidthtoJSON_spark/tree/master/src/main/scala/FixedWidthFileToJSON/output)

A sample invocation inside the object [ToJSON_Final_With_Class](https://github.com/tonybastin/fixedWidthtoJSON_spark/blob/master/src/main/scala/FixedWidthFileToJSON/ToJSON_Final_With_Class.scala) is shown below:
`````
// Define Input Path, Output Path, fixedWidths and Schema String
val inputPath1: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\input\\fruit__noheader.txt"
val outputPath1: String = "C:\\tmp\\hive\\tonyb\\TonyDemos\\FixedWidthFileToJSON\\output\\file1\\"
val fixedWidths1 = Array(3, 10, 5, 4)
var schemaString1: String = "Col1,COl2,COl3,COl4"

val file1 = new ToJSONClass(inputPath = inputPath1,
  outputPath = outputPath1, fixedWidths = fixedWidths1, schemaString = schemaString1
)
file1.execute()
`````
