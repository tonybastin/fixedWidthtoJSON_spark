# flatWidthtoJSON_spark

This program allows reading fixed-width files in local or distributed filesystem as JSON files. The program accepts the following options:

inputpath (REQUIRED): location of input files. 
outputpath (REQUIRED): location of output files. 
fixedWidths (REQUIRED): Int array of the fixed widths of the source file(s)
schemaString: Column names seperated by ','. All types assumed as string
useHeader: when set to true the first line of files will be used to name columns. All types will be assumed string. Default value is false. Will also skip commented lines when selecting the lines containing column names.
comment: skip lines beginning with this character. Default is "#". Disable comments by setting this to null.
