## [1.1.2] - Schema Merging
### Added
- A scheme merging option (mergeSchema) to readFile. Now you can read partitions of a "file" with different schemes.
- A schema inferring option (inferSchema) to readFile. Now you can get other than String types reading your file.

## [1.1.0] - Control options in commands readFile and writeFile
### New
- Arg "header" to readFile and writeFile commands. Now you can disable a header of a csv file.
- Default value "parquet" to "format" arg in readFile and writeFile commands. Now you can skip required before arg "format=". 
- Arg "mode"  ('overwrite', 'append', 'ignore', 'error', 'errorifexists') to writeFile command. Now you can append data instead of overwriting it.
- Arg "numPartitions" to writeFile command. Now you can control the number of output files.
- Arg "partitionBy" to writeFile command. Partitioning is now available.
- Aliases "put" and "get" to writeFile and readFile commands.

# [1.0.3] - Repository improvements
### Changed
- plugin.conf.example.
- README.md.

# [1.0.2] - Updating dependency

### New
- Dispatcher_sdk dependency updated to 1.2.0.
- ReleaseNotes.md.