
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [2.0.0] - 2021-12-10
### Added
1. Upgraded dispatcher_sdk version to 2.0.0 in build.sbt.
2. Upgraded spark-mllib version to 3.1.2 in build.sbt.
3. Upgraded scala version to 2.12.10 in build.sbt.
4. Upgraded scalatest version to 3.2.10 in build.sbt.
5. Commented out some dependencies in build.sbt due to inheritance from dispatcher_sdk.
6. Done refactoring due to new method signatures and to avoid serialization issues (Rex, Eval) in Spark 3.1.2.
8. Added the ability to run tests in client mode on a cluster.

## [1.7.6] - 2021-12-27
### Fixed
- log4j 2.17.0.

## [1.7.5] - 2021-12-16
### Fixed
- log4j RCE.

## [1.7.4] - 2021-12-02
### Fixed
- Search command doesn't use Bloom Filter for FTS.

## [1.7.3] - 2021-11-01
### Fixed
- Dispatcher throws exception if delta in runInfiniteLoop is negative.

## [1.7.2] - 2021-10-14
### Fixed
- CacheManager now supports all spark types

## [1.7.1] - 2021-09-27
### Fixed
- OTLWhere works incorrectly with casting INT to BIGINT  
- OTLFieldSummary returns DF with incorrect SCHEMA
- OTLReplace throws exception on fields with null,
incorrectly replaces with empty string
- OTLLookup throws "Error in 'lookup' command" if new columns are added after
- CacheManager works incorrectly with types Decimal, Float 

## [1.7.0] - 2021-09-06
### Added
- CustomExceptions usage (dispatcher SDK successor class)
### Fixed
- OTLTop creates an empty and unexpected extra column named after the top number argument.
- OTLTop works incorrectly with "by" argument.
- OTLRex works incorrectly with complex regex, part of the expression is recognized as a subsearch.
- OTLRename works incorrectly if the source column name contains a space.

## [1.6.0] - 2021-03-12
### Added
- Organization fields to build.sbt. Now authorized developers can import modules from SuperDispatcher.
- Pause to InfinitiveLoop of SuperVisor in order to decrease postgres load in platform idle.  
### Fixed
- OTLCollect saves buckets in wrong format
- OTLSort creates wrong columns based on ascending/descending syntax of args
- OTLTimechart mistakenly creates 0-column 
- Converter ignores fieldsUsed property set by command from a plugin. This results to unexpected columns in output
 dataset.
  

## [1.5.2] - 2020-08-03
### Fixed
- OTLEval concat instead of addition because of Catalyst unresolved functions

## [1.5.1] - 2020-07-13
### Fixed
- OTLWhere fails if spaces are in field names

## [1.5.0]
### Added
- Add boolean type for loaded caches
- Logging to OTLWhere command
### Fixed
- Subsearch caches've been still locked if parent job failed
- Don't catch throwable if job fails
- OTLWhere fails on String strip method over expression
### Changed
- Update to new SDK

## [1.4.0]
### Changed
- Remove unmanaged dependencies (lib dir).
- Remove unused files in tests.

## [1.3.1]
### Fixed
- Command _filldown_ doesn't fill null values.

## [1.3.0]
### Added
- Makefile.
### Changed
- Remove all SPL and Splunk mentions from code.
### Fixed
- Command _untable_ doesn't work at all.

## [1.2.1]
### Changed
- Adding non existent columns removed from _FullRead_ class.
- Row number limit parameter moved to configuration file.
- Plugin test updated.
### Fixed
- Fixed invalid path for _fieldsummary_ command in _command.properties_ file.

## [1.2.0]
### Changed
- SDK for creating plugin commands moved to another repository and included as dependency.

## [1.1.0]
### Add
- SDK for creating plugin commands. More details in [Plugins.md](https://github.com/ISGNeuroTeam/dispatcher_sdk)
- Mapping from commands to classes with realisation moved to [commands.properties](src/main/resources/commands.properties)
- Configuring of logging levels in loglevel.properties
- Possibility to write comments in query using triple backticks like  `` ```MY COMMENT```  ``

## [1.0.0]
### Changed
- Commands _read_ and _otstats_ returns multivaue fields when search have wildacarded values.
If field is used with index (for example _text {2} .val_) commands returns single value.
When field is used like _fieldname {}. postfix_ always commands returns multivalue.
If path to list used in search without _ {} _, returns null.
Fields without _ {} _ returns as single value.
- Command _read_ does not returns fields extracted in index time
- Command _otstats_ is set as default when running _execute_ method in tests
### Fixed
- Command _eval_ doesn't create ambigous fields.
- Command _eval_ with function _tonumber_ doesn't throw NullPointerException when argument is null
- Tests modified to match output of search time field extraction
- Function strptime serialisation error fixed

## [0.18.0] - 2020-01-20
### Add
- Command otstats for reading index time extracted fields. 
### Changed
- By default all buckets consist of parquets with one scheme: _time, _raw, so scheme merging or other mechanisms are disabled.
- Command search is used for reading only _time and _raw fields with STFE mechanism.
- Command otstats is used for reading all columns in parquet which were extracted at index time by advanced users.
### Fixed
- Problems with Field Extraction mechanism.

## [0.17.4]
### Changed
- Search time field extraction was modified for working with multivalues 
### Fixed
- Search time field extraction was modified for working with fieldnames which have dot in name
- Head and tail not retuns an argument as the column

## [0.17.3] - 2019-11-22
### Fixed
- Command where works with like operator.
### Changed
- Schema files read and merged by spark.
- Command stats returns df with all used and generated columns if _by_ column is absent.
- Command inputlookup generates absent fields.

## [0.17.2] - 2019-11-18
### Add
- Command rex can take multivalues as input. Param max_match=0 means ulimited matches. 
### Changed
- Command eval supports multiple expressions separated by comma.
- Command where supports logical operators end eval expressions
- Command join removes filed from left DF if field with the same name exists in rigtht df.
If right df is empty result df is the same as the left df. 
- Command read throws exception only if search on every index in query throws an exception.
### Fixed
- Command rex doesn't fail on null values
- Fails of subsearches on failed owner.
- Command table works with cyrillic symbols and quotes

## [0.17.1] - 2019-11-15
### Changed
- Schema is readed from separate _.schema_ file
- Read removes null elements from multivalues 
- Table * doesn't returns nested fields
### Fixed
- Parsing fields in read command
- All queried fields existing in schema have values in df.Parameter _max_mv_size_ added to _application.conf/indexes_
- Command timechart removes data frame prints from stdout.

## [0.17.0] - 2019-10-25
### Fixed
- Field names with {} in eval interprets as multivalue.
- Command read correctly add backticks if one field is substring of another field in query section.
- Command stats-like works with multiple words in as statement.
- Command read backticks appears in schema.
- Command join rename as problem.

### Changed
- Remove raw_logs directory from bucket structure. 
- Parquet partitions now contains Spark Schema in meta section.  
- Command where explicitly converts field to string if field is compared with constant value.  

## [0.16.15] - 2019-10-25
### Add
- Add BIGINT -> BigIntType to CacheManager.  

## [0.16.14] - 2019-10-24
### Add
- Add spark.sql.crossJoin.enabled=true to deploy.bash  

### Changed
- Command return returns dataframe with columns from args limited by first arg.
- CacheManager loads cache forming StructType manually. Supported types:

| DDL format | SparkType |
|------------|-----------|
|"STRING" | StringType |  
|"DOUBLE" | DoubleType |  
|"INTEGER" | IntegerType |  
|"INT" | IntegerType |  
|"LONG" | LongType |  
|"NULL" | NullType |  
|"ARRAY<STRING>" | ArrayType(StringType) |  
|"ARRAY<DOUBLE>" | ArrayType(DoubleType) |  
|"ARRAY<INTEGER>" | ArrayType(IntegerType) |  
|"ARRAY<INT>" | ArrayType(IntegerType) |  
|"ARRAY<LONG>" | ArrayType(LongType) |  
|"ARRAY<NULL>" | ArrayType(NullType) |  

- Command mvrange can take field name as any argument.

### Fixed
- Command mvindex can take any numeric type as index arg.
- Command join works on empty data frame from the right side.

## [0.16.13] - 2019-10-22
### Changed
- Command return returns single string value of kv pairs separated by OR
- Command where realises custom logic for multivalues:

| rvalue  | lvalue | = | != | >  or  < |
| ------- | ------ | --- | --- | --- |
| mv | mv | arrays equality | arrays unequality | false |
| sv | mv | mv contains sv | mv not contains sv | false |
| mv | sv | mv contains sv | mv not contains sv | false |

### Fixed
- Command lookup returns input dataframe if lookup fields doesn't match to dataframe fields
- Command strptime now works with null columns. 

## [0.16.12] - 2019-10-21
### Changed
- Now search will fail if subsearch fails.

## [0.16.11] - 2019-10-21
### Fixed
- Command lookup returns input dataframe if lookup fields doesn't match to dataframe fields.
- Command filter now push down it's fields to read command.

## [0.16.10] - 2019-10-16
### Changed
- Command mvindex returns null in input column is null or single value.
- Command mvcount returns null in input column is null or single value.
- Command mvexpand returns null in input column is null and input value if input is single value.
- Command mvzip returns empty multivalue in any of input columns is not multivalue.

## [0.16.9] - 2019-10-16
### Added
- Scheme of empty DataFrame.

### Changed
- Start using "changelog".
- Command rex returns single value if max_match=1 else multi value.

### Fixed
- Locking caches of several subsearches.
- Command appendpipe.
- Command rex now works with multi value columns. 

### Removed
- Troubled times because of unknown changes.
