All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.8.1] - 2024-01-24
### Fixed
- Not worked operator NOT in where command.

## [2.8.0] - 2024-01-19
### Changed
- Processing of stats command moved to otl_processors library.
### Fixed
- Not worked alias in stats-based commands if alias word not 'as', but 'AS' or 'As' or 'aS'.
- Incorrect work of where command with nested conditions, bounded by parentheses.
- 'head of empty list' bug in otstats.
- Cyrillic named columns was ignored.
- Application crashing when eval with nested functions in argument is calculating.

## [2.7.0] - 2023-11-29
### Added
- Possibility to use rename command in Arch 2.0 queries.
- Possibility to use replace command in Arch 2.0 queries.
- Possibility to use search command in Arch 2.0 queries.
- Possibility to use eval command in Arch 2.0 queries (practically all functionality - elementary operands, expressions and functions).
- Possibility to use where command in Arch 2.0 queries (partially realized - only where column = <number/text/boolean>)
- Possibility to use makeresults command in Arch 2.0 queries.
### Changed
- Processing of rename command moved to otl_processors library.
- Processing of replace command moved to otl_processors library.
- Processing of eval command moved to otl_processors library.
- Processing of where command moved to otl_processors library.
- Processing of search command moved to otl_processors library.
- Processing of makeresults command moved to otl_processors library.
### Fixed
- Incorrect behavior of rename command in case there is lexical error in 'as' keyword: syntax error is thrown now.
- Incorrect behavior of replace command in case of double quotes in replacing text.
- Bug of where command in 'where a = false' case: improved definition of variables as separate words in parsing of where expression.

## [2.6.1] - 2023-09-11
### Fixed
- eventstats behavior for columns with only null values: command return input dataframe now.

## [2.6.0] - 2023-09-08
### Added
- Possibility of processing of the null-containing fields by eventstats command.
### Changed
- untable command in case of dataframe, containing only fixed column (no columns for untable processing), return input dataframe now.
### Fixed
- Incorrect behavior in parsing OTL command arguments when the equals sign is inside quotes.
- Parsing of evaluated expressions: equal symbol inside eval functions not identified as expression equal symbol now.
- Non-renaming of field by as in *stats commands in cases of > 1 whitespaces between function name and as or between as and alias.
- Incorrect behavior of rex command in case of extracting from raw if new field name equals name from raw.
- OTLEventstats dependency from OTLStats deleted.
- Output of command inner work field 'fake' in result table of eventstats command.
- OTLTransaction dependency from OTLStats deleted.

## [2.5.0] - 2023-08-15
### Changed
- Optional keyword usedLimit added to checkpoints command.

## [2.4.2] - 2023-07-27
### Fixed
- Insufficiently flexible configuration of the checkpoints config section - added parameters for managing the use of checkpoints limits.

## [2.4.1] - 2023-07-27
### Fixed
- Low performance of queries with checkpoints - repeating queries to config was deleted.

## [2.4.0] - 2023-07-26
### Changed
- checkpoints command used for checkpointing management in limits of 1 query now.

## [2.3.3] - 2023-07-18
### Fixed
- Checkpoints not clean if job was failed/canceled.

## [2.3.2] - 2023-07-17
### Fixed
- No support for checkpointing on parallel working queries.

## [2.3.1] - 2023-07-14
### Fixed
- No support for long commands combined to long queries.

## [2.3.0] - 2023-07-05
- Introduced the use of checkpointing technology to support the performance and fault tolerance of large queries.

## [2.2.2] - 2023-06-29
### Fixed
- Incorrect definition of mainArgs parameter in dispatcher-sdk, initiating errors in smallplugin.

## [2.2.1] - 2023-06-26
### Fixed
- Incorrect parsing of quoted many-words-consisted keywords.

## [2.2.0] - 2023-06-01
### Changed
- Principle of stats command working by separating time and other aggregation functions and calculations of them by different logic.
- Principle of extraction of fields, used in query - distribution of extraction between commands, which require fields, introduced.
### Fixed
- Incorrect work of first-last aggregate functions in stats and stats-like commands in cases of large dataframe volume.
- Incorrect work of join command in cases of existence of repeating by name, but not containing by join list columns in connected dataframes.

## [2.1.2] - 2023-02-01
### Fixed
- Rollback of hotfix 2.1.1. 

## [2.1.1] - 2023-01-30
### Fixed
- Deleted unacceptable parameter of accuracy in percentile_approx spark function calls.

## [2.1.0] - 2023-01-18
### Changed
- Filldown command optimized for case of missing of targeting columns.
- Added keyword _defineTargetColumns_ to filldown syntax. 

## [2.0.3] - 2022-11-03 
### Changed
- Number of spark_exec_env version.
### Removed
- All actions with sparkexecenv.conf

## [2.0.2] - 2022-10-21
### Fixed
- Non-copied sparkexecenv.conf.example file.
- For start.sh paths generation project name replaced from Dispatcher to dispatcher.

## [2.0.1] - 2022-10-20
### Fixed
- Problem with not-worked command filldown without arguments.
- problem with unworked consecutive parameter in OTLDedup.
- OTLDedup dependency from OTLSort deleted.
- For version 2.0.0 features working: changed application.conf.example and added sparkexecenv.conf.example

## [2.0.0] - 2022-10-20
### Added
- Dispatcher working as Spark computing node in full accordance with requirements.
- OTL commands processing through connection to Spark Execution Environment and work delegation to SEE.

## [1.9.0] - 2022-09-16
### Added
- Dispatcher connection to Kafka service (Dispatcher as Spark computing node)
- Computing Node Mode for Architecture 2.0

## [1.8.0] - 2022-08-26
### Added
- test dataset csv-file for RawRead / FullRead tests (sensors.csv).
- logging for IndexSearch and FileSystemSearch (now loglevel for this classes must be set in loglevel.properties).
- ability to select read command when running OTL-queries (otstats or search).
- methods readIndexDF, compareDataFrames, setNullableStateOfColumn for CommandTest.
- documentation for RawRead, FullRead, OTLBaseCommand, OTLTop, OTLUntable, OTLTransaction, OTLReturn, OTLFields commands.
- documentation for OTLDedup command.
### Changed
- command fields aligned with the implementation in the Splunk.
- commands untable, transaction, return, fields now return an empty dataframe for invalid arguments.
- bloom filter filename must be set in application.conf.
- tests for RawRead, FullRead, OTLTop, OTLUntable, OTLTransaction, OTLReturn, OTLFields classes updated.
### Fixed
- regexp for search command in search time field extraction.
- bug in nomv command with not unremoved backticks.
- problem with unworked consecutive parameter in OTLDedup.
- OTLDedup dependency from OTLSort deleted.
- problem with non-work 0 limit.

## [1.7.8] - 2022-08-05
### Fixed
-  problem with long OTL query by altered CachesDL table with new "hash" column.

## [1.7.7] - 2022-02-16
### Fixed
- scala code formatting.
- return type added to some methods.
- commented out some unused values.
- some code has been improved.

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
