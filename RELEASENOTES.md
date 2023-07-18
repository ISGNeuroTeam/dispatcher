## [2.3.3]
### Hotfix
- Functionality of checkpoints cleaning extended for cases of failed/canceled jobs.

## [2.3.2]
### Hotfix
- Added support for checkpointing on parallel working queries.

## [2.3.1]
### Hotfix
- Added support for long commands combined to long queries.

## [2.3.0]
### New
- Applying of checkpointing technology to support the performance and fault tolerance of large queries.

## [2.2.2] - Hotfix
### Hotfix
- Fixed incorrect definition of mainArgs parameter in dispatcher-sdk.

## [2.2.1] - Hotfix
### Hotfix
- Fixed incorrect parsing of quoted many-words-consisted keywords.

## [2.2.0]
### New
- Principle of stats command working by separating time and other aggregation functions and calculations of them by different logic.
- Principle of extraction of fields, used in query.
- Improved connected logic in join command.

## [2.1.2] - Hotfix
### Hotfix
- Rollback of hotfix 2.1.1. 

## [2.1.1] - Hotfix
### Hotfix
- Setted default accuracy argument in percentile_approx spark function calls.

## [2.1.0]
### New
- Filldown command optimized for case of missing of targeting columns.
- Added keyword _defineTargetColumns_ to filldown syntax.

## [2.0.3] - Hotfix
### Hotfix
- Changed number of spark_exec_env version.
- Deleting copying of sparkexecenv.conf.example to app directory because spark_exec_env library isn't need config file now and use standard not-changed values for working.

## [2.0.2] - Hotfix
### Hotfix
- Added copying of sparkexecenv.conf.example file to dispatcher directory in deployment process.
- For start.sh paths generation project name replaced from Dispatcher to dispatcher.

## [2.0.1] - Hotfix
### Hotfix
- Fixed problem with not-worked command filldown without arguments.
- Fixed problem with unworked consecutive parameter in dedup command.
- application.conf.example changed for ability of working in Computing Node Mode. (Computing Node Mode - mode of Dispatcher's work, required for Architecture 2.0 )
- Added config file for Spark Execution Environment - sparkexecenv.conf. (Spark Execution Environment require for work in Computing Node Mode.)

## [2.0.0]
### New
- Spark Computing Node Mode for Architecture 2.0 with OTL commands processing ability.

## [1.9.0]
### New
- Connection to Kafka service
- Spark Computing Node Mode for Architecture 2.0

## [1.8.0]
### New
- command fields aligned with the implementation in the Splunk.
- commands untable, transaction, return, fields now return an empty dataframe for invalid arguments.
- bloom filter filename must be set in application.conf.
### Bugfix
- regexp for search command in search time field extraction.
- bug in nomv command with not unremoved backticks.
- problem with unworked consecutive parameter in dedup command.
- problem with non-work 0 limit.

# [1.7.8] - Hotfix
### Hotfix
Fixed problem with long OTL query by altered CachesDL table with new "hash" column.

# [1.7.7] - Bugfix
We have fixed few bugs in the Dispatcher core and some OTL commands.

## [1.7.6] - Hotfix
### Hotfix
Updated log4j to 2.17.0.

## [1.7.5] - Hotfix
### Hotfix
Fixed log4j RCE.

# [1.7.4] - Hotfix
### Hotfix
Fixed Bloom filter support in search command. Now FTS and column filters are fast again on not indexed data. 

# [1.7.3] - Hotfix
### Hotfix
- Dispatcher throws exception if delta in runInfiniteLoop is negative.

# [1.7.2] - Bugfix
### Bugfix
We changed the way the manager works, now it supports all spark types.

# [1.7.1] - Bugfix
### Bugfix
We have fixed few bugs in the core OTL commands: where, fieldsummary, replace, lookup.
- OTLWhere works incorrectly with casting INT to BIGINT
- OTLFieldSummary returns DF with incorrect SCHEMA
- OTLReplace throws exception on fields with null,
  incorrectly replaces with empty string
- OTLLookup throws "Error in 'lookup' command" if new columns are added after
- CacheManager works incorrectly with types Decimal, Float

# [1.7.0] - CustomException class from the Dispatcher SDK and few bugfixes

### New
The brand new CustomException class inherited from the Dispatcher SDK.

This class is being used to uniform and localize the exception messages. Developer could use a pre-defined error code prop. (like E00027 or something) or call the exception in the usual way.

For example, one can throw an issue like this: 
`throw E0008($seatchId)` or using the legacy way: `throw CustomException(8, $searchId)`.

If you want to trow an issue that is undefined in CustomException properties, please, use the legacy way. In the next release we are going to decouple CustomException class, so one can add his own error codes in convenient way.

### Bugfix
We have fixed few bugs in the core OTL commands: top, rename, rex.
- TOP will not produce an empty and unexpected extra column named after the top number argument. Consider that the command will still produce an empty column if you try to refer to the non-existent field (column will be named after). This behavior is a splunk-like and is needed to keep some compatibility.
- Also, TOP now works correct with _"by"_ argument.
- REX regular expressions processing improved. Please, use this command wisely, do not name RE groups after parts of the text being examined.
- RENAME command now process composite named columns correct.