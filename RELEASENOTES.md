# [1.7.8] - Hotfix
### Hotfix
Fixed problem with long OTL query by altered CachesDL table with new "hash" column.

# [1.7.7] - Hotfix
TODO

# [1.7.6] - Hotfix
TODO

# [1.7.5] - Hotfix
TODO

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