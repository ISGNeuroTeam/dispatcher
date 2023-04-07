# OT Platform. Spark Driver App.


Converts OTL queries to Spark DAG and orchestrates computing processes based on user pools. Also, manages caches in
 order to decrease a computational load on a platform.

## Getting Started

You'd to `publish local` dispatcher-sdk lib in order not to use unmanaged libs.

### Prerequisites
Install:
* java 1.8
* sbt 1.8.0
* scala 2.11.12

### Installing

1. Publish local dispatcher-sdk.
2. Download managed dependencies.

### Command List

[Documentation](https://github.com/ISGNeuroTeam/otp/blob/master/docs/functions/index.md)

We've tests for these and think it works... Maybe it is with little problems.  
* addtotals   
* appendpipe
* collect  
* command  
* dedup
* eval (MV-subcommands don't work with SV-fields and vice verse, also some of them are not able to work with nulls)
* fields    
* fillnull 
* foreach
* head  
* inputlookup
* join     
* lookup  
* makemv  
* makeresults
* mvcombine 
* mvexpand 
* otfieldsummary
* otinputlookup
* otloadjob
* otmakeresults
* otoutputlookup
* otstats
* rename    
* replace
* return
* reverse 
* rex  
* search   
* spath  
* stats  
* table    
* tail 
* timechart
* transaction
* union 
* untable
* where  
* xyseries


Untested but realized commands:
* chart
* convert
* delta  
* eventstats
* fieldsummary  
* filldown
* rangemap  
* sort  
* streamstats
* top
* transpose  
* outputlookup

## Window functions performance problem

There are commands that have performance problems on large amounts of data (commands built on spark window functions): appendCols, dedup, delta, filldown, head, eval, stats, streamstats, tail, top, lookup, latestrow, rare.
These problems are internal to Spark and cannot be resolved as part of the platform development process. To solve the problem, platform users are recommended to reduce the amount of data to which these commands will be applied (by filtering or limiting).

## For the platform administrator: 
### spark configuration options to increase the performance of problematic commands:
* spark.sql.shuffle.partitions. Default value: 5000. Increase it to increase performance, but be careful: large values can cause commands to fail at very small volumes.

* spark.driver.maxResultSize. Default value: 6G. Increase according to hardware capabilities.

* spark.default.parallelism. Default value: 8. Adjust according to the situation and hardware capabilities.

* spark.executor.cores. Default value: 4. Adjust according to the situation and hardware capabilities

## Running the tests

sbt test

## Deployment

Use `make pack` to get a deployable tarball. Unpack it to $OTP_HOME. Create conf files based on examples.

### Configuring
Check example configs in docs dir.  

### Logging 
For use logging you should specify path to file _loglevel.properties_  in `loglevel` property in _application.conf_.
File  _loglevel.properties_  contains mapping log levels to logger name.
For command classes name of logger is simple class name by default.

### Starting
Use start.sh from Makefile

## Plugin system
See _Readme.md_ in [Software Development Kit](https://github.com/ISGNeuroTeam/dispatcher_sdk) repository. 

## Built with

config-1.3.4.jar  
json4s-ast_2.11-3.5.5.jar  
json4s-native_2.11-3.5.5.jar  
postgresql-42.2.5.jar  
dispatcher-sdk_2.11-1.2.2.jar

## Compatible with

ot_simple_rest 1.7.0   
ot_simple 1.0.0  
zeppelin-spl 2.2.0
ot_simple_zeppelin 0.2.1
nifi_tools 1.0.5  

## Contributing

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository. 

## Authors

Nikolay Ryabykh (nryabykh@isgneuro.com)  
Sergei Ermilov (sermilov@isgneuro.com)  
Dmitriy Gusarov (dgusarov@isgneuro.com)  
Andrey Starchenkov (astarchenkov@isgneuro.com) 

## License

[OT.PLATFORM. License agreement.](LICENSE.md)

## Acknowledgments

Aleksandr Matiakubov (amatiakubov@isgneuro.com)  
Evgenii Sofronov (esofronov@isgneuro.com)  
Mikhail Rubashenkov (mrubashenkov@isgneuro.com)  
Mikhail Babakov (mbabakov@isgneuro.com)  
Dmitrii Sobolev (dsobolev@isgneuro.com)  
Dmitrii Trubenkov (dtrubenkov@isgneuro.com)  
Aleksandr Kronov (akronov@isgneuro.com)  
Pavel Volkov (pvolkov@isgneuro.com)  
