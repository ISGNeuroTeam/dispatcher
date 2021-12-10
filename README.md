# OT Platform. Spark Driver App.


Converts OTL queries to Spark DAG and orchestrates computing processes based on user pools. Also, manages caches in
 order to decrease a computational load on a platform.

## Getting Started

You'd to `publish local` dispatcher-sdk lib in order not to use unmanaged libs.

### Prerequisites
Install:
* java 11
* sbt 1.5.5
* scala 2.12.10

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
* union 
* untable
* where  
* xyseries


Untested but realized commands:
* chart
* convert  
* dedup 
* delta  
* eventstats
* fieldsummary  
* filldown
* nomv  
* rangemap  
* sort  
* streamstats
* top
* transaction
* transpose  
* outputlookup

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

json4s-native_2.12-3.5.5.jar  
postgresql-42.2.5.jar  
dispatcher-sdk_2.12-2.0.0.jar

## Compatible with

ot_simple_rest 1.7.0   
ot_simple 1.0.0  
zeppelin-spl 2.2.0
ot_simple_zeppelin 0.2.1
nifi-custom-processors 0.1.12  
nifi_processors 1.4.1  

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
