APP_CLASS = SuperDriver

define ANNOUNCE_BODY
Required section:
 build - build project into build directory, with configuration file and environment
 clean - clean all addition file, build directory and output archive file
 test - run all tests
 pack - make output archive
Addition section:
 start.sh - generate start script
 package - sbt package
endef

define START_BODY
#!/bin/sh \n
cd /opt/otp/$(PROJECT_NAME) \n

if [[ ! -f application.conf ]] ; then \n
    echo 'File "application.conf" is missed, aborting.' \n
    exit \n
fi \n
if [[ ! -f fairscheduler.xml ]] ; then \n
    echo 'File "fairscheduler.xml" is missed, aborting.' \n
    exit \n
fi \n

/opt/otp/spark_master/bin/spark-submit \\\n
--verbose \\\n
--master spark://localhost:7077 \\\n
--deploy-mode cluster \\\n
--supervise \\\n
--driver-cores 2 \\\n
--driver-memory 4G \\\n
--executor-cores 1 \\\n
--executor-memory 6G \\\n
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \\\n
--conf "spark.application.config=/opt/otp/dispatcher/application.conf" \\\n
--conf "spark.blacklist.enable=true" \\\n
--conf "spark.driver.maxResultSize=4G" \\\n
--conf "spark.dynamicAllocation.enabled=false" \\\n
--conf "spark.locality.wait=0" \\\n
--conf "spark.scheduler.allocation.file=/opt/otp/dispatcher/fairscheduler.xml" \\\n
--conf "spark.scheduler.mode=FAIR" \\\n
--conf "spark.shuffle.service.enabled=false" \\\n
--conf "spark.speculation=true" \\\n
--conf "spark.sql.caseSensitive=true" \\\n
--conf "spark.sql.crossJoin.enabled=true" \\\n
--conf "spark.sql.files.ignoreCorruptFiles=true" \\\n
--conf "spark.sql.files.ignoreMissingFiles=true" \\\n
--conf "spark.driver.extraClassPath=/opt/otp/dispatcher/mssql-jdbc-8.2.0.jre8.jar" \\\n
--conf "spark.executor.extraClassPath=/opt/otp/dispatcher/mssql-jdbc-8.2.0.jre8.jar" \\\n
--jars `find $$(cd ..; pwd)/$(PROJECT_NAME) -name "*.jar" | xargs | sed -r 's/ /,/g'` \\\n
--class $(APP_CLASS) $$(cd ..; pwd)/$(PROJECT_NAME)/jars/ot.$(PROJECT_NAME_LOW_CASE)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)-$(VERSION).jar
endef


GENERATE_VERSION = $(shell grep version build.sbt  | sed -r 's/version := "(.+?)"/\1/' )
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')
GENERATE_SCALA_VERSION = $(shell grep scalaVersion build.sbt  | sed -r 's/scalaVersion := "([0-9]+?\.[0-9]+?)\.[0-9]+"/\1/' )
GENERATE_PROJECT_NAME = $(shell grep name build.sbt  | sed -r 's/name := "(.+?)"/\1/' )
GENERATE_PROJECT_NAME_LOW_CASE = $(shell grep name build.sbt  | sed -r 's/name := "(.+?)"/\1/' | tr A-Z a-z)


SET_VERSION = $(eval VERSION=$(GENERATE_VERSION))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))
SET_SCALA_VERSION = $(eval SCALA_VERSION=$(GENERATE_SCALA_VERSION))
SET_PROJECT_NAME = $(eval PROJECT_NAME=$(GENERATE_PROJECT_NAME))
SET_PROJECT_NAME_LOW_CASE = $(eval PROJECT_NAME_LOW_CASE=$(GENERATE_PROJECT_NAME_LOW_CASE))



.SILENT:

COMPONENTS := package start.sh

export ANNOUNCE_BODY
all:
	echo "$$ANNOUNCE_BODY"

pack: build
	$(SET_VERSION)
	$(SET_BRANCH)
	$(SET_PROJECT_NAME_LOW_CASE)
	rm -f $(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz
	echo Create archive \"$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz\"
	cd build; tar czf ../$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz .

package:
	echo Package
	sbt package
	touch package

build: $(COMPONENTS)
	# required section
	echo Build
	$(SET_VERSION)
	$(SET_SCALA_VERSION)
	$(SET_PROJECT_NAME)
	$(SET_PROJECT_NAME_LOW_CASE)
	mkdir build
	mkdir build/$(PROJECT_NAME)
	mkdir build/$(PROJECT_NAME)/plugins
	cp -r lib_managed/jars build/$(PROJECT_NAME)/
	cp -r lib_managed/bundles build/$(PROJECT_NAME)/
	mkdir build/$(PROJECT_NAME)/jars/ot.$(PROJECT_NAME_LOW_CASE)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)
	cp target/scala-$(SCALA_VERSION)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)-$(VERSION).jar build/$(PROJECT_NAME)/jars/ot.$(PROJECT_NAME_LOW_CASE)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)/
	cp start.sh build/$(PROJECT_NAME)/start.sh
	cp docs/application.conf.example build/$(PROJECT_NAME)/
	cp docs/sparkexecenv.conf.example build/$(PROJECT_NAME)/
	cp docs/fairscheduler.xml.example build/$(PROJECT_NAME)/
	cp docs/loglevel.properties.example build/$(PROJECT_NAME)/
	cp README.md build/$(PROJECT_NAME)/
	cp CHANGELOG.md build/$(PROJECT_NAME)/
	cp LICENSE.md build/$(PROJECT_NAME)/





export START_BODY
start.sh: $(package)
	$(SET_VERSION)
	$(SET_SCALA_VERSION)
	$(SET_PROJECT_NAME)
	$(SET_PROJECT_NAME_LOW_CASE)
	echo Create start.sh
	echo -e $$START_BODY > $@
	chmod +x $@

clean:
	# required section
	$(SET_VERSION)
	$(SET_SCALA_VERSION)
	$(SET_PROJECT_NAME_LOW_CASE)
	rm -rf build start.sh lib_managed $(PROJECT_NAME_LOW_CASE)-*.tar.gz package project target

test:
	# required section
	echo "Testing..."
	sbt test