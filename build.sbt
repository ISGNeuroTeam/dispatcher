name := "dispatcher"

organization := "ot.dispatcher"

version := "1.8.1"

scalaVersion := "2.11.12"

ThisBuild / useCoursier := false

retrieveManaged := true

resolvers += Resolver.jcenterRepo

resolvers += ("Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS","http://storage.dev.isgneuro.com")
  + "/repository/ot.platform-sbt-releases/")).withAllowInsecureProtocol(true)

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.2"% Compile

libraryDependencies += "net.totietje" %% "evaluator" % "1.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.17.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.17.0" % Runtime

libraryDependencies += "com.typesafe" % "config" % "1.3.4"

libraryDependencies += "org.apache.zeppelin" % "zeppelin-interpreter" % "0.8.1"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.1"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

libraryDependencies += "io.spray" %% "spray-json" % "1.3.5" % Test

Test / parallelExecution := false