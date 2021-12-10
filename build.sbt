name := "Dispatcher"

organization := "ot.dispatcher"

version := "2.0.0"

scalaVersion := "2.12.10"

ThisBuild / useCoursier := false

retrieveManaged := true

resolvers += Resolver.jcenterRepo

resolvers += "Sonatype OSS Snapshots" at (sys.env.getOrElse("NEXUS_OTP_URL_HTTPS","")+"/repository/ot.platform-sbt-releases/")

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.12" % "2.0.0" % Compile

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.1.2"

libraryDependencies += "net.totietje" %% "evaluator" % "1.1.0"

// from dispatcher_sdk
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3"
//libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
//libraryDependencies += "com.typesafe" % "config" % "1.3.4"

libraryDependencies += "org.postgresql" % "postgresql" % "42.2.5"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.0"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.0" % Runtime

libraryDependencies += "org.apache.zeppelin" % "zeppelin-interpreter" % "0.8.1"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.3.0"

libraryDependencies += "org.json4s" %% "json4s-native" % "3.5.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test

libraryDependencies += "io.spray" %% "spray-json" % "1.3.5" % Test
