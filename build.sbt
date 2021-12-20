name := "ExternalDataOTPlugin"

version := "1.0.2"

scalaVersion := "2.11.12"

resolvers += Resolver.jcenterRepo

libraryDependencies += "ot.dispatcher" % "dispatcher-sdk_2.11" % "1.2.0"  % Compile

parallelExecution in Test := false
