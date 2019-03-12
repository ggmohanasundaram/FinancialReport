
name := "FinancialReport"
version := "0.1"
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "2.3.0")
parallelExecution in Test := false

lazy val commonSettings = Seq( organization := "au.com.prospa",
  scalaVersion := "2.11.11",
  libraryDependencies ++= Seq( "com.typesafe" % "config" % "1.3.1",
    "com.softwaremill.sttp" %% "core" % "1.5.11",
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion ,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.specs2" %% "specs2-core" % "4.0.1" % Test,
    "org.specs2" %% "specs2-matcher-extra" % "4.0.1"% Test,
    "org.specs2" %% "specs2-mock" % "4.0.1" % Test))

lazy val `common` = (project in file("common"))
  .settings(commonSettings)
  .settings(addArtifact(artifact in(Compile, assembly), assembly))
  .settings(Defaults.itSettings)

lazy val `exchangerateproducer` = (project in file("exchangerateproducer"))
  .settings(commonSettings)
  .dependsOn(common)
  .settings(addArtifact(artifact in(Compile, assembly), assembly))
  .settings(Defaults.itSettings)

lazy val `report` = (project in file("report"))
  .settings(commonSettings)
  .dependsOn(common)
  .settings(addArtifact(artifact in(Compile, assembly), assembly))
  .settings(Defaults.itSettings)

lazy val `e2e` = (project in file("e2e"))
  .settings(commonSettings)
  .dependsOn(common,exchangerateproducer,report)
  .settings(addArtifact(artifact in(Compile, assembly), assembly))
  .settings(Defaults.itSettings)




//db132441f4b6453fa4e632eee11f61a6  