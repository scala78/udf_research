import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "udf_research",
    idePackagePrefix := Some("ru.scala78.spark.udf_research"),
    // org.apache.spark.sql
    assembly / mainClass := Some("ru.scala78.spark.udf_research.UDFResearch"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided withSources (),
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided withSources (),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided withSources (),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided withSources (),
      //      "org.apache.spark" %% "spark-core" % sparkVersion withSources(),
      //      "org.apache.spark" %% "spark-streaming" % sparkVersion withSources(),
      //      "org.apache.spark" %% "spark-sql" % sparkVersion withSources(),
      //      "org.apache.spark" %% "spark-catalyst" % sparkVersion withSources(),
      "org.scala-lang.modules" % "scala-java8-compat_2.12" % "1.0.2" withSources () withJavadoc (),
      // https://mvnrepository.com/artifact/net.openhft/zero-allocation-hashing
      "net.openhft" % "zero-allocation-hashing" % "0.15" withSources () withJavadoc (),
      // SPARK Testing Suites
      "org.apache.spark" %% "spark-core" % sparkVersion % Test withSources (),
      "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests" withSources (),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test withSources (),
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests" withSources (),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test withSources (),
      "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests" withSources (),
      "org.scalactic" %% "scalactic" % "3.2.12" withSources (),
      "org.scalatest" %% "scalatest" % "3.2.12" % "test" withSources ()
    )
  )
val sparkVersion = "3.2.1"
