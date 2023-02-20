import sbt.librarymanagement.InclExclRule

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.scylladb",
      scalaVersion := "2.12.14"
    )),
  name := "scylla-migrator",
  version := "0.0.1",
  mainClass := Some("com.scylladb.migrator.Migrator"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  javaOptions ++= Seq(
    "-Xms512M",
    "-Xmx2048M",
    "-XX:MaxPermSize=2048M",
    "-XX:+CMSClassUnloadingEnabled"),
  scalacOptions ++= Seq("-deprecation", "-unchecked", "-Ypartial-unification"),
  parallelExecution in Test := false,
  fork := true,
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-streaming" % "3.1.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.1.1" % "provided",
    "org.yaml" % "snakeyaml" % "1.33",
    "io.circe" %% "circe-yaml" % "0.10.0",
    "io.circe" %% "circe-generic" % "0.10.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test",
    "com.google.cloud" % "google-cloud-storage" % "1.100.0"

  ),
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("org.yaml.snakeyaml.**" -> "com.scylladb.shaded.@1").inAll
  ),
  assemblyMergeStrategy in assembly := {
    case PathList("org", "joda", "time", _@_*) => MergeStrategy.first
    case PathList("org", "apache", "commons", "logging", _@_*) => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "annotation", _@_*) => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "core", _@_*) => MergeStrategy.first
    case PathList("com", "fasterxml", "jackson", "databind", _@_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },
  // uses compile classpath for the run task, including "provided" jar (cf http://stackoverflow.com/a/21803413/3827)
  run in Compile := Defaults
    .runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
    .evaluated,
  scalacOptions ++= Seq("-deprecation", "-unchecked"),
  pomIncludeRepository := { x =>
    false
  },
  resolvers ++= Seq(
    "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
    "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/",
    "Second Typesafe repo" at "https://repo.typesafe.com/typesafe/maven-releases/",
    Resolver.sonatypeRepo("public")
  ),
  pomIncludeRepository := { x =>
    false
  },
  // publish settings
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
)
