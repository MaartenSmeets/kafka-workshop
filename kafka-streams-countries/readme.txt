java -cp target/Country-Events-Analyzer-1.0-SNAPSHOT.jar;target/dependency/* nl.amis.streams.countries.App

http://stackoverflow.com/questions/97640/force-maven2-to-copy-dependencies-into-target-lib

use this statement to have all JARs downloaded for the dependencies stemming from the pom.xml; the jars are downloaded to directory target/dependency

mvn install dependency:copy-dependencies 

