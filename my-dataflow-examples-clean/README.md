To get the code run

mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.4.0 \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false

First run MinimalWordCount
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MinimalWordCount

Here is a verbose version updating a smaller file
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.a_MinimalWordCount


mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=counts" -Pdirect-runner

