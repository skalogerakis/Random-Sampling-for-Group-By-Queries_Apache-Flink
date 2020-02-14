Data Source

-OpenAq
	https://openaq.org/

	-OpenAq_Data files fetched from
	https://api.openaq.org/v1/latest

	-Convert json formatted files to csv file for easier handling
	https://json-csv.com/

	-openaq_Bosnia.csv
	No need for convertion already a csv file downloaded from openaq


-Divvy Bikes
	https://www.divvybikes.com/system-data
	
	-Divvy_Trips_2018_Q1.zip contains .csv files with a lot of data


Flink Project

-Project Setup
	Directions: https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/projectsetup/java_api_quickstart.html

	Create project running by executing the following command(Give your artifactID, groupID)
	mvn archetype:generate                               \
      	-DarchetypeGroupId=org.apache.flink              \
      	-DarchetypeArtifactId=flink-quickstart-java      \
      	-DarchetypeVersion=1.10.0
	
	Note: We use maven project builder and java to build our template

	Import on Intellij Idea IDE
	
	-Import project using Maven
	-Change project SDK to Java 1.8
	-Change pom file using /Template_WordCount/pomInit (Note: Be careful with scala and flink version)
	-Add WordCount.java and WordCountData.java to make sure everything works
	-Proceed with next steps(Note: Always be careful and change main class name from pom.xml)
	-Also change resources folder for logger in file and not in console

-Run on cluster

	Go on project directory and run command
	mvn clean package

	A .jar file is created inside target folder.

	-Start cluster
		-Inside flink folder/bin/start-cluster.sh
	-Stop cluster
		-Inside flink folder/bin/stop-cluster.sh

	-Command Line Interface
	https://ci.apache.org/projects/flink/flink-docs-stable/ops/cli.html

		
	-Add task to cluster using the following

	<flink_folder>/bin run <jar_path>
	./flink run /home/skalogerakis/TUC_Projects/TUC_Advanced_Database_Systems/ECE622/target/ECE622-1.0-SNAPSHOT.jar






