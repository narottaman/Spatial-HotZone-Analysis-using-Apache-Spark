# Assignment-3-ngangada

## Project Description
This project is part of CSE 511 (Data Processing at Scale) course. It is designed to perform spatial hot spot analysis using Apache Spark. The project includes:
- **Hot Zone Analysis**: A range join operation on a rectangle dataset and a point dataset to determine the 'hotness' of each rectangle based on the number of points within.
- **Hot Cell Analysis**: Applies spatial statistics to spatio-temporal big data to identify statistically significant spatial hot spots using the Getis-Ord Gi* statistic.

## Installation

### Prerequisites
- Apache Spark 2.4.x or 3.x
- Scala 2.11.x or 2.12.x
- Java 8 or 11
- sbt (Scala Build Tool)
- Docker (optional for containerized environment)

### Building the Project
To compile the project and create an executable jar:
```bash
sbt clean compile assembly
```
##This will produce a fat JAR file in the target/scala-2.11/ directory.

###Running the Application
Using Docker (Optional)
Start the Docker container which has all the required packages installed:
```bash
docker run -it veedata/cse511-assignment3:latest
```
###Running on Spark
Submit the application to Spark using the following command:

```bash
spark-submit /root/cse511/target/scala-2.11/CSE511-Hotspot-Analysis-assembly-0.1.0.jar result/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow-trip-sample-100000.csv
```
###Moving JAR file with Docker
To copy the compiled JAR file from a Docker container to your local machine:

```bash
docker cp 0628bedd9cf39333b9521d646585be584cd7e80478926bafde67d2bcd19f61eb:/root/cse511/target/scala-2.11/CSE511-Hotspot-Analysis-assembly-0.1.0.jar $HOME/Downloads/Assignment3/
```

###Testing
Ensure the application functions correctly by running it against sample data provided in the src/main/resources/ directory.
```bash
spark-submit /root/cse511/target/scala-2.11/CSE511-Hotspot-Analysis-assembly-0.1.0.jar result/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow-trip-sample-100000.csv
```