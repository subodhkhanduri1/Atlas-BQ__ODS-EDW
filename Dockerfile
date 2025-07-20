  # Stage 1: Build the Java application
FROM sapmachine:21-jre-ubuntu AS build

# Set working directory inside the container
WORKDIR /app

# Copy the Maven project files
# Assuming you are using Maven for dependency management
COPY pom.xml .
RUN mkdir -p /src/main/java/ods_edw
COPY src/main/java/ods_edw/ODStoEDW_MoviesCDCSource.java ./src/main/java/ods_edw/ODStoEDW_MoviesCDCSource.java

# Download dependencies and build the JAR
# This step also caches dependencies, so subsequent builds are faster if pom.xml doesn't change
RUN apt-get update && apt-get install -y maven \
    && mvn clean package -DskipTests

# Stage 2: Create the final runtime image
FROM sapmachine:21-jre-ubuntu

# Set working directory inside the container
WORKDIR /app

# Copy the built JAR from the build stage
COPY --from=build /app/target/ods_edw-1.0-SNAPSHOT.jar ods_edw.jar

# Command to run the application
# This assumes your JAR is named 'ods_edw-1.0-SNAPSHOT.jar'
# and the main class is 'ODStoEDW_MoviesCDCSource'
# You will pass PROJECT_ID, TOPIC_ID, and MONGO_URI as arguments when running the container

# ENTRYPOINT ["java", "-jar", "ods_edw.jar"]
# Use CMD for arguments that can be overridden easily
CMD ["java", "-jar", "ods_edw.jar", "${PROJECT_ID}", "${TOPIC_ID}", "${MONGO_URI}"]
