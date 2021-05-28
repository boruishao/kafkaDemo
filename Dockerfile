FROM dockette/mvn
WORKDIR /demo
COPY pom.xml pom.xml
RUN mvn dependency:go-offline
COPY . .
RUN mvn install
CMD ["java","-jar","target/demo-0.0.1-SNAPSHOT.jar"]