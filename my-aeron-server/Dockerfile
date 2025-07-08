FROM openjdk:20-jdk
LABEL authors="Osseum1"
COPY ./target/my-aeron-server-1.0-SNAPSHOT-jar-with-dependencies.jar /server.jar

ENTRYPOINT ["java", "-cp", "server.jar", "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-exports", "java.base/jdk.internal.misc=ALL-UNNAMED", "org.example.Main"]
