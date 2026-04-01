FROM public.ecr.aws/docker/library/maven:3.9.4-eclipse-temurin-11-focal AS build-core
COPY . /app
RUN mvn clean install -DskipTests -f /app/pom.xml

FROM public.ecr.aws/docker/library/maven:3.9.4-eclipse-temurin-11-focal AS build-pipeline
COPY --from=build-core /root/.m2 /root/.m2
COPY . /app
RUN mvn clean package -DskipTests -f /app/pipeline/pom.xml

FROM public.ecr.aws/docker/library/flink:1.20-scala_2.12-java11 AS unified-image
USER flink
# flink-s3-fs-hadoop must live in its own plugins subfolder (not lib/) per Flink plugin loading rules.
# ${FLINK_VERSION} is set by the base image, ensuring the plugin version always matches the runtime.
RUN mkdir -p $FLINK_HOME/usrlib && \
    mkdir -p $FLINK_HOME/plugins/flink-s3-fs-hadoop && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
         -O $FLINK_HOME/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar
COPY --from=build-pipeline /app/pipeline/unified-pipeline/target/unified-pipeline-1.0.0.jar $FLINK_HOME/usrlib/

FROM public.ecr.aws/docker/library/flink:1.20-scala_2.12-java11 AS cache-indexer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib && \
    mkdir -p $FLINK_HOME/plugins/flink-s3-fs-hadoop && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar \
         -O $FLINK_HOME/plugins/flink-s3-fs-hadoop/flink-s3-fs-hadoop-${FLINK_VERSION}.jar
COPY --from=build-pipeline /app/pipeline/cache-indexer/target/cache-indexer-1.0.0.jar $FLINK_HOME/usrlib/