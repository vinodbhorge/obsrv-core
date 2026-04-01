FROM public.ecr.aws/docker/library/maven:3.9.4-eclipse-temurin-11-focal AS build-core
COPY . /app
RUN mvn clean install -DskipTests -f /app/pom.xml

FROM public.ecr.aws/docker/library/maven:3.9.4-eclipse-temurin-11-focal AS build-pipeline
COPY --from=build-core /root/.m2 /root/.m2
COPY . /app
RUN mvn clean package -DskipTests -f /app/pipeline/pom.xml

FROM public.ecr.aws/docker/library/flink:1.20-scala_2.12-java11 AS unified-image
USER flink
# Move the bundled flink-s3-fs-hadoop plugin from opt/ to the required plugins subfolder.
# This avoids a network download and guarantees the plugin version matches the runtime.
RUN mkdir -p $FLINK_HOME/usrlib && \
    mkdir -p $FLINK_HOME/plugins/flink-s3-fs-hadoop && \
    mv $FLINK_HOME/opt/flink-s3-fs-hadoop-*.jar $FLINK_HOME/plugins/flink-s3-fs-hadoop/
COPY --from=build-pipeline /app/pipeline/unified-pipeline/target/unified-pipeline-1.0.0.jar $FLINK_HOME/usrlib/

FROM public.ecr.aws/docker/library/flink:1.20-scala_2.12-java11 AS cache-indexer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib && \
    mkdir -p $FLINK_HOME/plugins/flink-s3-fs-hadoop && \
    mv $FLINK_HOME/opt/flink-s3-fs-hadoop-*.jar $FLINK_HOME/plugins/flink-s3-fs-hadoop/
COPY --from=build-pipeline /app/pipeline/cache-indexer/target/cache-indexer-1.0.0.jar $FLINK_HOME/usrlib/