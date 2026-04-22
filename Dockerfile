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
# Use IRSA/OIDC (Web Identity Token) for S3 auth instead of static access keys.
# EKS injects AWS_ROLE_ARN and AWS_WEB_IDENTITY_TOKEN_FILE into pods whose service
# account has an IAM role annotation; WebIdentityTokenCredentialsProvider reads them.
RUN if [ -f "$FLINK_HOME/conf/config.yaml" ]; then \
        echo 's3.aws.credentials.provider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider' >> $FLINK_HOME/conf/config.yaml; \
    else \
        echo 's3.aws.credentials.provider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider' >> $FLINK_HOME/conf/flink-conf.yaml; \
    fi
COPY --from=build-pipeline /app/pipeline/unified-pipeline/target/unified-pipeline-1.0.0.jar $FLINK_HOME/usrlib/

FROM public.ecr.aws/docker/library/flink:1.20-scala_2.12-java11 AS cache-indexer-image
USER flink
RUN mkdir -p $FLINK_HOME/usrlib && \
    mkdir -p $FLINK_HOME/plugins/flink-s3-fs-hadoop && \
    mv $FLINK_HOME/opt/flink-s3-fs-hadoop-*.jar $FLINK_HOME/plugins/flink-s3-fs-hadoop/
RUN if [ -f "$FLINK_HOME/conf/config.yaml" ]; then \
        echo 's3.aws.credentials.provider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider' >> $FLINK_HOME/conf/config.yaml; \
    else \
        echo 's3.aws.credentials.provider: com.amazonaws.auth.WebIdentityTokenCredentialsProvider' >> $FLINK_HOME/conf/flink-conf.yaml; \
    fi
COPY --from=build-pipeline /app/pipeline/cache-indexer/target/cache-indexer-1.0.0.jar $FLINK_HOME/usrlib/