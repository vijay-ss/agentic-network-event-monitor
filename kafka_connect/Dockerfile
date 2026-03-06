FROM confluentinc/cp-kafka-connect:7.6.0

# Pre-install the S3 sink connector during image build rather than at
# container startup. This makes startup faster and more reliable —
# no runtime internet dependency for the connector plugin.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest

# Also install the Elasticsearch and JDBC (Postgres) sink connectors
# so Kafka Connect can write directly to those stores if needed later.
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:latest
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest