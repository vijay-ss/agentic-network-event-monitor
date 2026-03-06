# Registers all Kafka Connect sink connectors via REST API.
# Called by the kafka-connect-init container on startup.
#
# Flush settings are tuned per environment via ENV_MODE:
#   ENV_MODE=dev  (default) — flush frequently, good for local testing
#   ENV_MODE=prod           — larger batches, fewer files, better for production
#
# Override individual settings directly:
#   ENV_MODE=prod FLUSH_SIZE=5000 ROTATE_INTERVAL_MS=300000 docker compose up
set -e
CONNECT_URL="http://kafka-connect:8083"
ENV_MODE="${ENV_MODE:-dev}"

# ── Flush settings per environment ────────────────────────────────────────────
if [ "$ENV_MODE" = "prod" ]; then
  BRONZE_FLUSH_SIZE="${FLUSH_SIZE:-5000}"
  BRONZE_ROTATE_MS="${ROTATE_INTERVAL_MS:-300000}"
  BRONZE_SCHEDULE_MS="3600000"

  SILVER_FLUSH_SIZE="${FLUSH_SIZE:-5000}"
  SILVER_ROTATE_MS="${ROTATE_INTERVAL_MS:-300000}"
  SILVER_SCHEDULE_MS="3600000"

  GOLD_FLUSH_SIZE="${FLUSH_SIZE:-1000}"
  GOLD_ROTATE_MS="${ROTATE_INTERVAL_MS:-60000}"
  GOLD_SCHEDULE_MS="600000"
else
  BRONZE_FLUSH_SIZE="${FLUSH_SIZE:-100}"
  BRONZE_ROTATE_MS="${ROTATE_INTERVAL_MS:-30000}"
  BRONZE_SCHEDULE_MS="60000"

  SILVER_FLUSH_SIZE="${FLUSH_SIZE:-100}"
  SILVER_ROTATE_MS="${ROTATE_INTERVAL_MS:-30000}"
  SILVER_SCHEDULE_MS="60000"

  GOLD_FLUSH_SIZE="${FLUSH_SIZE:-50}"
  GOLD_ROTATE_MS="${ROTATE_INTERVAL_MS:-10000}"
  GOLD_SCHEDULE_MS="20000"
fi

echo "Waiting for Kafka Connect to be ready..."
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  sleep 5
done
echo "Kafka Connect ready. Mode: $ENV_MODE"
echo "  Bronze: flush.size=$BRONZE_FLUSH_SIZE  rotate=${BRONZE_ROTATE_MS}ms  schedule=${BRONZE_SCHEDULE_MS}ms"
echo "  Silver: flush.size=$SILVER_FLUSH_SIZE  rotate=${SILVER_ROTATE_MS}ms  schedule=${SILVER_SCHEDULE_MS}ms"
echo "  Gold:   flush.size=$GOLD_FLUSH_SIZE    rotate=${GOLD_ROTATE_MS}ms    schedule=${GOLD_SCHEDULE_MS}ms"


# ── Helper: register or update connector (idempotent) ─────────────────────────
# Uses PUT /config so it's safe to re-run on container restart — won't fail
# if connector already exists, just updates config.
register_connector() {
  NAME=$1
  CONFIG=$2
  STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$NAME" || true)

  if [ "$STATUS" = "200" ]; then
    echo "  Updating existing connector: $NAME"
    curl -sf -X PUT "$CONNECT_URL/connectors/$NAME/config" \
      -H "Content-Type: application/json" \
      -d "$CONFIG" > /dev/null
  else
    echo "  Registering new connector: $NAME"
    WRAPPED=$(printf '{"name":"%s","config":%s}' "$NAME" "$CONFIG")
    curl -sf -X POST "$CONNECT_URL/connectors" \
      -H "Content-Type: application/json" \
      -d "$WRAPPED" > /dev/null
  fi
}


# ── Bronze → MinIO ────────────────────────────────────────────────────────────
register_connector "minio-bronze-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.bronze.events.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$BRONZE_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$BRONZE_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$BRONZE_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"timestamp\",
  \"topics.dir\":                        \"bronze\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.parse-errors.v1\"
}"
echo "Bronze sink registered"


# ── Silver → MinIO ────────────────────────────────────────────────────────────
register_connector "minio-silver-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.silver.events.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$SILVER_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$SILVER_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$SILVER_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"event_time\",
  \"topics.dir\":                        \"silver\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.clean-errors.v1\"
}"
echo "Silver sink registered"


# ── Gold → MinIO ──────────────────────────────────────────────────────────────
register_connector "minio-gold-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.gold.assessed-events.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$GOLD_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$GOLD_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$GOLD_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"silver.event_time\",
  \"topics.dir\":                        \"gold\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.enrich-errors.v1\"
}"
echo "Gold sink registered"


# ── Aggregated: Windowed → MinIO ──────────────────────────────────────────────
register_connector "minio-aggregated-windowed-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.aggregated.windowed-events.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$GOLD_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$GOLD_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$GOLD_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"event_time\",
  \"topics.dir\":                        \"aggregated/windowed\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.enrich-errors.v1\"
}"
echo "Aggregated windowed sink registered"


# ── Aggregated: Correlated → MinIO ────────────────────────────────────────────
register_connector "minio-aggregated-correlated-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.aggregated.correlated-events.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$GOLD_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$GOLD_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$GOLD_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"event_time\",
  \"topics.dir\":                        \"aggregated/correlated\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.enrich-errors.v1\"
}"
echo "Aggregated correlated sink registered"


# ── Aggregated: Baseline Alerts → MinIO ───────────────────────────────────────
register_connector "minio-aggregated-baseline-sink" "{
  \"connector.class\":                   \"io.confluent.connect.s3.S3SinkConnector\",
  \"tasks.max\":                         \"1\",
  \"topics\":                            \"logs.aggregated.baseline-alerts.v1\",
  \"s3.region\":                         \"us-east-1\",
  \"s3.bucket.name\":                    \"security-lake\",
  \"s3.part.size\":                      \"5242880\",
  \"store.url\":                         \"http://minio:9000\",
  \"storage.class\":                     \"io.confluent.connect.s3.storage.S3Storage\",
  \"format.class\":                      \"io.confluent.connect.s3.format.json.JsonFormat\",
  \"flush.size\":                        \"$GOLD_FLUSH_SIZE\",
  \"rotate.interval.ms\":                \"$GOLD_ROTATE_MS\",
  \"rotate.schedule.interval.ms\":       \"$GOLD_SCHEDULE_MS\",
  \"locale\":                            \"en_US\",
  \"timezone\":                          \"UTC\",
  \"timestamp.extractor\":               \"RecordField\",
  \"timestamp.field\":                   \"event_time\",
  \"topics.dir\":                        \"aggregated/baseline\",
  \"path.format\":                       \"'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH\",
  \"aws.access.key.id\":                 \"minioadmin\",
  \"aws.secret.access.key\":             \"minioadmin\",
  \"s3.ssea.name\":                      \"\",
  \"errors.tolerance\":                  \"all\",
  \"errors.log.enable\":                 \"true\",
  \"errors.log.include.messages\":       \"true\",
  \"errors.deadletterqueue.topic.name\": \"logs.dead.enrich-errors.v1\"
}"
echo "Aggregated baseline sink registered"


echo ""
echo "All connectors registered. Check status:"
echo "  curl http://localhost:8083/connectors/minio-bronze-sink/status | python3 -m json.tool"