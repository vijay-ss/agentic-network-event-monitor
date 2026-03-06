#!/bin/sh
set -e
ES_URL="http://elasticsearch:9200"

echo "Waiting for Elasticsearch to be ready..."
until curl -sf "$ES_URL/_cluster/health" > /dev/null; do
  sleep 5
done
echo "Elasticsearch ready."

create_index() {
  NAME=$1
  MAPPING_FILE=$2

  STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "$ES_URL/$NAME" || true)

  if [ "$STATUS" = "200" ]; then
    echo "Index already exists: $NAME — skipping"
  else
    echo "Creating index: $NAME"
    curl -sf -X PUT "$ES_URL/$NAME" \
      -H "Content-Type: application/json" \
      -d "@$MAPPING_FILE"
    echo ""
    echo "✓ Created: $NAME"
  fi
}

create_index "security-events" "/mappings/security-events-mapping.json"

create_index "security-aggregations" "/mappings/security-aggregations-mapping.json"

echo ""
echo "All Elasticsearch indices ready."
echo "Verify at: curl http://localhost:9200/_cat/indices?v"