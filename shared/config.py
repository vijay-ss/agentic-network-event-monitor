import os
from pydantic import BaseModel


class KafkaConfig(BaseModel):
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic_bronze:      str = "logs.bronze.events.v1"
    topic_silver:      str = "logs.silver.events.v1"
    topic_gold:        str = "logs.gold.assessed-events.v1"
    topic_dead_parse:  str = "logs.dead.parse-errors.v1"
    topic_dead_clean:  str = "logs.dead.clean-errors.v1"
    topic_dead_enrich: str = "logs.dead.enrich-errors.v1"
    topic_aggregated_windowed:    str = "logs.aggregated.windowed-events.v1"
    topic_aggregated_correlated:  str = "logs.aggregated.correlated-events.v1"
    topic_aggregated_baseline:    str = "logs.aggregated.baseline-alerts.v1"
    group_cleaner:     str = "security.cleaner.v1"
    group_enricher:    str = "security.enricher.v1"
    group_flink:       str = "security.flink.aggregator.v1"
    group_es:          str = "security.elasticsearch-sink.v1"
    group_postgres:    str = "security.postgres-sink.v1"

    auto_offset_reset: str = "earliest"
    max_poll_records:  int = 100


class OllamaConfig(BaseModel):
    base_url:    str   = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    model:       str   = os.getenv("OLLAMA_MODEL", "llama3.1")
    temperature: float = 0.1
    max_tokens:  int   = 1024


class ElasticsearchConfig(BaseModel):
    host:  str = os.getenv("ELASTICSEARCH_HOST", "http://localhost:9200")
    index: str = "security-events"
    index_aggregations: str = "security-aggregations"
    alias: str = "security-events-active"


class PostgresConfig(BaseModel):
    host:     str = os.getenv("POSTGRES_HOST", "localhost")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "security")
    user:     str = os.getenv("POSTGRES_USER", "security")
    password: str = os.getenv("POSTGRES_PASSWORD", "security")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class MinIOConfig(BaseModel):
    endpoint:   str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket:     str = os.getenv("MINIO_BUCKET", "security-lake")


class EnrichmentAPIConfig(BaseModel):
    abuseipdb_key:  str = os.getenv("ABUSEIPDB_API_KEY", "")
    otx_key:        str = os.getenv("OTX_API_KEY", "")
    ipinfo_token:   str = os.getenv("IPINFO_TOKEN", "")
    virustotal_key: str = os.getenv("VIRUSTOTAL_API_KEY", "")


class FlinkConfig(BaseModel):
    window_minutes:        int = int(os.getenv("FLINK_WINDOW_MINUTES", "5"))
    cep_window_minutes:    int = int(os.getenv("FLINK_CEP_WINDOW_MINUTES", "10"))
    velocity_threshold:    int = int(os.getenv("FLINK_VELOCITY_THRESHOLD", "10"))
    baseline_multiplier: float = float(os.getenv("FLINK_BASELINE_MULTIPLIER", "3.0"))
    baseline_window_count: int = int(os.getenv("FLINK_BASELINE_WINDOW_COUNT", "12"))


class PipelineConfig(BaseModel):
    kafka:       KafkaConfig         = KafkaConfig()
    ollama:      OllamaConfig        = OllamaConfig()
    elastic:     ElasticsearchConfig = ElasticsearchConfig()
    postgres:    PostgresConfig      = PostgresConfig()
    minio:       MinIOConfig         = MinIOConfig()
    enrichment:  EnrichmentAPIConfig = EnrichmentAPIConfig()
    flink:       FlinkConfig         = FlinkConfig()

    thresholds: dict = {
        "critical": 80,
        "high":     60,
        "medium":   40,
        "low":      20,
    }

config = PipelineConfig()