package models

type LogEntry struct {
	ID                 string              `json:"id"`
	Timestamp          string              `json:"timestamp"`
	SourceIP           string              `json:"source_ip"`
	DestinationIP      string              `json:"destination_ip"`
	DestinationDomain  *string             `json:"destination_domain,omitempty"`
	SourcePort         int                 `json:"source_port"`
	DestinationPort    int                 `json:"destination_port"`
	Protocol           string              `json:"protocol"`
	Application        string              `json:"application"`
	Action             string              `json:"action"`
	BytesSent          *int64              `json:"bytes_sent,omitempty"`
	BytesReceived      *int64              `json:"bytes_received,omitempty"`
	PacketCount        *int                `json:"packet_count,omitempty"`
	DurationMs         *int                `json:"duration_ms,omitempty"`
	EventType          string              `json:"event_type"`
	Severity           string              `json:"severity"`
	User               *string             `json:"user,omitempty"`
	CorrelationID      *string             `json:"correlation_id,omitempty"`
	DeviceName         *string             `json:"device_name,omitempty"`
	VlanID             *int                `json:"vlan_id,omitempty"`
	GeoLocation        *string             `json:"geo_location,omitempty"`
	UserAgent          *string             `json:"user_agent,omitempty"`
	LogMetadata        *LogMetadata        `json:"log_metadata,omitempty"`
	NetworkDetails     *NetworkDetails     `json:"network_details,omitempty"`
	ConnectionState    *string             `json:"connection_state,omitempty"`
	TCPFlags           *string             `json:"tcp_flags,omitempty"`
	PolicyViolation    *PolicyViolation    `json:"policy_violation,omitempty"`
	AssetTags          []string            `json:"asset_tags,omitempty"`
	SessionDetails     interface{}         `json:"session_details,omitempty"`
	ResourceUtilization *ResourceUtilization `json:"resource_utilization,omitempty"`
	ResponseCode       *int                `json:"response_code,omitempty"`
	ThreatIndicators   []string            `json:"threat_indicators,omitempty"`
	RuleID             *string             `json:"rule_id,omitempty"`
	Description        *string             `json:"description,omitempty"`
	Reason             *string             `json:"reason,omitempty"`
	AlertID            *string             `json:"alert_id,omitempty"`
	FileHash           *string             `json:"file_hash,omitempty"`
	MalwareFamily      *string             `json:"malware_family,omitempty"`
}

type LogMetadata struct {
	Collector      string  `json:"collector"`
	SourceType     string  `json:"source_type"`
	IngestionTime  string  `json:"ingestion_time"`
	Pipeline       *string `json:"pipeline,omitempty"`
}

type NetworkDetails struct {
	NetworkSegment string  `json:"network_segment"`
	Subnet         string  `json:"subnet"`
	SecurityZone   *string `json:"security_zone,omitempty"`
}

type PolicyViolation struct {
	PolicyName     string  `json:"policy_name"`
	ViolationType  string  `json:"violation_type"`
	PolicySeverity *string `json:"policy_severity,omitempty"`
}

type ResourceUtilization struct {
	CPUPercent    *float64 `json:"cpu_percent,omitempty"`
	MemoryPercent *float64 `json:"memory_percent,omitempty"`
	DiskIOPercent *float64 `json:"disk_io_percent,omitempty"`
	ProcessName   *string  `json:"process_name,omitempty"`
	ProcessID     *int     `json:"process_id,omitempty"`
}