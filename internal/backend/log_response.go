package backend

const (
	// MaxLogContentBytes is the aggregate content budget for one log retrieval,
	// shared by live container reads and retained failed-attempt diagnostics.
	MaxLogContentBytes = 32 << 20

	// Failed-attempt records bound both entry count and key size before storage.
	MaxFailureDiagnosticEntries  = 4096
	MaxFailureDiagnosticKeyBytes = 256

	// AggregateLogLimitMessage also bounds the per-entry placeholder overhead
	// left by the live API after it exhausts the content budget.
	AggregateLogLimitMessage = "[log truncated: aggregate log size limit reached]"

	// A response may merge the current runtime and one failed-attempt record.
	// Live keys are DNS service labels (63 bytes), '/' and an instance number;
	// the larger diagnostic key bound covers both, including the failed/ prefix.
	maxProjectedLogEntries  = MaxOperationQuantity + MaxFailureDiagnosticEntries
	maxProjectedLogKeyBytes = MaxFailureDiagnosticKeyBytes + len("failed/")

	// JSON can expand each input byte to six bytes (control characters, HTML
	// escaping or invalid UTF-8). Include keys, per-entry markers, object framing
	// and the encoder's trailing newline, not just the unescaped content budget.
	MaxProjectedLogsResponseBytes = 6*(MaxLogContentBytes+maxProjectedLogEntries*(maxProjectedLogKeyBytes+len("\n")+len(AggregateLogLimitMessage))) +
		6*maxProjectedLogEntries + 3
)
