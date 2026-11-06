package backend

import "github.com/prometheus/client_golang/prometheus"

// requestOperation is the closed operation label of the client's request
// metrics. Its field is unexported, so a string literal cannot stand in for
// one, and declareRequestOperation is the only place one is built
// (TestRequestOperationsAreDeclaredOnce).
type requestOperation struct{ name string }

// String is the operation's metric label.
func (operation requestOperation) String() string { return operation.name }

// requestOperations is every operation the client sends, in declaration
// order. declareRequestOperation appends each one as it is declared, so the
// list cannot miss an operation the client can record.
var requestOperations []requestOperation

// declareRequestOperation declares one operation and adds it to
// requestOperations.
func declareRequestOperation(name string) requestOperation {
	operation := requestOperation{name: name}
	requestOperations = append(requestOperations, operation)
	return operation
}

var (
	requestGetInfo               = declareRequestOperation("get_info")
	requestProvision             = declareRequestOperation("provision")
	requestDeprovision           = declareRequestOperation("deprovision")
	requestListProvisions        = declareRequestOperation("list_provisions")
	requestGetProvision          = declareRequestOperation("get_provision")
	requestLookupProvisions      = declareRequestOperation("lookup_provisions")
	requestGetLogs               = declareRequestOperation("get_logs")
	requestRestart               = declareRequestOperation("restart")
	requestUpdate                = declareRequestOperation("update")
	requestRestore               = declareRequestOperation("restore")
	requestGetReleases           = declareRequestOperation("get_releases")
	requestReconcileCustomDomain = declareRequestOperation("reconcile_custom_domain")
	requestGetLoadStats          = declareRequestOperation("get_load_stats")
	requestListRetentions        = declareRequestOperation("list_retentions")
)

// RequestCounters are the counters of one backend operation: requests by
// status, and client-error responses whose body was not the declared JSON
// error envelope.
type RequestCounters struct {
	Success, Fenced, Pending, Error prometheus.Counter
	MalformedErrorBody              prometheus.Counter
}

// RequestCounterBinder creates the counters of one backend and operation.
type RequestCounterBinder func(backend, operation string) RequestCounters

// bindRequestCounters binds every operation's counters for one backend, so
// each series exists before the client's first request. A nil binder records
// nothing.
func bindRequestCounters(bind RequestCounterBinder, backend string) map[requestOperation]RequestCounters {
	if bind == nil {
		return nil
	}
	counters := make(map[requestOperation]RequestCounters, len(requestOperations))
	for _, operation := range requestOperations {
		counters[operation] = bind(backend, operation.name)
	}
	return counters
}

// statusCounter is the request counter of a call's metric status.
func (counters RequestCounters) statusCounter(status string) prometheus.Counter {
	switch status {
	case "success":
		return counters.Success
	case "fenced":
		return counters.Fenced
	case "pending":
		return counters.Pending
	default:
		return counters.Error
	}
}
