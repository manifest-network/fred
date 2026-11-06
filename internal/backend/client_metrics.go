package backend

import "github.com/prometheus/client_golang/prometheus"

// requestOperation is the closed operation label of the client's request
// metrics. Its field is unexported, so only the values below exist and a
// string literal cannot stand in for one.
type requestOperation struct{ name string }

// String is the operation's metric label.
func (operation requestOperation) String() string { return operation.name }

var (
	requestGetInfo               = requestOperation{"get_info"}
	requestProvision             = requestOperation{"provision"}
	requestDeprovision           = requestOperation{"deprovision"}
	requestListProvisions        = requestOperation{"list_provisions"}
	requestGetProvision          = requestOperation{"get_provision"}
	requestLookupProvisions      = requestOperation{"lookup_provisions"}
	requestGetLogs               = requestOperation{"get_logs"}
	requestRestart               = requestOperation{"restart"}
	requestUpdate                = requestOperation{"update"}
	requestRestore               = requestOperation{"restore"}
	requestGetReleases           = requestOperation{"get_releases"}
	requestReconcileCustomDomain = requestOperation{"reconcile_custom_domain"}
	requestGetLoadStats          = requestOperation{"get_load_stats"}
	requestListRetentions        = requestOperation{"list_retentions"}
)

// requestOperations is every operation the client sends. A client binds the
// counters of each one when it is built.
var requestOperations = [...]requestOperation{
	requestGetInfo, requestProvision, requestDeprovision, requestListProvisions,
	requestGetProvision, requestLookupProvisions, requestGetLogs, requestRestart,
	requestUpdate, requestRestore, requestGetReleases, requestReconcileCustomDomain,
	requestGetLoadStats, requestListRetentions,
}

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
