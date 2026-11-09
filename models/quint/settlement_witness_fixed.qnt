module settlement_witness_fixed {
  import settlement(SKIP_TIMEOUT_AFTER_SUCCESS = true, TIMEOUT_REJECTS = true).* from "./lease_settlement"
  action witnessStep =
    if (not(dispatched)) dispatch
    else if (be == BProvisioning) backendWork
    else if (not(timedOut)) timePasses
    else if (not(successAuthenticated)) cbClaim
    else if (cb == CbSuccess) cbSuccess
    else timeoutSettle
}
