package api

import (
	"net/http"

	"github.com/manifest-network/fred/internal/backend"
	"github.com/manifest-network/fred/internal/provisioner/callbackwire"
)

func (a *CallbackAuthenticator) VerifyRequest(request *http.Request) ([]byte, error) {
	proof, err := a.VerifyCallbackEvidence(request)
	if err != nil {
		return nil, err
	}
	return proof.Body(), nil
}

func (a *CallbackAuthenticator) VerifyCallbackRequest(
	request *http.Request,
) (backend.CallbackPayload, error) {
	proof, err := a.VerifyCallbackEvidence(request)
	if err != nil {
		return backend.CallbackPayload{}, err
	}
	observation, err := callbackwire.DecodeVerified(proof)
	if err != nil {
		return backend.CallbackPayload{}, err
	}
	return observation.Payload(), nil
}

func (a *CallbackKeyringAuthenticator) VerifyCallbackRequest(
	request *http.Request,
) (backend.CallbackPayload, error) {
	proof, err := a.VerifyCallbackEvidence(request)
	if err != nil {
		return backend.CallbackPayload{}, err
	}
	observation, err := callbackwire.DecodeVerified(proof)
	if err != nil {
		return backend.CallbackPayload{}, err
	}
	return observation.Payload(), nil
}
