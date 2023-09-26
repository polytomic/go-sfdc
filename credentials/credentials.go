package credentials

import (
	"crypto/rsa"
	"errors"
	"io"
)

// PasswordCredentials is a structure for the OAuth credentials
// that are needed to authenticate with a Salesforce org.
//
// URL is the login URL used, examples would be https://test.salesforce.com or https://login.salesforce.com
//
// Username is the Salesforce user name for logging into the org.
//
// Password is the Salesforce password for the user.
//
// ClientID is the client ID from the connected application.
//
// ClientSecret is the client secret from the connected application.
type PasswordCredentials struct {
	URL          string
	Username     string
	Password     string
	ClientID     string
	ClientSecret string
}

// JwtCredentials is a structure for the Jwt credentials
// that are needed to authenticate with a Salesforce org.
//
// URL is the login URL used, examples would be https://test.salesforce.com or https://login.salesforce.com
//
// # ClientID the client id as defined in the connected app is Salesforce
//
// # Username is the client username
//
// ClientKey is the client DSA key uploaded for authentication in the ConnectedApp
type JwtCredentials struct {
	URL       string
	ClientID  string
	Username  string
	ClientKey *rsa.PrivateKey
}

// Credentials is the structure that contains all of the
// information for creating a session.
type Credentials struct {
	provider Provider
}

// Provider is the interface that is able to provide the
// session creator with all of the valid information.
//
// Retrieve will return the reader for the HTTP request body.
//
// URL is the URL base for the session endpoint.
type Provider interface {
	Retrieve() (io.Reader, error)
	URL() string
}

type grantType string

const (
	passwordGrantType grantType = "password"
	jwtGrantType      grantType = "urn:ietf:params:oauth:grant-type:jwt-bearer"
)

// Retrieve will return the reader for the HTTP request body.
func (creds *Credentials) Retrieve() (io.Reader, error) {
	return creds.provider.Retrieve()
}

// URL is the URL base for the session endpoint.
func (creds *Credentials) URL() string {
	return creds.provider.URL()
}

// NewCredentials will create a credential with the custom provider.
func NewCredentials(provider Provider) (*Credentials, error) {
	if provider == nil {
		return nil, errors.New("credentials: the provider can not be nil")
	}
	return &Credentials{
		provider: provider,
	}, nil
}

// NewPasswordCredentials will create a credential with the password credentials.
func NewPasswordCredentials(creds PasswordCredentials) (*Credentials, error) {
	if err := validatePasswordCredentials(creds); err != nil {
		return nil, err
	}
	return &Credentials{
		provider: &passwordProvider{
			creds: creds,
		},
	}, nil
}

func validatePasswordCredentials(cred PasswordCredentials) error {
	if cred.URL == "" {
		return errors.New("credentials: URL can not be empty")
	}
	if cred.Username == "" {
		return errors.New("credentials: username can not be empty")
	}
	if cred.Password == "" {
		return errors.New("credentials: password can not be empty")
	}
	if cred.ClientID == "" {
		return errors.New("credentials: client ID can not be empty")
	}
	if cred.ClientSecret == "" {
		return errors.New("credentials: client secret can not be empty")
	}
	return nil
}

// NewJWTCredentials will create a credentials with all required info about generating a JWT claims parameter
func NewJWTCredentials(creds JwtCredentials) (*Credentials, error) {
	if err := validateJWTCredentials(creds); err != nil {
		return nil, err
	}
	return &Credentials{
		provider: &jwtProvider{
			creds: creds,
		},
	}, nil
}
func validateJWTCredentials(creds JwtCredentials) error {
	switch {
	case len(creds.URL) == 0:
		return errors.New("credentials: URL can not be empty")
	case creds.ClientKey == nil:
		return errors.New("credentials: client key can not be empty")
	case len(creds.Username) == 0:
		return errors.New("credentials: client username can not be empty")
	case len(creds.ClientID) == 0:
		return errors.New("credentials: client ID can not be empty")
	}
	return nil
}
