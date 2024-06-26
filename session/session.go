// Package session provides handles creation of a Salesforce session
package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/namely/go-sfdc/v3"
	"github.com/namely/go-sfdc/v3/credentials"
)

const (
	// CallOptionsHeader defines the header name to use when passing call
	// options with a request.
	CallOptionsHeader string = "Sforce-Call-Options"
)

// Session is the authentication response.  This is used to generate the
// authorization header for the Salesforce API calls.
type Session struct {
	// thread safe:
	config sfdc.Configuration

	// thread unsafe:
	mu        sync.RWMutex
	response  *sessionPasswordResponse
	expiresAt time.Time
}

// Clienter interface provides the HTTP client used by the the resources.
type Clienter interface {
	Client() *http.Client
}

// InstanceFormatter is the session interface that formats the session
// instance information used by the resources.
type InstanceFormatter interface {
	// InstanceURL will returns the Salesforce hostname as a URL; for example,
	// https://na42.salesforce.com.
	//
	// The return value of InstanceURL is suitable for use as the base of API URLs
	InstanceURL() string
	// AuthorizationHeader will add the authorization to the HTTP request's header.
	AuthorizationHeader(*http.Request)
	Refresh(context.Context) error
	Clienter
}

// ServiceFormatter is the session interface that formats the session for
// service resources.
type ServiceFormatter interface {
	InstanceFormatter
	// Version will return the Salesforce API version for this session.
	Version() int
	// DataServiceURL returns the URL for the Salesforce Data service; for
	// example, https://na42.salesforce.com/services/data/v42.0.
	DataServiceURL() string
}

type sessionPasswordResponse struct {
	AccessToken string `json:"access_token"`
	InstanceURL string `json:"instance_url"`
	ID          string `json:"id"`
	TokenType   string `json:"token_type"`
	IssuedAt    string `json:"issued_at"`
	Signature   string `json:"signature"`
}

const (
	oauthEndpoint          = "/services/oauth2/token"
	defaultSessionDuration = 24 * time.Hour
)

// Open is used to authenticate with Salesforce and open a session.  The user will need to
// supply the proper credentials and a HTTP client.
func Open(ctx context.Context, config sfdc.Configuration) (*Session, error) {
	if config.Credentials == nil {
		return nil, errors.New("session: configuration credentials can not be nil")
	}
	if config.Client == nil {
		return nil, errors.New("session: configuration client can not be nil")
	}
	if config.Version <= 0 {
		return nil, errors.New("session: configuration version can not be less than zero")
	}
	if config.SessionDuration == 0 {
		config.SessionDuration = defaultSessionDuration
	}

	session := &Session{
		config: config,
	}

	err := session.refresh(ctx)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func passwordSessionRequest(ctx context.Context, creds *credentials.Credentials) (*http.Request, error) {
	oauthURL := creds.URL() + oauthEndpoint

	body, err := creds.Retrieve()
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, oauthURL, body)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Accept", "application/json, */*")
	return request, nil
}

func passwordSessionResponse(request *http.Request, client *http.Client) (*sessionPasswordResponse, error) {
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("session response: %w", sfdc.HandleError(response))
	}

	var sessionResponse sessionPasswordResponse
	err = json.NewDecoder(response.Body).Decode(&sessionResponse)
	if err != nil {
		return nil, err
	}

	return &sessionResponse, nil
}

// InstanceURL will return the Salesforce instance
// from the session authentication.
func (s *Session) InstanceURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.response.InstanceURL
}

// Version will return the Salesforce API version for this session.
func (s *Session) Version() int {
	return s.config.Version
}

// DataServiceURL will return the Salesforce instance for the service URL.
func (s *Session) DataServiceURL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return fmt.Sprintf("%s/services/data/v%d.0", s.response.InstanceURL, s.config.Version)
}

// AuthorizationHeader will add the authorization to the
// HTTP request's header.
func (s *Session) AuthorizationHeader(req *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	auth := s.response.TokenType + " " + s.response.AccessToken
	req.Header.Add("Authorization", auth)
}

// Client returns the HTTP client to be used in APIs calls.
func (s *Session) Client() *http.Client {
	return s.config.Client
}

// Refresh check if session is expired and refresh it if needed.
func (s *Session) Refresh(ctx context.Context) error {
	if s.isExpired() {
		return s.refresh(ctx)
	}

	return nil
}

func (s *Session) isExpired() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.expiresAt.Before(time.Now().UTC())
}

// refresh the session
func (s *Session) refresh(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	req, err := passwordSessionRequest(ctx, s.config.Credentials)
	if err != nil {
		return err
	}

	resp, err := passwordSessionResponse(req, s.config.Client)
	if err != nil {
		return err
	}

	s.response = resp
	s.expiresAt = time.Now().Add(s.config.SessionDuration).UTC()

	return nil
}
