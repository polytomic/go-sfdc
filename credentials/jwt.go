package credentials

import (
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	JwtExpiration = 3 * time.Minute
)

type jwtProvider struct {
	creds JwtCredentials
}

func (provider *jwtProvider) Retrieve() (io.Reader, error) {
	expirationTime := provider.GetAppropriateExpirationTime()
	tokenString, err := provider.BuildClaimsToken(expirationTime, provider.creds.URL, provider.creds.ClientID, provider.creds.Username)
	if err != nil {
		return nil, fmt.Errorf("jwtProvider.Retrieve() error: %w", err)
	}

	form := url.Values{}
	form.Add("grant_type", string(jwtGrantType))
	form.Add("assertion", tokenString)
	return strings.NewReader(form.Encode()), nil
}

func (provider *jwtProvider) URL() string {
	return provider.creds.URL
}

func (provider *jwtProvider) GetAppropriateExpirationTime() int64 {
	return time.Now().Add(JwtExpiration).Unix()
}

// BuildClaimsToken build the actual claims token required for authentication
func (provider *jwtProvider) BuildClaimsToken(expirationTime int64, url string, clientID string, username string) (string, error) {
	claims := &jwt.MapClaims{
		"exp": jwt.NewNumericDate(time.Unix(expirationTime, 0)),
		"aud": url,
		"iss": clientID,
		"sub": username,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(provider.creds.ClientKey)
}
