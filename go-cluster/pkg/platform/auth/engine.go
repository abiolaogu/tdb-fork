package auth

import (
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lumadb/cluster/pkg/cluster"
	"go.uber.org/zap"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("expired token")
)

type Claims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
	jwt.RegisteredClaims
}

type AuthEngine struct {
	node      *cluster.Node
	logger    *zap.Logger
	secretKey []byte
}

func NewAuthEngine(node *cluster.Node, logger *zap.Logger) *AuthEngine {
	// In production, load from config/env
	return &AuthEngine{
		node:      node,
		logger:    logger,
		secretKey: []byte("luma-super-secret-key-change-me"),
	}
}

func (e *AuthEngine) Start() error {
	e.logger.Info("Auth Engine started")
	return nil
}

// GenerateToken creates a new JWT for a user
func (e *AuthEngine) GenerateToken(userID, role string) (string, error) {
	expirationTime := time.Now().Add(24 * time.Hour)
	claims := &Claims{
		UserID: userID,
		Role:   role,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			Issuer:    "luma-platform",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(e.secretKey)
}

// ValidateToken parses and validates a JWT
func (e *AuthEngine) ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
		return e.secretKey, nil
	})

	if err != nil {
		if err == jwt.ErrTokenExpired {
			return nil, ErrExpiredToken
		}
		return nil, err
	}

	if !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}
