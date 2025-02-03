package ratelimitergo

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Algorithm represents the rate limiting algorithm type
type Algorithm string

const (
	TokenBucket   Algorithm = "token_bucket"
	LeakyBucket   Algorithm = "leaky_bucket"
	SlidingWindow Algorithm = "sliding_window"
	FixedWindow   Algorithm = "fixed_window"
)

// RateLimiter interface defines the common behavior for all rate limiters
type RateLimiter interface {
	Allow(key string) bool
	AllowN(key string, n int) bool
}

// Config holds the configuration for rate limiters
type Config struct {
	Algorithm  Algorithm
	Rate       int           // Requests per second
	Burst      int           // Maximum burst size
	WindowSize time.Duration // For sliding/fixed window algorithms
}

// distributed rate limiter implementation using Redis
type DistributedRateLimiter struct {
	mu     sync.RWMutex
	config Config
	store  Storage
}

// Storage interface for different backend storages (Redis, Memcached, etc.)
type Storage interface {
	Increment(ctx context.Context, key string, value int64) (int64, error)
	Decrement(ctx context.Context, key string, value int64) (int64, error)
	Get(ctx context.Context, key string) (int64, error)
	Set(ctx context.Context, key string, value int64, expiry time.Duration) error
	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZRemRangeByScore(ctx context.Context, key string, min, max float64) error
	ZCount(ctx context.Context, key string, min, max float64) (int64, error)
}

// RedisStorage implements Storage interface using Redis
type RedisStorage struct {
	client RedisClient
}

// RedisClient interface to make testing easier
type RedisClient interface {
	Incr(ctx context.Context, key string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value interface{}, expiry time.Duration) error
	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZRemRangeByScore(ctx context.Context, key string, min, max float64) error
	ZCount(ctx context.Context, key string, min, max float64) (int64, error)
}

// NewDistributedRateLimiter creates a new rate limiter with the specified algorithm
func NewDistributedRateLimiter(config Config, storage Storage) (*DistributedRateLimiter, error) {
	if config.Rate <= 0 {
		return nil, fmt.Errorf("rate must be positive")
	}

	return &DistributedRateLimiter{
		config: config,
		store:  storage,
	}, nil
}

// TokenBucket implementation
func (rl *DistributedRateLimiter) tokenBucketAllow(key string, tokens int) bool {
	ctx := context.Background()
	now := time.Now().UnixNano()

	// Key for storing last update time
	timeKey := fmt.Sprintf("%s:last_update", key)
	tokenKey := fmt.Sprintf("%s:tokens", key)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Get last update time
	lastUpdateTime, err := rl.store.Get(ctx, timeKey)
	if err != nil {
		lastUpdateTime = now
		_ = rl.store.Set(ctx, timeKey, lastUpdateTime, 24*time.Hour)
	}

	// Calculate tokens to add based on time passed
	timePassed := now - lastUpdateTime
	tokensToAdd := int64(float64(timePassed) * float64(rl.config.Rate) / float64(time.Second))

	// Get current tokens
	currentTokens, err := rl.store.Get(ctx, tokenKey)
	if err != nil {
		currentTokens = int64(rl.config.Burst)
	}

	// Add new tokens up to burst limit
	newTokens := min(currentTokens+tokensToAdd, int64(rl.config.Burst))

	// Check if we have enough tokens
	if newTokens < int64(tokens) {
		return false
	}

	// Consume tokens
	newTokens -= int64(tokens)
	_ = rl.store.Set(ctx, tokenKey, newTokens, 24*time.Hour)
	_ = rl.store.Set(ctx, timeKey, now, 24*time.Hour)

	return true
}

// SlidingWindow implementation
func (rl *DistributedRateLimiter) slidingWindowAllow(key string) bool {
	ctx := context.Background()
	now := time.Now().UnixNano() / int64(time.Millisecond) // Convert to milliseconds
	windowStart := now - rl.config.WindowSize.Milliseconds()

	// Use sorted set key
	windowKey := fmt.Sprintf("%s:window", key)

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Remove old entries outside the window
	err := rl.store.ZRemRangeByScore(ctx, windowKey, 0, float64(windowStart))
	if err != nil {
		return false
	}

	// Count current requests in window
	count, err := rl.store.ZCount(ctx, windowKey, float64(windowStart), float64(now))
	if err != nil {
		return false
	}

	if count >= int64(rl.config.Rate) {
		return false
	}

	// Add new request with current timestamp as score
	requestID := fmt.Sprintf("%d", now)
	err = rl.store.ZAdd(ctx, windowKey, float64(now), requestID)
	if err != nil {
		return false
	}

	// Set expiry for the window
	_ = rl.store.Set(ctx, fmt.Sprintf("%s:ttl", windowKey), 1, rl.config.WindowSize*2)

	return true
}

// Allow checks if a request should be allowed based on the configured algorithm
func (rl *DistributedRateLimiter) Allow(key string) bool {
	return rl.AllowN(key, 1)
}

// AllowN checks if n requests should be allowed
func (rl *DistributedRateLimiter) AllowN(key string, n int) bool {
	switch rl.config.Algorithm {
	case TokenBucket:
		return rl.tokenBucketAllow(key, n)
	case SlidingWindow:
		// For sliding window, we handle one request at a time
		for i := 0; i < n; i++ {
			if !rl.slidingWindowAllow(key) {
				return false
			}
		}
		return true
	default:
		return rl.tokenBucketAllow(key, n) // Default to token bucket
	}
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
