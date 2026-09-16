// Package llm wraps a LangChain Go chat model behind a small interface so the
// rest of the application never touches provider SDK types directly.
package llm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/llms/openai"

	"github.com/example/golang-staterpack/internal/config"
)

// Client is the seam the service layer codes against. It keeps the provider
// swappable and makes the summariser trivially fakeable in tests.
type Client interface {
	// Complete returns a single assistant response for one user prompt.
	Complete(ctx context.Context, prompt string) (string, error)
	// Summarize condenses arbitrary text to a short paragraph.
	Summarize(ctx context.Context, text string) (string, error)
	// Model reports the configured model name, for logging / diagnostics.
	Model() string
}

type client struct {
	llm         *openai.LLM
	model       string
	temperature float64
	maxTokens   int
}

// New builds a LangChain Go chat client. DeepSeek's V4.1 Flash is served over
// an OpenAI-compatible API, so we point the OpenAI client at the configured
// base URL and model name rather than depending on a DeepSeek-specific SDK.
func New(cfg config.LLMConfig) (Client, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("llm: API key is empty")
	}

	opts := []openai.Option{
		openai.WithToken(cfg.APIKey),
		openai.WithModel(cfg.Model),
		openai.WithBaseURL(strings.TrimRight(cfg.BaseURL, "/")),
	}

	model, err := openai.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("llm: init %s client: %w", cfg.Provider, err)
	}

	return &client{
		llm:         model,
		model:       cfg.Model,
		temperature: cfg.Temperature,
		maxTokens:   cfg.MaxTokens,
	}, nil
}

func (c *client) Model() string { return c.model }

func (c *client) Complete(ctx context.Context, prompt string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	resp, err := c.llm.GenerateContent(ctx,
		[]llms.MessageContent{
			llms.TextParts(llms.ChatMessageTypeHuman, prompt),
		},
		llms.WithTemperature(c.temperature),
		llms.WithMaxTokens(c.maxTokens),
	)
	if err != nil {
		return "", fmt.Errorf("llm: generate: %w", err)
	}
	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("llm: empty response from %s", c.model)
	}
	return strings.TrimSpace(resp.Choices[0].Content), nil
}

const summarizerPrompt = `You are a concise technical summarizer.
Summarize the user's text in at most three sentences.
Preserve technical specifics (identifiers, numbers, names).
Respond with the summary only - no preamble, no markdown fences.

Text:
%s`

func (c *client) Summarize(ctx context.Context, text string) (string, error) {
	if strings.TrimSpace(text) == "" {
		return "", nil
	}
	return c.Complete(ctx, fmt.Sprintf(summarizerPrompt, text))
}

// disabled is a Client that never calls a provider. It is used when no API key
// is configured so CRUD keeps working while summarisation is a no-op.
type disabled struct{ model string }

// Disabled returns a Client that returns the input unchanged as a "summary".
// Handlers still succeed, which keeps the worker healthy and avoids flooding the
// dead-letter queue while the LLM is not configured.
func Disabled(model string) Client { return &disabled{model: model} }

func (d *disabled) Model() string { return d.model }

func (d *disabled) Complete(_ context.Context, _ string) (string, error) {
	return "", ErrDisabled
}

func (d *disabled) Summarize(_ context.Context, text string) (string, error) {
	return text, nil
}

// ErrDisabled is returned by Disabled().Complete.
var ErrDisabled = fmt.Errorf("llm: client disabled (no API key configured)")
