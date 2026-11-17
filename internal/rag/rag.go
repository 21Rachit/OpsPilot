// Package rag answers questions with Retrieval-Augmented Generation:
// find the relevant runbooks and incidents, give them to Claude, and let Claude answer
// from them.
package rag

import (
	"context"
	"fmt"
	"strings"

	"opspilot/internal/llm"
	"opspilot/internal/search"
)

const (
	maxDocs  = 4   // how many incidents and how many runbook sections to look up
	minScore = 0.6 // weaker matches are left out (see /search?min_score)
)

// noDocsAnswer is the reply when search finds nothing relevant enough.
const noDocsAnswer = "I couldn't find any runbook or past incident about this, so I can't answer it."

// systemPrompt tells Claude its job. It is the same for every question.
const systemPrompt = `You are OpsPilot, an assistant for on-call engineers.
Answer the question using only the documents inside <documents>.
After each fact, cite the document it came from by its number, like [1] or [2].
If the documents don't answer the question, say you don't know. Don't guess.
Keep the answer short: a few sentences or a short list of steps.`

// Searcher is what rag needs from the search package.
type Searcher interface {
	Search(ctx context.Context, query string, limit int, minScore float64) (search.Results, error)
}

// Source is one document Claude was given. N is the number Claude cites, e.g. [1].
type Source struct {
	N     int     `json:"n"`
	Kind  string  `json:"kind"`  // "runbook" or "incident"
	Title string  `json:"title"` // what a person would recognise
	Ref   string  `json:"ref"`   // where to find it, e.g. runbooks/disk-full.md#Fix or incident 15
	Score float64 `json:"score"`
}

// Answer is what Ask returns.
type Answer struct {
	Question string   `json:"question"`
	Answer   string   `json:"answer"`
	Sources  []Source `json:"sources"`
	Model    string   `json:"model"`
	CostUSD  float64  `json:"cost_usd"`
}

type Asker struct {
	searcher Searcher
	llm      llm.Client
}

func NewAsker(s Searcher, c llm.Client) *Asker {
	return &Asker{searcher: s, llm: c}
}

// Ask runs the three RAG steps: retrieve, augment, generate.
func (a *Asker) Ask(ctx context.Context, question string) (Answer, error) {
	// 1. Retrieve: find the documents closest in meaning to the question.
	res, err := a.searcher.Search(ctx, question, maxDocs, minScore)
	if err != nil {
		return Answer{}, fmt.Errorf("search: %w", err)
	}

	// 2. Augment: put the documents and the question into one prompt.
	prompt, sources := buildPrompt(question, res)
	// Nothing relevant found: Claude could only say "I don't know", so say it ourselves.
	// Free, instant, and Claude never gets a chance to guess.
	if len(sources) == 0 {
		return Answer{Question: question, Answer: noDocsAnswer, Sources: sources}, nil
	}

	// 3. Generate: Claude writes the answer from those documents.
	out, err := a.llm.Complete(ctx, systemPrompt, prompt)
	if err != nil {
		return Answer{}, fmt.Errorf("llm: %w", err)
	}

	return Answer{
		Question: question,
		Answer:   out.Text,
		Sources:  sources,
		Model:    out.Model,
		CostUSD:  out.CostUSD,
	}, nil
}

// buildPrompt numbers every document, so Claude can cite [1], [2], ... and we know which
// document each number means. Runbooks come first: they say how to fix things.
func buildPrompt(question string, res search.Results) (string, []Source) {
	var b strings.Builder
	sources := []Source{}

	b.WriteString("<documents>\n")
	for _, h := range res.Runbooks {
		n := len(sources) + 1
		sources = append(sources, Source{N: n, Kind: "runbook", Title: h.Title + " > " + h.Heading,
			Ref: h.Source + "#" + h.Heading, Score: h.Score})
		fmt.Fprintf(&b, "<document n=\"%d\" type=\"runbook\" source=%q>\n%s\n</document>\n",
			n, h.Source, h.Content)
	}
	for _, h := range res.Incidents {
		n := len(sources) + 1
		sources = append(sources, Source{N: n, Kind: "incident", Title: h.Title,
			Ref: fmt.Sprintf("incident %d", h.ID), Score: h.Score})
		fmt.Fprintf(&b, "<document n=\"%d\" type=\"incident\">\nIncident %d: %s\nSeverity: %s\nStatus: %s\nCreated: %s\n</document>\n",
			n, h.ID, h.Title, h.Severity, h.Status, h.CreatedAt.Format("2006-01-02"))
	}
	b.WriteString("</documents>\n\n")
	fmt.Fprintf(&b, "Question: %s", question)

	return b.String(), sources
}
