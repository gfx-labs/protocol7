package learning

import (
	"fmt"
	"log/slog"

	"go.temporal.io/sdk/client"

	base "github.com/gfx-labs/protocol7/pkg/orchestration/chat"
	"github.com/gfx-labs/protocol7/pkg/support/embedding"
)

// Config is a worker's settings for the learnings. A command embeds it in its
// kong struct with `embed:""`, so that the settings file names each field by
// its flag: embedding-url, embedding-model, embedding-timeout,
// learning-duplicate-threshold, learning-search-min-score,
// learning-area-min-score and learning-area. Without embedding-url the
// learnings match words only.
type Config struct {
	Embedding      embedding.Settings `embed:"" prefix:"embedding-"`
	DuplicateScore float64            `name:"learning-duplicate-threshold" default:"0.8" help:"Cosine similarity from which a new learning is reported as a likely duplicate of a recorded one."`
	SearchMinScore float64            `name:"learning-search-min-score" default:"0.45" help:"Cosine similarity from which a learning answers a search that has words."`
	AreaMinScore   float64            `name:"learning-area-min-score" default:"0.55" help:"Cosine similarity from which an area is suggested for a new learning."`
	Areas          map[string]string  `name:"learning-area" help:"Area of the work, name to description, which a new learning is compared with to suggest its area."`
}

// Semantic is the settings' embeddings, or nil when they name no embeddings
// server. A nil logger uses slog.Default.
func (c Config) Semantic(logger *slog.Logger) (*Semantic, error) {
	if !c.Embedding.Enabled() {
		return nil, nil
	}
	embedder, err := embedding.New(c.Embedding.Config())
	if err != nil {
		return nil, fmt.Errorf("learning settings: %w", err)
	}
	areas, err := ParseAreas(c.Areas)
	if err != nil {
		return nil, fmt.Errorf("learning settings: learning-area: %w", err)
	}
	return &Semantic{
		Embedder: embedder,
		Logger:   logger,
		Settings: Settings{DuplicateThreshold: c.DuplicateScore, SearchMinScore: c.SearchMinScore, AreaMinScore: c.AreaMinScore, Areas: areas},
	}, nil
}

// Activities is the log's activities, which read and write through the
// Temporal client of the worker's own namespace. The semantic may be nil.
func (c Config) Activities(temporal client.Client, logger *slog.Logger) (*Activities, error) {
	if temporal == nil {
		return nil, fmt.Errorf("learning settings: no Temporal client")
	}
	semantic, err := c.Semantic(logger)
	if err != nil {
		return nil, err
	}
	return &Activities{Store: Store{Client: temporal}, Semantic: semantic}, nil
}

// Register registers the learning workflow and the log's four activities on
// the registry, each under its explicit name, which running workflows and
// chats record in their histories and which must not change.
func Register(registry base.Registry, activities *Activities) error {
	if registry == nil {
		return fmt.Errorf("register learning: no registry")
	}
	if activities == nil {
		return fmt.Errorf("register learning: no activities")
	}
	registry.RegisterWorkflow(WorkflowName, Workflow)
	registry.RegisterActivity(ActivityRecord, activities.Record)
	registry.RegisterActivity(ActivitySearch, activities.Search)
	registry.RegisterActivity(ActivityBrief, activities.Brief)
	registry.RegisterActivity(ActivityDecide, activities.Decide)
	return nil
}
