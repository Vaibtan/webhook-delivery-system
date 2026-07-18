package domain

import (
	"context"
	"time"
)

// Cursor is an opaque keyset-pagination position for listing subscriptions,
// ordered by (created_at DESC, id DESC). A nil cursor means "from the start".
type Cursor struct {
	CreatedAt time.Time
	ID        string
}

// Page is one page of a keyset-paginated listing. NextCursor is nil when the
// last page has been reached.
type Page[T any] struct {
	Items      []T
	NextCursor *Cursor
}

// Deliverer performs a single delivery attempt for a delivery-log row, applying
// the CAS-guarded status transition. Implemented by internal/worker.
type Deliverer interface {
	Process(ctx context.Context, deliveryLogID string) error
}
