package store

import "testing"

func TestMemStoreContract(t *testing.T) {
	RunStoreContract(t, func() Store { return NewMemStore() })
}
