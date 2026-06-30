package filtrify_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/liminaab/filtrify"
	"github.com/liminaab/filtrify/test"
	"github.com/liminaab/filtrify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentNewColumnNoSchemaRace runs many newColumn transforms in parallel.
//
// The newColumn operator runs its statement as an in-memory SQL query against a
// schema registered in qlbridge's process-global schema registry, and drops that
// schema in a deferred call once the query returns. That registry is not safe for
// concurrent use: one transform's SchemaDrop mutates the shared schema/database
// list while another transform's query reads & sorts it, and the per-call schema
// name came from an unsynchronised math/rand.Source that could collide. Either
// path could make a concurrent transform's derived column come back empty/wrong
// (the bug that made import dedupe save rows it should have skipped).
//
// This is a data race, so it is caught DETERMINISTICALLY only under `go test
// -race` (CI runs `go test -race -short ./...`). Without -race the corruption is
// rare and scheduler-dependent, so do not rely on a non-race run to catch a
// regression here. The functional symptom is also covered end-to-end by the
// importmanager dedupe integration tests (TestTransactionsTxv1CanSkipRows et al.).
func TestConcurrentNewColumnNoSchemaRace(t *testing.T) {
	base, err := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
	require.NoError(t, err, "basic data conversion failed")

	step := &types.TransformationStep{
		Operator:      types.NewColumn,
		Configuration: "{\"statement\": \"`Instrument Type` AS `Test Column`\"}",
	}

	const goroutines = 64
	const iterations = 30

	var wg sync.WaitGroup
	errs := make(chan error, goroutines*iterations)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				// Fresh dataset per call so goroutines never share row slices —
				// the only shared state under test is the schema registration.
				ds, convErr := filtrify.ConvertToTypedData(test.UAT1TestDataFormatted, true, true, true)
				if convErr != nil {
					errs <- fmt.Errorf("conversion failed: %w", convErr)
					return
				}
				out, tErr := filtrify.Transform(ds, []*types.TransformationStep{step}, nil)
				if tErr != nil {
					errs <- fmt.Errorf("transform failed: %w", tErr)
					return
				}
				if len(out.Rows) != len(base.Rows) {
					errs <- fmt.Errorf("row count mismatch: got %d want %d", len(out.Rows), len(base.Rows))
					return
				}
				for _, r := range out.Rows {
					derived := test.GetColumn(r, "Test Column")
					source := test.GetColumn(r, "Instrument Type")
					if derived == nil || source == nil {
						errs <- fmt.Errorf("derived or source column missing on a row")
						return
					}
					if derived.CellValue.DataType != source.CellValue.DataType ||
						derived.CellValue.StringValue != source.CellValue.StringValue {
						errs <- fmt.Errorf("derived column wrong: got (%v,%q) want (%v,%q)",
							derived.CellValue.DataType, derived.CellValue.StringValue,
							source.CellValue.DataType, source.CellValue.StringValue)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		assert.NoError(t, e)
	}
}
