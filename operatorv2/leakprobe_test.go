package operatorv2

import (
	"testing"

	"github.com/liminaab/filtrify/conversion"
	"github.com/liminaab/filtrify/types"
)

// Chunked.Release() sets chunks=nil when the refcount reaches 0, so a non-empty
// Chunks() after the owning table is released proves a refcount was never given back.
func TestProbeConvertedColumnRefcount(t *testing.T) {
	rows := [][]string{{"a", "b"}, {"1", "2"}, {"3", "4"}}

	ds, err := conversion.ConvertToTypedDatav2(rows, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion: %v", err)
	}

	op := &ChangeColumnTypeOperator{}
	out, err := op.TransformInternal(ds, &ChangeColumnTypeConfiguration{
		Columns: map[string]ConversionConfiguration{
			// column "a" is converted, column "b" is passed through untouched
			"a": {TargetType: types.StringType},
		},
	})
	if err != nil {
		t.Fatalf("transform: %v", err)
	}

	convertedChunked := out.Column(0).Data()
	passthroughChunked := out.Column(1).Data()

	// drop both tables: nothing should be left alive
	out.Release()
	ds.Release()

	t.Logf("after releasing BOTH tables:")
	t.Logf("  converted   column chunks alive = %d", len(convertedChunked.Chunks()))
	t.Logf("  passthrough column chunks alive = %d", len(passthroughChunked.Chunks()))

	if n := len(passthroughChunked.Chunks()); n != 0 {
		t.Errorf("passthrough column leaked: %d chunks still alive", n)
	}
	if n := len(convertedChunked.Chunks()); n != 0 {
		t.Errorf("converted column leaked: %d chunks still alive", n)
	}
}

// Probe the error path: a column that converts fine followed by one whose
// conversion cannot even be attempted.
//
// The failure has to be a configuration one - no route from the source type to
// the target - rather than a value that will not parse. A cell that fails to
// convert is not an error at all: convertColumn leaves a null behind and carries
// on, which is what v1 does by way of TransformInternal answering every per-cell
// error with a nil cell.
func TestProbeErrorPathRefcount(t *testing.T) {
	rows := [][]string{{"a", "b"}, {"1", "xyz"}, {"3", "qrs"}}

	ds, err := conversion.ConvertToTypedDatav2(rows, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion: %v", err)
	}
	t.Logf("col a type = %v, col b type = %v", ds.Column(0).DataType(), ds.Column(1).DataType())

	op := &ChangeColumnTypeOperator{}
	// "a" (int32) converts cleanly; "b" (string) has no conversion to an object
	_, err = op.TransformInternal(ds, &ChangeColumnTypeConfiguration{
		Columns: map[string]ConversionConfiguration{
			"a": {TargetType: types.StringType},
			"b": {TargetType: types.ObjectType},
		},
	})
	if err == nil {
		t.Fatalf("expected an error from the column with no conversion to its target")
	}
	t.Logf("got expected error: %v", err)
	ds.Release()
}
