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

// Probe the error path: a column that converts fine followed by one that fails.
func TestProbeErrorPathRefcount(t *testing.T) {
	rows := [][]string{{"a", "b"}, {"1", "xyz"}, {"3", "qrs"}}

	ds, err := conversion.ConvertToTypedDatav2(rows, true, true, nil, true)
	if err != nil {
		t.Fatalf("conversion: %v", err)
	}
	t.Logf("col a type = %v, col b type = %v", ds.Column(0).DataType(), ds.Column(1).DataType())

	op := &ChangeColumnTypeOperator{}
	// "a" (int32) converts cleanly; "b" (string) fails to parse as double
	_, err = op.TransformInternal(ds, &ChangeColumnTypeConfiguration{
		Columns: map[string]ConversionConfiguration{
			"a": {TargetType: types.StringType},
			"b": {TargetType: types.DoubleType},
		},
	})
	if err == nil {
		t.Fatalf("expected an error from the unparseable column")
	}
	t.Logf("got expected error: %v", err)
	ds.Release()
}
