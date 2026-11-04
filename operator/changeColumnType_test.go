package operator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConvertJavaLayoutToGoLayout(t *testing.T) {
	tests := []struct {
		javaLayout string
		want       string
	}{
		{"yyyy-MM-dd", "2006-01-02"},
		{"dd/MM/yyyy", "02/01/2006"},
		{"d/M/yyyy", "2/1/2006"},
		{"dd.MM.yy", "02.01.06"},
		{"yyyyMMdd", "20060102"},
		{"yyyy-MM-dd HH:mm:ss", "2006-01-02 15:04:05"},
		{"yyyy-MM-dd hh:mm:ss", "2006-01-02 15:04:05"},
		{"H:m:s", "15:4:5"},
		{"HH:mm:ss.SSS", "15:04:05.000"},
		{"dd MMM yyyy", "02 Jan 2006"},
		{"dd MMMM yyyy", "02 January 2006"},
		{"yyyy-MM-dd'T'HH:mm:ssZ", "2006-01-02'T'15:04:05Z07:00"},
		{"yyyy-MM-ddTHH:mm:ssZZ", "2006-01-02T15:04:05-07:00"},
	}
	for _, tt := range tests {
		t.Run(tt.javaLayout, func(t *testing.T) {
			got, err := convertJavaLayoutToGoLayout(tt.javaLayout)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConvertJavaLayoutToGoParseLayout(t *testing.T) {
	tests := []struct {
		javaLayout string
		want       string
	}{
		{"yyyy-MM-dd", "2006-1-2"},
		{"dd/MM/yyyy", "2/1/2006"},
		{"d/M/yyyy", "2/1/2006"},
		{"yyyyMMdd", "200612"},
		{"dd.MM.yyyy HH:mm:ss", "2.1.2006 15:4:5"},
		{"dd MMM yyyy", "2 Jan 2006"},
		{"HH:mm:ss.SSS", "15:4:5.000"},
		{"yyyy-MM-ddTHH:mm:ssZZ", "2006-1-2T15:4:5-07:00"},
	}
	for _, tt := range tests {
		t.Run(tt.javaLayout, func(t *testing.T) {
			got, err := convertJavaLayoutToGoParseLayout(tt.javaLayout)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStringToTimeAcceptsSingleAndDoubleDigits(t *testing.T) {
	tests := []struct {
		name   string
		format string
		input  string
		want   time.Time
	}{
		// double digit format, both padded and non-padded input
		{"dd/MM/yyyy padded", "dd/MM/yyyy", "05/03/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd/MM/yyyy non-padded", "dd/MM/yyyy", "5/3/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd/MM/yyyy non-padded day", "dd/MM/yyyy", "5/03/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd/MM/yyyy non-padded month", "dd/MM/yyyy", "05/3/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd/MM/yyyy two digit values", "dd/MM/yyyy", "15/12/2024", time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC)},
		// single digit format, both padded and non-padded input
		{"d/M/yyyy padded", "d/M/yyyy", "05/03/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"d/M/yyyy non-padded", "d/M/yyyy", "5/3/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"d/M/yyyy two digit values", "d/M/yyyy", "15/12/2024", time.Date(2024, 12, 15, 0, 0, 0, 0, time.UTC)},
		{"M/d/yyyy padded", "M/d/yyyy", "03/05/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"MM/dd/yyyy non-padded", "MM/dd/yyyy", "3/5/2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"yyyy-MM-dd non-padded", "yyyy-MM-dd", "2024-3-5", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"yyyyMMdd without separators", "yyyyMMdd", "20240305", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd.MM.yy non-padded", "dd.MM.yy", "5.3.24", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{"dd MMM yyyy non-padded", "dd MMM yyyy", "5 Mar 2024", time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		// datetime
		{"datetime padded", "dd/MM/yyyy HH:mm:ss", "05/03/2024 09:05:07", time.Date(2024, 3, 5, 9, 5, 7, 0, time.UTC)},
		{"datetime non-padded", "dd/MM/yyyy HH:mm:ss", "5/3/2024 9:5:7", time.Date(2024, 3, 5, 9, 5, 7, 0, time.UTC)},
		{"datetime single digit format padded input", "d/M/yyyy H:m:s", "05/03/2024 09:05:07", time.Date(2024, 3, 5, 9, 5, 7, 0, time.UTC)},
		{"datetime H is 24 hour", "d/M/yyyy H:mm", "5/3/2024 16:45", time.Date(2024, 3, 5, 16, 45, 0, 0, time.UTC)},
		{"datetime hh is 24 hour", "dd/MM/yyyy hh:mm", "5/3/2024 16:45", time.Date(2024, 3, 5, 16, 45, 0, 0, time.UTC)},
		{"datetime with millis", "dd/MM/yyyy HH:mm:ss.SSS", "5/3/2024 16:45:09.123", time.Date(2024, 3, 5, 16, 45, 9, 123000000, time.UTC)},
		{"datetime with offset", "yyyy-MM-ddTHH:mm:ssZZ", "2024-3-5T16:45:09+00:00", time.Date(2024, 3, 5, 16, 45, 9, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ConversionConfiguration{StringDate: &StringDateConfiguration{DateFormat: tt.format}}
			got, err := commonStringToTime(tt.input, config, time.RFC3339)
			assert.NoError(t, err)
			assert.True(t, tt.want.Equal(got), "want %v, got %v", tt.want, got)
		})
	}
}

func TestStringToTimeRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		format string
		input  string
	}{
		{"dd/MM/yyyy", "32/03/2024"},
		{"dd/MM/yyyy", "05/13/2024"},
		{"dd/MM/yyyy", "123/03/2024"},
		{"dd/MM/yyyy", "05-03-2024"},
		{"dd/MM/yyyy", "/03/2024"},
	}
	for _, tt := range tests {
		t.Run(tt.format+" "+tt.input, func(t *testing.T) {
			config := ConversionConfiguration{StringDate: &StringDateConfiguration{DateFormat: tt.format}}
			_, err := commonStringToTime(tt.input, config, time.RFC3339)
			assert.Error(t, err)
		})
	}
}

func TestTimeToStringKeepsPadding(t *testing.T) {
	input := time.Date(2024, 3, 5, 9, 5, 7, 0, time.UTC)
	tests := []struct {
		format string
		want   string
	}{
		{"dd/MM/yyyy", "05/03/2024"},
		{"d/M/yyyy", "5/3/2024"},
		{"dd/MM/yyyy HH:mm:ss", "05/03/2024 09:05:07"},
		{"d/M/yyyy H:m:s", "5/3/2024 09:5:7"},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			config := ConversionConfiguration{StringDate: &StringDateConfiguration{DateFormat: tt.format}}
			got, err := commonTimeToString(input, config, "2006-01-02 15:04:05")
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
