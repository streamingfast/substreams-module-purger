package datastore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_fileInfo(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantLower  uint64
		wantHigher uint64
		wantType   FileType
		wantErr    bool
	}{
		{
			name:       "index",
			in:         "sol-mainnet/substreams-states/vsomething/23452345234534adeadbeef/0000989000-0000990000.index.zst",
			wantLower:  989000,
			wantHigher: 990000,
			wantType:   Index,
		},
		{
			name:       "output",
			in:         "0001973000-0001974000.output.zst",
			wantLower:  1973000,
			wantHigher: 1974000,
			wantType:   Output,
		},
		{
			name:       "state",
			in:         "eth-mainnet/substreams-states/v5/3ec75d75eea4b8dfd8b515e09dca992a14f059ed/states/0015369000-0012369621.kv.zst",
			wantLower:  12369621,
			wantHigher: 15369000,
			wantType:   State,
		},
		{
			name:    "invalid",
			in:      "/my/invalid-file.txt",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lower, higher, ft, err := FileInfo(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.Equal(t, tt.wantLower, lower)
			assert.Equal(t, tt.wantHigher, higher)
			assert.Equal(t, tt.wantType, ft)
		})
	}
}
