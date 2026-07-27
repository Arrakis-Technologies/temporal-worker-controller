// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog, Inc. Copyright 2024 Datadog, Inc.

package main

import (
	"testing"
	"time"
)

func TestValidateControllerSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		concurrent int
		interval   time.Duration
		wantError  bool
	}{
		{
			name:       "valid",
			concurrent: 1,
			interval:   2 * time.Minute,
		},
		{
			name:      "zero concurrency",
			interval:  2 * time.Minute,
			wantError: true,
		},
		{
			name:       "negative concurrency",
			concurrent: -1,
			interval:   2 * time.Minute,
			wantError:  true,
		},
		{
			name:       "zero interval",
			concurrent: 1,
			wantError:  true,
		},
		{
			name:       "negative interval",
			concurrent: 1,
			interval:   -time.Second,
			wantError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateControllerSettings(tt.concurrent, tt.interval)
			if tt.wantError && err == nil {
				t.Fatal("validateControllerSettings() error = nil, want error")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("validateControllerSettings() error = %v, want nil", err)
			}
		})
	}
}
