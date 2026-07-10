package main

import (
	"bytes"
	"testing"
)

func TestVersionCommandUsesConfiguredOutput(t *testing.T) {
	var out bytes.Buffer
	cmd := newVersionCmd()
	cmd.SetOut(&out)
	cmd.SetArgs(nil)

	if err := cmd.Execute(); err != nil {
		t.Fatalf("version command: %v", err)
	}
	if got, want := out.String(), effectiveVersion()+"\n"; got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestEffectiveVersionRequiresBothReleaseLinkerAssignments(t *testing.T) {
	previousVersion := version
	previousSetting := releaseLinkerSetting
	t.Cleanup(func() {
		version = previousVersion
		releaseLinkerSetting = previousSetting
	})

	tests := []struct {
		name    string
		version string
		setting string
		want    string
	}{
		{name: "ordinary build", want: sourceVersion},
		{
			name:    "release build",
			version: sourceVersion,
			setting: releaseLinkerSettingPrefix + sourceVersion + "]",
			want:    sourceVersion,
		},
		{
			name:    "Homebrew HEAD source build",
			version: sourceVersion,
			want:    sourceVersion,
		},
		{
			name:    "marker without version assignment",
			setting: releaseLinkerSettingPrefix + sourceVersion + "]",
			want:    "invalid-release-linker-version",
		},
		{
			name:    "conflicting assignments",
			version: sourceVersion,
			setting: releaseLinkerSettingPrefix + "9.9.9]",
			want:    "invalid-release-linker-version",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			version = test.version
			releaseLinkerSetting = test.setting
			if got := effectiveVersion(); got != test.want {
				t.Fatalf("effectiveVersion() = %q, want %q", got, test.want)
			}
		})
	}
}
