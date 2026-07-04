package s3exporter

import "testing"

func TestConfigValidateRequiresFilenameTemplateOrUUID(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.FilenameTemplate = ""
	cfg.FilenameAppendUUID = false

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestConfigValidateAcceptsMarshalerType(t *testing.T) {
	for _, marshalerType := range []string{marshalerTypeProto, marshalerTypeJSON} {
		cfg := NewDefaultConfig()
		cfg.Bucket = "bucket"
		cfg.Credentials.AccessKeyID = "access"
		cfg.Credentials.SecretAccessKey = "secret"
		cfg.MarshalerType = marshalerType

		if err := cfg.Validate(); err != nil {
			t.Fatalf("validate failed for %q: %v", marshalerType, err)
		}
		if cfg.MarshalerType != marshalerType {
			t.Fatalf("expected marshaler type %q, got %q", marshalerType, cfg.MarshalerType)
		}
	}
}
