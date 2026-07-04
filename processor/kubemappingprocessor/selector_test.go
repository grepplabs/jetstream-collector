package kubemappingprocessor

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/fields"
)

func TestConfiguredFieldPathsWithStaticSelectors(t *testing.T) {
	paths, err := configuredFieldPaths([]string{
		"metadata.name=router",
		"spec.routerId=router-1",
		"metadata.name=other-router",
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"metadata.name", "spec.routerId"}, paths)
}

func TestConfiguredFieldPathsWithHeaderTemplates(t *testing.T) {
	paths, err := configuredFieldPaths([]string{
		"metadata.name=${header:Name}",
		"spec.routerId=prefix-${header:Router-ID}",
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"metadata.name", "spec.routerId"}, paths)
}

func TestConfiguredFieldPathsRejectsNonEqualitySelector(t *testing.T) {
	_, err := configuredFieldPaths([]string{"metadata.name!=router"})
	require.ErrorContains(t, err, "must use equality")
}

func TestValidateEqualityFieldSelector(t *testing.T) {
	require.NoError(t, validateEqualityFieldSelector(fields.OneTermEqualSelector("metadata.name", "router")))
	require.ErrorContains(
		t,
		validateEqualityFieldSelector(fields.OneTermNotEqualSelector("metadata.name", "router")),
		"must use equality",
	)
}
