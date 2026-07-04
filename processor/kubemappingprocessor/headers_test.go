package kubemappingprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/client"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
)

func TestExpandHeaderTemplate(t *testing.T) {
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}, "A": {"one"}, "B": {"two"}, "Empty": {""}})
	for _, tc := range []struct{ in, want string }{{"${header:Token}", "abc"}, {"foo=${header:Token}", "foo=abc"}, {"${header:A}-${header:B}", "one-two"}, {"x=${header:Empty}", "x="}} {
		got, ok, err := expandHeaderTemplate(context.Background(), tc.in, md)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, tc.want, got)
	}
	_, ok, err := expandHeaderTemplate(context.Background(), "x=${header:Missing}", md)
	require.NoError(t, err)
	require.False(t, ok)
	require.Error(t, validateHeaderTemplate("${header.Token}"))
	require.Error(t, validateHeaderTemplate("${metadata.Token}"))
}

func TestBuildSelectorsValidatesExpandedSyntax(t *testing.T) {
	md := client.NewMetadata(map[string][]string{"Token": {"abc"}})
	selectors, ok, err := buildSelectors(context.Background(), SelectorConfig{Labels: []string{"token=${header:Token}", "environment in (prod,staging)"}, Fields: []string{"metadata.name=router"}}, md)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, selectors.labels.Matches(labels.Set{"token": "abc", "environment": "prod"}))
	require.False(t, selectors.labels.Matches(labels.Set{"token": "other", "environment": "prod"}))
	require.True(t, selectors.fields.Matches(fields.Set{"metadata.name": "router"}))
	_, _, err = buildSelectors(context.Background(), SelectorConfig{Labels: []string{"bad in (${header:Token}"}}, md)
	require.Error(t, err)
	_, _, err = buildSelectors(context.Background(), SelectorConfig{Fields: []string{"metadata.name!=${header:Token}"}}, md)
	require.ErrorContains(t, err, "must use equality")
}

func TestBuildSelectorsWithStaticLabelSelector(t *testing.T) {
	selectors, ok, err := buildSelectors(
		context.Background(),
		SelectorConfig{Labels: []string{"environment=production"}},
		client.NewMetadata(nil),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "environment=production", selectors.labels.String())
	require.True(t, selectors.labels.Matches(labels.Set{"environment": "production"}))
}

func TestBuildSelectorsWithStaticFieldSelector(t *testing.T) {
	selectors, ok, err := buildSelectors(
		context.Background(),
		SelectorConfig{Fields: []string{"metadata.name=router"}},
		client.NewMetadata(nil),
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "metadata.name=router", selectors.fields.String())
	require.True(t, selectors.fields.Matches(fields.Set{"metadata.name": "router"}))
}
