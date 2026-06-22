package cmd

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRootCommandContainsExpectedCommands(t *testing.T) {
	want := []string{
		"backup", "database", "destroy", "fence", "group", "logs", "maintenance", "metrics",
		"promote", "reinit", "reload", "restart", "status", "user", "version",
	}
	root := NewRootCommand()
	for _, name := range want {
		if command, _, err := root.Find([]string{name}); err != nil || command == root {
			t.Errorf("root command missing %q", name)
		}
	}
	if !root.SilenceErrors || !root.SilenceUsage {
		t.Error("root should silence errors and usage")
	}
}

func TestCommandValidationDoesNotRequireCluster(t *testing.T) {
	tests := []struct {
		name        string
		commandArgs []string
		wantErr     string
	}{
		{name: "database create requires name", commandArgs: []string{"database", "create"}, wantErr: "--name is required"},
		{name: "database drop requires name", commandArgs: []string{"database", "drop"}, wantErr: "--name is required"},
		{
			name: "database protects system schema", commandArgs: []string{"database", "drop", "--name=mysql"},
			wantErr: "system database",
		},
		{
			name: "fence validates state", commandArgs: []string{"fence", "maybe", "demo", "demo-1"},
			wantErr: "first argument must be 'on' or 'off'",
		},
		{
			name: "restart rejects too many args", commandArgs: []string{"restart", "a", "b", "c"},
			wantErr: "accepts at most 2 arg",
		},
		{name: "version rejects args", commandArgs: []string{"version", "extra"}, wantErr: "unknown command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := NewRootCommand()
			root.SetArgs(tt.commandArgs)
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRootHelpers(t *testing.T) {
	t.Parallel()
	if got := firstArg(nil); got != "" {
		t.Errorf("firstArg(nil) = %q", got)
	}
	if got := firstArg([]string{"cluster", "instance"}); got != "cluster" {
		t.Errorf("firstArg() = %q", got)
	}
	if options := deleteNow(); options.GracePeriodSeconds == nil || *options.GracePeriodSeconds != 0 {
		t.Errorf("deleteNow() = %#v", options)
	}
}

func TestSplitReinit(t *testing.T) {
	t.Parallel()
	tests := map[string][]string{"": nil, "one": {"one"}, " one, ,two ": {"one", "two"}}
	for input, want := range tests {
		got := splitReinit(input)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("splitReinit(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestStringHelpers(t *testing.T) {
	t.Parallel()

	if got := splitCSV(" SELECT, ,INSERT,UPDATE "); strings.Join(got, ",") != "SELECT,INSERT,UPDATE" {
		t.Errorf("splitCSV() = %v", got)
	}
	if got := defaultHost(""); got != "%" {
		t.Errorf("defaultHost(\"\") = %q", got)
	}
	if got := defaultHost("localhost"); got != "localhost" {
		t.Errorf("defaultHost(localhost) = %q", got)
	}
	if got := orNone(""); got != "<none>" {
		t.Errorf("orNone(\"\") = %q", got)
	}
	if got := orNone("value"); got != "value" {
		t.Errorf("orNone(value) = %q", got)
	}
}

func TestCompletionStopsAfterMaximumArguments(t *testing.T) {
	t.Parallel()

	command := &cobra.Command{}
	values, directive := completeClusterArg(command, []string{"cluster"}, "")
	if values != nil || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("completeClusterArg() = %v, %v", values, directive)
	}
	values, directive = completeClusterInstanceArgs(command, []string{"cluster", "instance"}, "")
	if values != nil || directive != cobra.ShellCompDirectiveNoFileComp {
		t.Errorf("completeClusterInstanceArgs() = %v, %v", values, directive)
	}
}
