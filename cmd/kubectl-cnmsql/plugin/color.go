/*
Copyright 2026 The CNMSQL - CloudNative for MySQL Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package plugin

import (
	"fmt"
	"os"

	"github.com/logrusorgru/aurora/v4"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

// colorConfiguration represents how the output should be colorized. It is a
// pflag.Value and therefore implements String, Set and Type.
type colorConfiguration string

const (
	colorAlways colorConfiguration = "always"
	colorAuto   colorConfiguration = "auto"
	colorNever  colorConfiguration = "never"
)

// String returns the string representation of the configuration.
func (c colorConfiguration) String() string { return string(c) }

// Set validates and stores the color configuration value.
func (c *colorConfiguration) Set(val string) error {
	switch v := colorConfiguration(val); v {
	case colorAlways, colorAuto, colorNever:
		*c = v
		return nil
	default:
		return fmt.Errorf("should be one of 'always', 'auto', or 'never'")
	}
}

// Type returns the flag data type used for completion.
func (c *colorConfiguration) Type() string { return "string" }

// ConfigureColor renews aurora.DefaultColorizer from the --color flag on cmd
// and the TTY status of stdout. It mirrors CNPG's ConfigureColor so that a
// command's PersistentPreRun can prepare colorization before rendering.
func ConfigureColor(cmd *cobra.Command) {
	configureColor(cmd, term.IsTerminal(int(os.Stdout.Fd()))) //nolint:gosec // file descriptors always fit in int
}

func configureColor(cmd *cobra.Command, isTTY bool) {
	colorConfig := colorAuto
	if colorFlag := cmd.Flag("color"); colorFlag != nil {
		colorConfig = colorConfiguration(colorFlag.Value.String())
	}

	var shouldColorize bool
	switch colorConfig {
	case colorAlways:
		shouldColorize = true
	case colorNever:
		shouldColorize = false
	default: // colorAuto
		shouldColorize = isTTY
	}

	aurora.DefaultColorizer = aurora.New(
		aurora.WithColors(shouldColorize),
		aurora.WithHyperlinks(true),
	)
}

// AddColorControlFlag attaches the --color flag to cmd and registers its
// shell completion (always, auto, never). The default value is "auto".
func AddColorControlFlag(cmd *cobra.Command) {
	colorValue := colorAuto
	cmd.Flags().Var(&colorValue, "color",
		"Control color output; options include 'always', 'auto', or 'never'")
	_ = cmd.RegisterFlagCompletionFunc("color",
		func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
			return []string{colorAlways.String(), colorAuto.String(), colorNever.String()},
				cobra.ShellCompDirectiveDefault | cobra.ShellCompDirectiveKeepOrder
		})
}
