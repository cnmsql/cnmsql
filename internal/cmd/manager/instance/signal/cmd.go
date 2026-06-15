/*
Copyright 2026 The cloudnative-mysql Authors.

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

// Package signal implements `manager instance signal`: send a Unix signal to a
// process inside the instance container.
package signal

import (
	"fmt"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

// NewCommand builds the `instance signal` command.
func NewCommand() *cobra.Command {
	var (
		pid    int
		signal string
	)
	cmd := &cobra.Command{
		Use:   "signal",
		Short: "Send a Unix signal to a process in this container",
		RunE: func(_ *cobra.Command, _ []string) error {
			sig, err := parseSignal(signal)
			if err != nil {
				return err
			}
			return syscall.Kill(pid, sig)
		},
	}
	cmd.Flags().IntVar(&pid, "pid", 1, "PID to signal")
	cmd.Flags().StringVar(&signal, "signal", "HUP", "Signal to send: HUP, TERM or INT")
	return cmd
}

func parseSignal(name string) (syscall.Signal, error) {
	switch strings.ToUpper(strings.TrimPrefix(name, "SIG")) {
	case "HUP":
		return syscall.SIGHUP, nil
	case "TERM":
		return syscall.SIGTERM, nil
	case "INT":
		return syscall.SIGINT, nil
	default:
		return 0, fmt.Errorf("unsupported signal %q", name)
	}
}
