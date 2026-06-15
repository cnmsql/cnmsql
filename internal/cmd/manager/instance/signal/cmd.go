/*
Copyright 2026 The CloudNative MySQL Authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
