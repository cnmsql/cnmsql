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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultWatchInterval is the refresh period used by --watch when no
// --watch-interval is given.
const defaultWatchInterval = 2 * time.Second

// runWatch repeatedly invokes render, clearing the screen between frames, until
// the context is cancelled (Ctrl-C). A per-frame error is printed but does not
// stop the loop, so a transient API blip doesn't end the watch. The header
// shows the cluster and refresh cadence, mirroring watch(1).
func runWatch(ctx context.Context, label string, interval time.Duration, render func(context.Context) error) error {
	if interval <= 0 {
		interval = defaultWatchInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		clearScreen()
		fmt.Printf("Every %s — %s — %s    (Ctrl-C to stop)\n",
			interval, label, time.Now().Format("15:04:05"))
		if err := render(ctx); err != nil {
			fmt.Printf("\nrender error: %v\n", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// clearScreen issues the ANSI clear + cursor-home sequence.
func clearScreen() {
	fmt.Print("\033[2J\033[H")
}

// watchOrOnce runs render once, or on a --watch loop when watch is true.
func watchOrOnce(
	ctx context.Context, watch bool, label string, interval time.Duration, render func(context.Context) error,
) error {
	if !watch {
		return render(ctx)
	}
	err := runWatch(ctx, label, interval, render)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
