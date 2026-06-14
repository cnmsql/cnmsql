/*
Copyright 2026 The CNMySQL Authors.

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
