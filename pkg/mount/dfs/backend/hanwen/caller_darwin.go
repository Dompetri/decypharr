//go:build darwin && amd64

package hanwen

import "context"

// callerProcess is a no-op on darwin: there is no /proc to resolve the caller.
func callerProcess(ctx context.Context) (callerInfo, bool) { return callerInfo{}, false }
