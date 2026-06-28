// Command worker runs an out-of-engine task worker.
package main

import "fmt"

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	fmt.Printf("liu worker %s\n", version)
}
