// Command engine runs the workflow engine control plane.
package main

import "fmt"

// version is overridden at build time via -ldflags.
var version = "dev"

func main() {
	fmt.Printf("liu engine %s\n", version)
}
