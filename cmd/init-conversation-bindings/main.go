package main

import (
	"fmt"
	"os"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: init-conversation-bindings /absolute/path/to/bindings.db")
		os.Exit(2)
	}
	if err := auth.InitializeConversationBindings(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
