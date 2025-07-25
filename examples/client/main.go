package main

import (
	"fmt"
	"log"
	"os"
)

const (
	// Configuration constants
	deviceID = "fd1825ec5a7b63f3fa2be9e04154d3b16f676663ba38e23d4ffafa7b0df29efb"
	amqpURL  = "amqp://burrowuser:burrowpass123@localhost:5672/"
	timeout  = "10s"
)

func main() {
	fmt.Println("🚀 burrowctl Complete Example")
	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Printf("📡 Device ID: %s\n", deviceID)
	fmt.Printf("🔗 AMQP URL: %s\n", amqpURL)
	fmt.Println()

	// Parse command line arguments
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "sql", "--sql":
			runSQLExamples()
		case "functions", "--func":
			runFunctionExamples()
		case "commands", "--cmd":
			runCommandExamples()
		default:
			showUsage()
		}
	}
}

func showUsage() {
	fmt.Println("Usage: go run main.go [sql|--sql|functions|--func|commands]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  sql, --sql    - Run SQL examples using database/sql interface")
	fmt.Println("  functions, --func - Run custom function examples")
	fmt.Println("  commands, --cmd   - Run system command examples")
	fmt.Println()
	fmt.Println("Note: sql.go, func.go, and cmd.go contain the individual functions")
}

func init() {
	// Set up logging
	log.SetPrefix("[client] ")
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
