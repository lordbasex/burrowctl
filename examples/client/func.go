package main

import (
	"fmt"
	"log"

	"github.com/lordbasex/burrowctl/client"
)

func runFunctionExamples() {
	fmt.Println("⚙️ Function Examples (BurrowClient)")
	fmt.Println("─────────────────────────────────")

	debug := false

	// Create BurrowClient for function calls
	dsn := fmt.Sprintf("deviceID=%s&amqp_uri=%s&timeout=%s&debug=%t&reconnect_enabled=true&reconnect_max_attempts=3",
		deviceID, amqpURL, timeout, debug)

	bc, err := client.NewBurrowClient(dsn)
	if err != nil {
		log.Printf("❌ Error creating BurrowClient: %v", err)
		return
	}
	defer bc.Close()

	fmt.Println("✅ BurrowClient connection established")

	// Example 1: String manipulation function
	fmt.Println("\n🔍 Example 1: String length function")
	result, err := bc.ExecFunction("lengthOfString", client.StringParam("Hello, burrowctl!"))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Input: 'Hello, burrowctl!'\n")
	fmt.Printf("  Length: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 2: Math function
	fmt.Println("\n🔍 Example 2: Add numbers function")
	result, err = bc.ExecFunction("addTwoNumbers", client.IntParam(15), client.IntParam(27))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Calculation: 15 + 27 = %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 3: Current time function
	fmt.Println("\n🔍 Example 3: Current time function")
	result, err = bc.ExecFunction("getCurrentTime")
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Server time: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 4: System info function
	fmt.Println("\n🔍 Example 4: System hostname function")
	result, err = bc.ExecFunction("getHostname")
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Server hostname: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 5: String manipulation
	fmt.Println("\n🔍 Example 5: String case conversion")
	result, err = bc.ExecFunction("toUpperCase", client.StringParam("burrowctl is awesome"))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Input: 'burrowctl is awesome'\n")
	fmt.Printf("  Uppercase: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 6: String reverse function
	fmt.Println("\n🔍 Example 6: String reverse function")
	result, err = bc.ExecFunction("reverseString", client.StringParam("burrowctl"))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Input: 'burrowctl'\n")
	fmt.Printf("  Reversed: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 7: Multiple parameters function
	fmt.Println("\n🔍 Example 7: String concatenation function")
	result, err = bc.ExecFunction("concatenateStrings",
		client.StringParam("Hello"),
		client.StringParam(" "),
		client.StringParam("World"),
		client.StringParam("!"))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Parts: 'Hello', ' ', 'World', '!'\n")
	fmt.Printf("  Result: %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 8: Math with float numbers
	fmt.Println("\n🔍 Example 8: Float division function")
	result, err = bc.ExecFunction("divideNumbers", client.Float64Param(22.5), client.Float64Param(4.5))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Calculation: 22.5 ÷ 4.5 = %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 9: Boolean function
	fmt.Println("\n🔍 Example 9: String contains function")
	result, err = bc.ExecFunction("stringContains",
		client.StringParam("burrowctl is great"),
		client.StringParam("great"))
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Text: 'burrowctl is great'\n")
	fmt.Printf("  Contains 'great': %v\n", result)

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 10: Generate UUID function
	fmt.Println("\n🔍 Example 10: Generate UUID function")
	result, err = bc.ExecFunction("generateUUID")
	if err != nil {
		log.Printf("❌ Function error: %v", err)
		return
	}
	fmt.Printf("  Generated UUID: %v\n", result)

	fmt.Println("\n✅ Function examples completed successfully!")
}
