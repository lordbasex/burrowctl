package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/lordbasex/burrowctl/client"
)

func runCommandExamples() {
	fmt.Println("💻 Command Examples (System Commands)")
	fmt.Println("────────────────────────────────────")

	debug := false
	// Create BurrowClient for command execution
	dsn := fmt.Sprintf("deviceID=%s&amqp_uri=%s&timeout=%s&debug=%t&reconnect_enabled=true&reconnect_max_attempts=3",
		deviceID, amqpURL, timeout, debug)

	bc, err := client.NewBurrowClient(dsn)
	if err != nil {
		log.Printf("❌ Error creating BurrowClient: %v", err)
		return
	}
	defer bc.Close()

	fmt.Println("✅ BurrowClient connection established")

	// Example 1: List directory contents
	fmt.Println("\n🔍 Example 1: List directory contents (ls -la)")
	cmdResult, err := bc.ExecCommand("ls -la")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Directory contents (first 5 lines):")
	for i, line := range cmdResult.Stdout {
		if i >= 5 {
			fmt.Println("  ...")
			break
		}
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 2: System uptime
	fmt.Println("\n🔍 Example 2: System uptime")
	cmdResult, err = bc.ExecCommand("uptime")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}
	if len(cmdResult.Stdout) > 0 {
		fmt.Printf("  Uptime: %s\n", strings.TrimSpace(cmdResult.Stdout[0]))
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 3: Disk usage
	fmt.Println("\n🔍 Example 3: Disk usage (df -h)")
	cmdResult, err = bc.ExecCommand("df -h")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Disk usage (first 4 lines):")
	for i, line := range cmdResult.Stdout {
		if i >= 4 {
			if len(cmdResult.Stdout) > 4 {
				fmt.Println("  ...")
			}
			break
		}
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 4: Memory usage
	fmt.Println("\n🔍 Example 4: Memory usage (free -h)")
	cmdResult, err = bc.ExecCommand("free -h")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Memory usage:")
	for _, line := range cmdResult.Stdout {
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 5: Current working directory
	fmt.Println("\n🔍 Example 5: Current working directory (pwd)")
	cmdResult, err = bc.ExecCommand("pwd")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}
	if len(cmdResult.Stdout) > 0 {
		fmt.Printf("  Current directory: %s\n", strings.TrimSpace(cmdResult.Stdout[0]))
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 6: Environment variables
	fmt.Println("\n🔍 Example 6: Environment variables (cat /etc/os-release)")
	cmdResult, err = bc.ExecCommand("cat /etc/os-release")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Environment variables (first 5):")
	for _, line := range cmdResult.Stdout {
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 7: Network information
	fmt.Println("\n🔍 Example 7: Network interfaces (cat /etc/passwd)")
	cmdResult, err = bc.ExecCommand("cat /etc/passwd")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Network interfaces (first 10 lines):")
	for _, line := range cmdResult.Stdout {
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 8: System date and time
	fmt.Println("\n🔍 Example 8: System date and time")
	cmdResult, err = bc.ExecCommand("date")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}
	if len(cmdResult.Stdout) > 0 {
		fmt.Printf("  Server date/time: %s\n", strings.TrimSpace(cmdResult.Stdout[0]))
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 9: Who is logged in
	fmt.Println("\n🔍 Example 9: Logged in users (who)")
	cmdResult, err = bc.ExecCommand("who")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	if len(cmdResult.Stdout) == 0 || (len(cmdResult.Stdout) == 1 && strings.TrimSpace(cmdResult.Stdout[0]) == "") {
		fmt.Println("  No users currently logged in")
	} else {
		fmt.Println("  Logged in users:")
		for _, line := range cmdResult.Stdout {
			if strings.TrimSpace(line) != "" {
				fmt.Printf("    %s\n", line)
			}
		}
	}

	fmt.Print("\n────────────────────────────────────────\n")

	// Example 10: Running processes
	fmt.Println("\n🔍 Example 10: Running processes (ps aux)")
	cmdResult, err = bc.ExecCommand("ps aux")
	if err != nil {
		log.Printf("❌ Command error: %v", err)
		return
	}

	fmt.Println("  Running processes (first 5):")
	for _, line := range cmdResult.Stdout {
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	fmt.Println("\n✅ Command examples completed successfully!")
}
