package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lordbasex/burrowctl/client"
)

func runSQLExamples() {
	fmt.Println("📊 SQL Examples (database/sql interface)")
	fmt.Println("────────────────────────────────────────")

	debug := false

	// Create DSN with all options
	dsn := fmt.Sprintf("deviceID=%s&amqp_uri=%s&timeout=%s&debug=%t&reconnect_enabled=true&reconnect_max_attempts=5&prepared_statements=true",
		deviceID, amqpURL, timeout, debug)

	// Open database connection
	db, err := sql.Open("rabbitsql", dsn)
	if err != nil {
		log.Printf("❌ Error opening database: %v", err)
		return
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Printf("❌ Error pinging database: %v", err)
		return
	}

	fmt.Println("✅ Database connection established")

	// Example 1: Simple SELECT query
	fmt.Println("\n🔍 Example 1: Simple SELECT query")
	query1 := "SELECT id, name FROM users LIMIT 5"
	fmt.Printf("SQL:  %s\n", query1)
	fmt.Printf("EXEC: %s\n", query1)
	rows, err := db.Query(query1)
	if err != nil {
		log.Printf("❌ Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Results:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("❌ Scan error: %v", err)
			continue
		}
		fmt.Printf("  ID: %d, Name: %s\n", id, name)
	}

	fmt.Printf("\n────────────────────────────────────────\n")

	// Example 2: Prepared statement
	fmt.Println("\n🔍 Example 2: Prepared statement with parameters")
	query2 := "SELECT id, name FROM users WHERE id > ? LIMIT ?"
	param1, param2 := 5, 3
	fmt.Printf("Param1: %d\n", param1)
	fmt.Printf("Param2: %d\n", param2)
	fmt.Printf("SQL:  %s\n", query2)
	fmt.Printf("EXEC: SELECT id, name FROM users WHERE id > %d LIMIT %d\n", param1, param2)

	stmt, err := db.Prepare(query2)
	if err != nil {
		log.Printf("❌ Prepare error: %v", err)
		return
	}
	defer stmt.Close()

	rows, err = stmt.Query(param1, param2)
	if err != nil {
		log.Printf("❌ Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Results:")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("❌ Scan error: %v", err)
			continue
		}
		fmt.Printf("  ID: %d, Name: %s\n", id, name)
	}

	fmt.Printf("\n────────────────────────────────────────\n")

	// Example 3: Query with WHERE clause
	fmt.Println("\n🔍 Example 3: Query with WHERE clause")
	query3 := "SELECT id, name FROM users WHERE id <= 3"
	fmt.Printf("SQL:  %s\n", query3)
	fmt.Printf("EXEC: %s\n", query3)
	rows, err = db.Query(query3)
	if err != nil {
		log.Printf("❌ Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Results (users with ID <= 3):")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("❌ Scan error: %v", err)
			continue
		}
		fmt.Printf("  ID: %d, Name: %s\n", id, name)
	}

	fmt.Printf("\n────────────────────────────────────────\n")

	// Example 4: ORDER BY query
	fmt.Println("\n🔍 Example 4: ORDER BY query")
	query4 := "SELECT id, name FROM users ORDER BY name LIMIT 4"
	fmt.Printf("SQL:  %s\n", query4)
	fmt.Printf("EXEC: %s\n", query4)
	rows, err = db.Query(query4)
	if err != nil {
		log.Printf("❌ Query error: %v", err)
		return
	}
	defer rows.Close()

	fmt.Println("Results (ordered by name):")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Printf("❌ Scan error: %v", err)
			continue
		}
		fmt.Printf("  ID: %d, Name: %s\n", id, name)
	}

	fmt.Printf("\n────────────────────────────────────────\n")

	// Example 5: Single row query
	fmt.Println("\n🔍 Example 5: Single row query with QueryRow")
	query5 := "SELECT id, name FROM users WHERE id = 1"
	fmt.Printf("SQL:  %s\n", query5)
	fmt.Printf("EXEC: %s\n", query5)
	row := db.QueryRow(query5)

	var userID int
	var userName string
	if err := row.Scan(&userID, &userName); err != nil {
		log.Printf("❌ QueryRow error: %v", err)
		return
	}

	fmt.Printf("  User with ID 1: %s (ID: %d)\n", userName, userID)

	fmt.Println("\n✅ SQL examples completed successfully!")
}
