package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net/http"
	"os"
	"strings"
)

var database = "NIFI"
var username = "nifiuser"
var password = "password"
var tablekey = "kind"
var hosts = []string{}
var my_dbs []*sql.DB

func main() {
	fmt.Println("Loading settings from environment")
	database = testAndSet("DATABASE", database)
	username = testAndSet("DATABASE_USERNAME", username)
	password = testAndSet("DATABASE_PASSWORD", password)
	hosts = strings.Split(testAndSet("DATABASE_HOSTS", "localhost:3306,localhost6:3306"), ",")
	tablekey = testAndSet("TABLEKEY", tablekey)
	http.HandleFunc("/", hello)

	fmt.Println("Testing connections to SQL databases...")

	// Open up our database connection.
	// I've set up a database on my local machine using phpmyadmin.
	// The database is called testDb
	for _, h := range hosts {
		fmt.Println(h)
		db, err := sql.Open("mysql", username+":"+password+"@"+"tcp("+h+")/?net_write_timeout=6000")
		if err != nil {
			fmt.Println("  Failed CONNECT", err)
			continue
		}

		// if database does not exist, attempt to create it
		create, err := db.Query("CREATE DATABASE IF NOT EXISTS " + database + ";")
		if err != nil {
			fmt.Println("  Warning: CREATE command returned error", err)
			db.Close()
			continue
		}
		create.Close()
		db.Close()

		db, err = sql.Open("mysql", username+":"+password+"@"+"tcp("+h+")/"+database+"?net_write_timeout=6000")
		if err != nil {
			fmt.Println("  Failed CONNECT", err)
			db.Close()
			continue
		}
		//defer db.Close()

		// perform a test query
		var val int
		err = db.QueryRow("SELECT 42 as test;").Scan(&val)
		//fmt.Printf("test = %v\n", val)
		if err != nil {
			fmt.Println("  Failed SELECT test query", err)
			db.Close()
			continue
		}

		if val != 42 {
			fmt.Println("  SELECT query replied with invalid result")
			db.Close()
			continue
		}

		fmt.Println("  Success")

		/*res, err := db.Query("SHOW TABLES;")
		if err != nil {
			fmt.Println("  Failed to list tables", err)
			continue
		}
		defer res.Close()

		var table string

		for res.Next() {
			res.Scan(&table)
			fmt.Println(table)
		}*/
		my_dbs = append(my_dbs, db)
	}

	// if there is an error opening the connection, handle it

	// defer the close till after the main function has finished
	// executing
	//fmt.Printf("test = %v\n", my_dbs)

	fmt.Printf("Starting server for testing HTTP POST...\n")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func hello(w http.ResponseWriter, r *http.Request) {
	/*if r.URL.Path != "/" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}*/

	switch r.Method {
	case "GET":
		//http.ServeFile(w, r, "form.html")
		fmt.Fprintf(w, "Nifi endpoint ready.")
	case "POST":
		// Call ParseForm() to parse the raw query and update r.PostForm and r.Form.

		//fmt.Fprintf(w, "Post from website! r = %v\n", r)
		if err := r.ParseForm(); err != nil {
			fmt.Fprintf(w, "Error parsing POST: %v", err)
			return
		}

		var X map[string]interface{}
		for _, db := range my_dbs {
			db.Ping()
		}

		//fmt.Fprintf(w, "Post from website! r.PostFrom = %v\n", r.PostForm)
		for dat := range r.PostForm {
			//fmt.Fprintf(w, "  dat = %v\n", dat)
			if err := json.Unmarshal([]byte(dat), &X); err != nil {
				log.Printf("  err parsing dat= %v, %v\n", dat, err)
				return
			}
			//fmt.Fprintf(w, "  dat = %v\n", X)

			mytable := ""
			var keys []string
			var vals []string

			for key, val := range X {
				if key == tablekey {
					mytable = val.(string)
					continue
				}
				//fmt.Fprintf(w, "  key = %v  val = %v\n", key, val)
				keys = append(keys, key)
				switch v := val.(type) {
				case float64:
					vals = append(vals, fmt.Sprintf("%g", val))
				case string:
					vals = append(vals, "\""+MysqlRealEscapeString(val.(string))+"\"")
				default:
					log.Printf("I don't know about type %T!\n", v)
				}
			}

			//fmt.Fprintf(w, "  mytable = %v\n", mytable)
			// perform a db.Query insert
			QueryStr := "INSERT INTO " + mytable + " (" + strings.Join(keys, ",") + ") VALUES (" + strings.Join(vals, ",") + ")"
			//fmt.Fprintf(w, "  qry = %v\n", QueryStr)
			for _, db := range my_dbs {
				insert, err := db.Query(QueryStr)

				// if there is an error inserting, handle it
				if err != nil {
					log.Println("error encountered ", err.Error())
					log.Println("  qry = ", QueryStr)
					continue
				}
				insert.Close()
			}
		}
		//name := r.FormValue("name")
		//address := r.FormValue("address")
		//fmt.Fprintf(w, "Name = %s\n", name)
		//fmt.Fprintf(w, "Address = %s\n", address)
	default:
		fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
	}
}

func testAndSet(env string, defaultval string) string {
	val, present := os.LookupEnv(env)
	if present {
		fmt.Printf("%-20s: %s\n", env, val)
		return val
	} else {
		fmt.Printf("%-20s: %s  (default)\n", env, defaultval)
		return defaultval
	}
}

func MysqlRealEscapeString(value string) string {
	replace := map[string]string{"\\": "\\\\", "'": `\'`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}

	for b, a := range replace {
		value = strings.Replace(value, b, a, -1)
	}

	return value
}
