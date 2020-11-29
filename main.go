package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	// Import the Elasticsearch library packages
	"github.com/elastic/go-elasticsearch"
	//"github.com/elastic/go-elasticsearch/esapi"
)

type db_sql struct {
	Address     string
	Username    string
	Password    string
	Database    string
	DB          *sql.DB
	Ok          bool
	Tables_seen []string
	Cols_seen   map[string][]string
}

var dbsql = []db_sql{}

type db_es struct {
	Address  string
	Username string
	Password string
	DB       *elasticsearch.Client
	Ok       bool
}

var dbes = []db_es{}

var tablekey = "type"
var hosts = []string{}

func main() {
	fmt.Println("Loading settings from environment")
	listen := testAndSet("LISTEN", ":8080")
	sql_database := testAndSet("SQL_DATABASE", "nifi")
	sql_username := testAndSet("SQL_USERNAME", "admin")
	sql_password := testAndSet("SQL_PASSWORD", "password")
	tablekey = testAndSet("SQL_TABLEKEY", tablekey)
	sql_hosts := strings.Split(testAndSet("SQL_HOSTS", "localhost:3306,localhost6:3306"), ",")
	es_username := testAndSet("ES_USERNAME", "admin")
	es_password := testAndSet("ES_PASSWORD", "password")
	es_hosts := strings.Split(testAndSet("ES_HOSTS", "localhost:9200"), ",")

	http.HandleFunc("/", post)

	fmt.Println("Testing connections to SQL databases...")

	// creation connection structs and fork off a routine to make this connection and keep things running
	for i, h := range sql_hosts {
		if len(h) > 2 {
			t := db_sql{Address: h, Database: sql_database, Username: sql_username, Password: sql_password, Ok: false, Cols_seen: make(map[string][]string)}
			dbsql = append(dbsql, t)
			go DialSQL(i)
		}
	}
	for i, h := range es_hosts {
		if len(h) > 2 {
			t := db_es{Address: h, Username: es_username, Password: es_password, Ok: false}
			dbes = append(dbes, t)
			go DialES(i)
		}
	}

	fmt.Printf("Starting server for testing HTTP POST...\n")
	if err := http.ListenAndServe(listen, nil); err != nil {
		log.Fatal(err)
	}
}

func DialES(i int) {
	wait := 0
	for {
		if dbes[i].Ok == false && wait == 30 {
			fmt.Println("Retrying shortly ", dbes[i].Address)
		}
		time.Sleep(time.Duration(wait) * time.Second)
		wait = 20
		if dbes[i].Ok == false {
			fmt.Println("Dialing ES", dbes[i].Address)
			hostport := strings.SplitN(dbes[i].Address, "/", 4)
			if len(hostport) == 1 {
				hostport = []string{"http:", "", dbes[i].Address, ""}
				dbes[i].Address = "http://" + dbes[i].Address
			}
			for {
				timeout := 3 * time.Second
				conn, err := net.DialTimeout("tcp", hostport[2], timeout)
				if err != nil {
					fmt.Println("Connecting error:", hostport[2], err)
				}
				if conn != nil {
					conn.Close()
					break
				}
				fmt.Println("Waiting for", dbes[i].Address, "TCP port to allow connections")
				time.Sleep(10 * time.Second)
			}
			// Declare an Elasticsearch configuration
			cfg := elasticsearch.Config{
				Addresses: []string{dbes[i].Address},
				Username:  dbes[i].Username,
				Password:  dbes[i].Password,
			}

			// Instantiate a new Elasticsearch client object instance
			client, err := elasticsearch.NewClient(cfg)

			if err != nil {
				fmt.Println("Elasticsearch connection error:", err)
				continue
			}
			info, err := client.Info()
			if err != nil {
				fmt.Println("Elasticsearch info error:", err)
				continue
			}
			fmt.Println("  Connect Success", dbes[i].Address)
			fmt.Println("Elasticsearch info:", info)
			dbes[i].DB = client
			dbes[i].Ok = true
		} else {
			_, err := dbes[i].DB.Info()
			if err != nil {
				if dbes[i].DB != nil {
					//err = dbes[i].DB.Close()
				}
				dbes[i].Ok = false
			}

		}
	}

}
func DialSQL(i int) {
	wait := 0
	for {
		if dbsql[i].Ok == false && wait == 30 {
			fmt.Println("Retrying shortly ", dbsql[i].Address)
		}
		time.Sleep(time.Duration(wait) * time.Second)
		wait = 20
		if dbsql[i].Ok == false {
			fmt.Println("Dialing SQL", dbsql[i].Address)
			for {
				timeout := 3 * time.Second
				conn, err := net.DialTimeout("tcp", dbsql[i].Address, timeout)
				if err != nil {
					fmt.Println("Connecting error:", err)
				}
				if conn != nil {
					conn.Close()
					break
				}
				fmt.Println("Waiting for", dbsql[i].Address, "TCP port to allow connections")
				time.Sleep(10 * time.Second)
			}
			db, err := sql.Open("mysql", dbsql[i].Username+":"+dbsql[i].Password+"@"+"tcp("+dbsql[i].Address+")/?net_write_timeout=6000")
			if err != nil {
				fmt.Println("  Failed to CONNECT to SQL server", dbsql[i].Address, err)
				continue
			}

			// if database does not exist, attempt to create it
			create, err := db.Query("CREATE DATABASE IF NOT EXISTS " + dbsql[i].Database + ";")
			if err != nil {
				fmt.Println("  Warning: CREATE command returned error", err)
				db.Close()
				continue
			}
			create.Close()

			usedb, err := db.Query("USE " + dbsql[i].Database + ";")
			if err != nil {
				fmt.Println("  Warning: USE database command returned error", err)
				db.Close()
				continue
			}
			usedb.Close()

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

			fmt.Println("  Connect Success", dbsql[i].Address)

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
			dbsql[i].Tables_seen = []string{}
			dbsql[i].DB = db
			dbsql[i].Ok = true
		} else {
			err := dbsql[i].DB.Ping()
			if err != nil {
				if dbsql[i].DB != nil {
					err = dbsql[i].DB.Close()
				}
				dbsql[i].Ok = false
				dbsql[i].Tables_seen = []string{}
			}
		}
	}
	//for _, db := range dbsql {
	//	db.Ping()
	//}
}

func post(w http.ResponseWriter, r *http.Request) {
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
			var cols []string

			for key, val := range X {
				if key == tablekey {
					mytable = val.(string)
					continue
				}
				//fmt.Fprintf(w, "  key = %v  val = %v\n", key, val)
				keys = append(keys, fmt.Sprintf("`%s`", key))
				switch v := val.(type) {
				case float64:
					vals = append(vals, fmt.Sprintf("%f", v))
					cols = append(cols, fmt.Sprintf("`%s` DOUBLE", key))
				case string:
					if strings.HasSuffix(key, "Seconds") {
						vals = append(vals, v)
						cols = append(cols, fmt.Sprintf("`%s` DOUBLE", key))
					} else if len(v) == 19 && v[4] == '-' && v[7] == '-' && v[10] == 'T' && v[13] == ':' && v[16] == ':' {
						vals = append(vals, ("STR_TO_DATE(\"" + v + "\",\"%Y-%m-%dT%H:%i:%s\")"))
						cols = append(cols, fmt.Sprintf("`%s` TIMESTAMP", key))
					} else {
						if len(v) < 15 {
							vals = append(vals, fmt.Sprintf("\"%s\"", MysqlRealEscapeString(v)))
							cols = append(cols, fmt.Sprintf("`%s` VARCHAR(30)", key))
						} else {
							vals = append(vals, fmt.Sprintf("\"%s\"", MysqlRealEscapeString(v)))
							cols = append(cols, fmt.Sprintf("`%s` VARCHAR(%d)", key, len(v)*2))
						}
					}
				default:
					log.Printf("I don't know about type %T!\n", v)
				}
			}

			//fmt.Fprintf(w, "  mytable = %v\n", mytable)
			// perform a db.Query insert
			//QueryStr := "INSERT INTO " + mytable + " (" + strings.Join(keys, ",") + ") VALUES (" + strings.Join(vals, ",") + ")"
			//fmt.Fprintf(w, "  qry = %v\n", QueryStr)
			for i, sqlh := range dbsql {
				QueryStr := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES (%s);", sqlh.Database, mytable, strings.Join(keys, ","), strings.Join(vals, ","))
				if sqlh.Ok == true {
					seen := false
					for _, t := range sqlh.Tables_seen {
						if t == mytable {
							seen = true
						}
					}
					if seen == false {
						var tbl string = ""
						err := sqlh.DB.QueryRow(fmt.Sprintf("SHOW TABLES LIKE '%s';", mytable)).Scan(&tbl)
						if err != nil {
							//log.Println("error showing tables", err.Error())
							//} else {
							//if test_table.Next() {
							//	tables_seen = append(tables_seen, mytable)
							//} else {
							create_str := fmt.Sprintf("CREATE TABLE `%s`.`%s` (NiFi_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, %s)", sqlh.Database, mytable, strings.Join(cols, ","))
							//log.Println("creating table", err)
							create, err := sqlh.DB.Query(create_str)
							if err != nil {
								log.Println("error creating table", err, "qry =", create_str)
								continue
							}
							dbsql[i].Cols_seen[mytable] = cols
							dbsql[i].Tables_seen = append(dbsql[i].Tables_seen, mytable)
							create.Close()
						} else {
							if tbl == mytable {
								log.Println("found table", mytable, "in database, don't need to create.")
								getcols_str := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`;", sqlh.Database, mytable)
								getcols, err := sqlh.DB.Query(getcols_str)
								if err != nil {
									log.Println("error getting columns from table", err, "qry =", getcols_str)
									continue
								}
								defer getcols.Close()

								gcols := []string{}
								for getcols.Next() {
									var (
										Field   string
										Type    interface{}
										Null    interface{}
										Key     string
										Default interface{}
										Extra   string
									)
									if err := getcols.Scan(&Field, &Type, &Null, &Key, &Default, &Extra); err != nil {
										log.Println("error scanning cols", err)
										break
									}
									gcols = append(gcols, Field)
								}
								dbsql[i].Cols_seen[mytable] = gcols
								dbsql[i].Tables_seen = append(dbsql[i].Tables_seen, mytable)
							} else {
								log.Println("Show tables passed, but the table name did not match... WHAT?  I'll try ignoring.", tbl, mytable)
							}
						}

					}

					insert, err := sqlh.DB.Query(QueryStr)
					// if there is an error inserting, handle it
					if err != nil {
						log.Println("error encountered ", err.Error())
						log.Println("cols", dbsql[i].Cols_seen[mytable])
						log.Println("  qry = ", QueryStr)
						continue
					}
					insert.Close()
				}
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
