## LogDB

LogDB is a very simple and dumb key-value database, backed by a single, append-only file.

* Has in-memory index.
* When an entry is deleted, it will be marked as tombstone. The value is not written.

LogDB's purpose is to demonstrate how to implement a basic key-value database.

---

#### Index description
* Index is implemented using map. <br />
* Each Entry has 3 fields: meta, key length, value length. <br />
* Meta is a byte. It's used to mark tombstone. <br />
* Key and value are stored using varuint. 

## Getting started

```go
package main

import (
	"log"

	logdb "github.com/ahmadmuzakkir/logdb"
)

func main() {
	db, err := logdb.Open("my.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
    err = db.Set([]byte("my-key"), []byte("my-value"))
	if err != nil {
		log.Fatal(err)
	}
    
    val, err := db.Get([]byte("my-key"))
    if err != nil {
        log.Fatal(err)
    }
    
    err = db.Delete([]byte("my-key"))
    if err != nil {
        log.Fatal(err)
    }
}
```