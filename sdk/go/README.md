# AIONBD Go SDK

Official Go client for the AIONBD HTTP API.

## Requirements

- Go `>= 1.22`

## Install (workspace)

```bash
cd sdk/go
go test ./...
```

## Quick Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	aionbd "github.com/aionbd/aionbd/sdk/go"
)

func main() {
	client := aionbd.NewClient("http://127.0.0.1:8080", nil)

	live, err := client.Live(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(live.Status)

	collection, err := client.CreateCollection(context.Background(), "demo", 3, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(collection.Name)
}
```

## Auth Usage

API key:

```go
client := aionbd.NewClient("http://127.0.0.1:8080", &aionbd.ClientOptions{
	APIKey: "secret-key-a",
})
```

Bearer token:

```go
client := aionbd.NewClient("http://127.0.0.1:8080", &aionbd.ClientOptions{
	BearerToken: "token-a",
})
```

## API Coverage

- `Live`, `Ready`, `Health`
- `Metrics`, `MetricsPrometheus`
- `Distance`
- `CreateCollection`
- `ListCollections`, `GetCollection`, `DeleteCollection`
- `UpsertPoint`, `UpsertPointsBatch`
- `GetPoint`, `DeletePoint`
- `ListPoints`
- `SearchCollection`
- `SearchCollectionTopK`
- `SearchCollectionTopKBatch`

## Run Tests

```bash
cd sdk/go
go test ./...
```
