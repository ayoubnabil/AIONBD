package aionbd

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

const serverReadyTimeout = 90 * time.Second

func TestClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	baseURL, stopServer := startServer(t)
	defer stopServer()

	client := NewClient(baseURL, &ClientOptions{Timeout: 10 * time.Second})
	ctx := context.Background()
	collectionName := fmt.Sprintf("go_sdk_demo_%d", time.Now().UnixNano())

	requireLiveAndReady(t, ctx, client)
	requireCollectionCreated(t, ctx, client, collectionName)
	t.Cleanup(func() {
		_, _ = client.DeleteCollection(context.Background(), collectionName)
	})

	requireDistance(t, ctx, client)
	requireUpserts(t, ctx, client, collectionName)
	requirePointRead(t, ctx, client, collectionName)
	requireSearches(t, ctx, client, collectionName)
	requireLists(t, ctx, client, collectionName)
	requirePointDeleted(t, ctx, client, collectionName)
	requireMetrics(t, ctx, client)
}

func startServer(t *testing.T) (string, func()) {
	t.Helper()

	port := reserveTCPPort(t)
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller path")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
	serverPath := resolveServerPath(t, repoRoot)

	ctx, cancel := context.WithCancel(context.Background())
	keepCancel := false
	defer func() {
		if !keepCancel {
			cancel()
		}
	}()

	command := exec.CommandContext(ctx, serverPath)
	command.Dir = repoRoot
	command.Env = append(os.Environ(),
		fmt.Sprintf("AIONBD_BIND=127.0.0.1:%d", port),
		"AIONBD_PERSISTENCE_ENABLED=false",
		"AIONBD_WAL_SYNC_ON_WRITE=false",
		"RUST_LOG=warn",
	)

	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to capture server stdout: %v", err)
	}
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatalf("failed to capture server stderr: %v", err)
	}

	logs := &logBuffer{}
	go logs.capture("stdout", stdout)
	go logs.capture("stderr", stderr)

	if err := command.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = command.Wait()
		close(done)
	}()

	waitForReady(t, baseURL, done, func() error { return waitErr }, logs)

	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			if command.Process != nil {
				_ = command.Process.Kill()
			}
			<-done
		}
	}

	keepCancel = true
	return baseURL, stop
}

func resolveServerPath(t *testing.T, repoRoot string) string {
	t.Helper()

	serverPath := filepath.Join(repoRoot, "target", "debug", "aionbd-server")
	if runtime.GOOS == "windows" {
		serverPath += ".exe"
	}

	info, err := os.Stat(serverPath)
	if err != nil || !info.Mode().IsRegular() {
		t.Skipf("missing server binary %s; run `cargo test -p aionbd-server` first", serverPath)
	}
	return serverPath
}

func waitForReady(t *testing.T, baseURL string, done <-chan struct{}, waitErr func() error, logs *logBuffer) {
	t.Helper()

	deadline := time.Now().Add(serverReadyTimeout)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatalf("aionbd-server exited before readiness check: %v\n%s", waitErr(), logs.dump())
		default:
		}

		response, err := http.Get(baseURL + "/live")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for server readiness on %s\n%s", baseURL, logs.dump())
}

func reserveTCPPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to reserve tcp port: %v", err)
	}
	defer listener.Close()

	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatal("failed to parse listener address")
	}
	return address.Port
}

func requireLiveAndReady(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	live, err := client.Live(ctx)
	if err != nil {
		t.Fatalf("live failed: %v", err)
	}
	if live.Status != "live" {
		t.Fatalf("unexpected live status: %s", live.Status)
	}

	ready, err := client.Ready(ctx)
	if err != nil {
		t.Fatalf("ready failed: %v", err)
	}
	if ready.Status != "ready" {
		t.Fatalf("unexpected ready status: %s", ready.Status)
	}
}

func requireCollectionCreated(t *testing.T, ctx context.Context, client *Client, name string) {
	t.Helper()

	created, err := client.CreateCollection(ctx, name, 4, true)
	if err != nil {
		t.Fatalf("create collection failed: %v", err)
	}
	if created.Name != name {
		t.Fatalf("unexpected collection name: %s", created.Name)
	}
}

func requireDistance(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	distance, err := client.Distance(ctx, []float32{1, 0, 0, 0}, []float32{1, 0, 0, 0}, MetricL2)
	if err != nil {
		t.Fatalf("distance failed: %v", err)
	}
	if distance.Value != 0 {
		t.Fatalf("expected l2 distance 0, got %f", distance.Value)
	}
}

func requireUpserts(t *testing.T, ctx context.Context, client *Client, collectionName string) {
	t.Helper()

	upsert, err := client.UpsertPoint(ctx, collectionName, 1, []float32{1, 0, 0, 0}, PointPayload{"label": "alpha"})
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if upsert.ID != 1 {
		t.Fatalf("unexpected upsert id: %d", upsert.ID)
	}

	batch, err := client.UpsertPointsBatch(ctx, collectionName, []UpsertPointsBatchItem{
		{ID: 2, Values: []float32{0.8, 0.1, 0, 0}, Payload: PointPayload{"label": "beta"}},
		{ID: 3, Values: []float32{0, 1, 0, 0}, Payload: PointPayload{"label": "gamma"}},
	})
	if err != nil {
		t.Fatalf("batch upsert failed: %v", err)
	}
	if batch.Created+batch.Updated != 2 {
		t.Fatalf("unexpected batch counters: created=%d updated=%d", batch.Created, batch.Updated)
	}
}

func requirePointRead(t *testing.T, ctx context.Context, client *Client, collectionName string) {
	t.Helper()

	point, err := client.GetPoint(ctx, collectionName, 2)
	if err != nil {
		t.Fatalf("get point failed: %v", err)
	}
	if point.Payload["label"] != "beta" {
		t.Fatalf("unexpected payload: %#v", point.Payload)
	}
}

func requireSearches(t *testing.T, ctx context.Context, client *Client, collectionName string) {
	t.Helper()

	top1, err := client.SearchCollection(ctx, collectionName, []float32{1, 0, 0, 0}, &SearchOptions{
		Metric:         MetricDot,
		Mode:           SearchModeExact,
		IncludePayload: BoolPtr(true),
	})
	if err != nil {
		t.Fatalf("top1 failed: %v", err)
	}
	if top1.ID != 1 {
		t.Fatalf("unexpected top1 id: %d", top1.ID)
	}

	topK, err := client.SearchCollectionTopK(ctx, collectionName, []float32{1, 0, 0, 0}, &SearchTopKOptions{
		SearchOptions: SearchOptions{
			Metric:         MetricDot,
			Mode:           SearchModeAuto,
			IncludePayload: BoolPtr(true),
		},
		Limit: IntPtr(2),
	})
	if err != nil {
		t.Fatalf("top-k failed: %v", err)
	}
	if len(topK.Hits) != 2 || topK.Hits[0].ID != 1 {
		t.Fatalf("unexpected top-k hits: %#v", topK.Hits)
	}

	batchSearch, err := client.SearchCollectionTopKBatch(
		ctx,
		collectionName,
		[][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}},
		&SearchTopKOptions{
			SearchOptions: SearchOptions{
				Metric: MetricDot,
				Mode:   SearchModeAuto,
			},
			Limit: IntPtr(2),
		},
	)
	if err != nil {
		t.Fatalf("batch search failed: %v", err)
	}
	if len(batchSearch.Results) != 2 {
		t.Fatalf("unexpected batch search results: %#v", batchSearch.Results)
	}
}

func requireLists(t *testing.T, ctx context.Context, client *Client, collectionName string) {
	t.Helper()

	listedOffset, err := client.ListPoints(ctx, collectionName, nil)
	if err != nil {
		t.Fatalf("list points (offset) failed: %v", err)
	}
	if len(listedOffset.Points) != 3 {
		t.Fatalf("unexpected offset list length: %d", len(listedOffset.Points))
	}

	listedCursor, err := client.ListPoints(ctx, collectionName, &ListPointsOptions{
		AfterID: Uint64Ptr(1),
		Limit:   IntPtr(10),
	})
	if err != nil {
		t.Fatalf("list points (cursor) failed: %v", err)
	}
	if len(listedCursor.Points) != 2 || listedCursor.Points[0].ID != 2 {
		t.Fatalf("unexpected cursor list result: %#v", listedCursor.Points)
	}
}

func requirePointDeleted(t *testing.T, ctx context.Context, client *Client, collectionName string) {
	t.Helper()

	deletedPoint, err := client.DeletePoint(ctx, collectionName, 3)
	if err != nil {
		t.Fatalf("delete point failed: %v", err)
	}
	if !deletedPoint.Deleted {
		t.Fatalf("point not deleted: %#v", deletedPoint)
	}
}

func requireMetrics(t *testing.T, ctx context.Context, client *Client) {
	t.Helper()

	metrics, err := client.Metrics(ctx)
	if err != nil {
		t.Fatalf("metrics failed: %v", err)
	}
	if metrics.Collections < 1 {
		t.Fatalf("unexpected metrics collections value: %d", metrics.Collections)
	}

	prometheusText, err := client.MetricsPrometheus(ctx)
	if err != nil {
		t.Fatalf("metrics prometheus failed: %v", err)
	}
	if !strings.Contains(prometheusText, "aionbd_") {
		t.Fatalf("unexpected prometheus output: %q", prometheusText)
	}
}

type logBuffer struct {
	mu    sync.Mutex
	lines []string
}

func (buffer *logBuffer) capture(prefix string, reader interface{ Read([]byte) (int, error) }) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		buffer.append(prefix + ": " + scanner.Text())
	}
}

func (buffer *logBuffer) append(line string) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	buffer.lines = append(buffer.lines, line)
	if len(buffer.lines) > 400 {
		buffer.lines = buffer.lines[len(buffer.lines)-400:]
	}
}

func (buffer *logBuffer) dump() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return strings.Join(buffer.lines, "\n")
}
