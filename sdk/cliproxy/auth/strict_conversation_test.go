package auth

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestStrictConversationDurableSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	makeManager := func() *Manager {
		m := NewManager(nil, &RoundRobinSelector{}, nil)
		m.executors["gemini"] = schedulerTestExecutor{}
		for _, id := range []string{"a", "b"} {
			if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "gemini"}); err != nil {
				t.Fatal(err)
			}
		}
		m.runtimeConfig.Store(&internalconfig.Config{Routing: internalconfig.RoutingConfig{StrictConversationAffinity: true, ConversationBindingsFile: path}})
		return m
	}
	m := makeManager()
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{"thread"}}}
	if _, _, err := m.pickNext(context.Background(), "gemini", "", opts, nil); err == nil {
		t.Fatal("missing caller accepted")
	}
	ctx := WithConversationCaller(context.Background(), "caller-a")
	first, _, err := m.pickNext(ctx, "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := makeManager().pickNext(ctx, "gemini", "", opts, nil)
	if err != nil || second.ID != first.ID {
		t.Fatalf("restart binding %v, %v", second, err)
	}
	if _, _, err = m.pickNext(ctx, "gemini", "", opts, map[string]struct{}{first.ID: {}}); err == nil {
		t.Fatal("failover selected another account")
	}
	if _, _, err = m.pickNext(ctx, "gemini", "", cliproxyexecutor.Options{}, nil); err == nil {
		t.Fatal("missing conversation accepted")
	}
}

func TestStrictConversationProcessRestartAndConcurrentWriters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bindings.db")
	if err := InitializeConversationBindings(path); err != nil {
		t.Fatal(err)
	}
	run := func(caller, conversation, preferred string) string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.v", "-test.run=^TestStrictConversationChild$")
		cmd.Env = append(os.Environ(), "STRICT_CHILD_DB="+path, "STRICT_CHILD_CALLER="+caller, "STRICT_CHILD_ID="+conversation, "STRICT_CHILD_PREFERRED="+preferred)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("child: %v: %s", err, output)
		}
		match := regexp.MustCompile(`strict-selected=(child-[ab])`).FindSubmatch(output)
		if len(match) != 2 {
			t.Fatalf("child did not report selection: %s", output)
		}
		return string(match[1])
	}
	const workers = 8
	results := make([]string, workers)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			preferred := []string{"child-a", "child-b"}[i%2]
			results[i] = run("caller-a", "conversation-a", preferred)
		}(i)
	}
	wg.Wait()
	for _, result := range results[1:] {
		if result != results[0] {
			t.Fatalf("concurrent processes disagreed: %q != %q", result, results[0])
		}
	}
	opposite := "child-a"
	if results[0] == opposite {
		opposite = "child-b"
	}
	if later := run("caller-a", "conversation-a", opposite); later != results[0] {
		t.Fatalf("restart selected different account: %q != %q", later, results[0])
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, _, err := strictChildManager(path).pickNext(WithConversationCaller(context.Background(), "caller-a"), "gemini", "", cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{"conversation-a"}}}, nil); err == nil {
		t.Fatal("storage loss silently reset binding")
	}
}

func strictChildManager(path string) *Manager {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.executors["gemini"] = schedulerTestExecutor{}
	for _, id := range []string{"child-a", "child-b"} {
		attributes := map[string]string{}
		if id == os.Getenv("STRICT_CHILD_PREFERRED") {
			attributes["priority"] = "100"
		}
		_, _ = m.Register(context.Background(), &Auth{ID: id, Provider: "gemini", Attributes: attributes})
	}
	m.runtimeConfig.Store(&internalconfig.Config{Routing: internalconfig.RoutingConfig{StrictConversationAffinity: true, ConversationBindingsFile: path}})
	return m
}

func TestStrictConversationChild(t *testing.T) {
	path := os.Getenv("STRICT_CHILD_DB")
	if path == "" {
		return
	}
	m := strictChildManager(path)
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Cliproxy-Conversation-Id": []string{os.Getenv("STRICT_CHILD_ID")}}}
	auth, _, err := m.pickNext(WithConversationCaller(context.Background(), os.Getenv("STRICT_CHILD_CALLER")), "gemini", "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The test runner also prints PASS; the selected ID is compared across processes.
	t.Log("strict-selected=" + auth.ID)
}
