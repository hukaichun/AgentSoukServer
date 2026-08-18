// The agent's hands. Every one of these reads; none writes, execs, spawns a
// shell, or changes anything in the pod. That is the whole design: the point
// was never to automate the colleague who edits source inside a running pod
// — it was to make going in unnecessary, by answering from outside what he
// used to go in to see. There is deliberately no counterpart that writes.
//
// Pure stdlib and /proc, so this works in a distroless or scratch image that
// has no `ps`, no `stat`, no shell — which is exactly the image a probe most
// often needs to inspect.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxReadBytes bounds a file read so a probe answer can never put a
// multi-megabyte file on the wire. A file larger than this is reported
// truncated, with its real size named.
const maxReadBytes = 64 * 1024

type fileFact struct {
	Path    string
	Size    int64
	Mode    string
	ModTime time.Time
}

func statFile(path string) (fileFact, error) {
	info, err := os.Stat(path)
	if err != nil {
		return fileFact{}, err
	}
	return fileFact{
		Path:    path,
		Size:    info.Size(),
		Mode:    info.Mode().String(),
		ModTime: info.ModTime(),
	}, nil
}

func listDir(path string) ([]fileFact, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	facts := make([]fileFact, 0, len(entries))
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		facts = append(facts, fileFact{
			Path:    filepath.Join(path, e.Name()),
			Size:    info.Size(),
			Mode:    info.Mode().String(),
			ModTime: info.ModTime(),
		})
	}
	return facts, nil
}

func readFile(path string) (string, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	buf := make([]byte, maxReadBytes)
	n, err := f.Read(buf)
	if err != nil && n == 0 {
		return "", false, err
	}
	info, _ := f.Stat()
	truncated := info != nil && info.Size() > int64(n)
	return string(buf[:n]), truncated, nil
}

// newestFiles walks root and returns the n files with the most recent
// modification times. This is the probe that catches a hand-edit: a file
// modified after the image was built stands out here, because everything
// original shares the build's timestamp and an edit does not.
func newestFiles(root string, n int) ([]fileFact, error) {
	var facts []fileFact
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable subtree: skip, don't abort the whole walk
		}
		if d.IsDir() {
			if skipDir(path, root) {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		facts = append(facts, fileFact{
			Path:    path,
			Size:    info.Size(),
			Mode:    info.Mode().String(),
			ModTime: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].ModTime.After(facts[j].ModTime) })
	if len(facts) > n {
		facts = facts[:n]
	}
	return facts, nil
}

// skipDir keeps the walk out of subtrees that are noise for a "what changed"
// question and expensive to descend — virtual filesystems and dependency
// directories whose churn is not a person editing source.
func skipDir(path, root string) bool {
	if path == root {
		return false
	}
	base := filepath.Base(path)
	switch base {
	case "proc", "sys", "dev", ".git", "node_modules", "__pycache__", ".venv", "site-packages":
		return true
	}
	return false
}

// podStarted reads process 1's start time as the closest honest marker of
// when this pod began — a file modified after it is a change made to a
// running pod, not something baked into the image. Best-effort: an empty
// time means "could not tell", which the report states rather than guessing.
func podStarted() (time.Time, bool) {
	info, err := os.Stat("/proc/1")
	if err != nil {
		return time.Time{}, false
	}
	// ModTime of /proc/1 tracks process 1's start closely enough for
	// "before or after the pod came up"; it needs no parsing of stat fields.
	return info.ModTime(), true
}

// processList reads /proc for the running processes, no `ps` required. Each
// entry is (pid, comm) from /proc/<pid>/comm — enough to answer "what is
// running in here" without reading anyone's cmdline, which can carry
// secrets passed as flags.
func processList() ([]string, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}
	var procs []string
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
		if err != nil {
			continue
		}
		procs = append(procs, fmt.Sprintf("%d %s", pid, strings.TrimSpace(string(comm))))
	}
	return procs, nil
}

// envSummary reports environment variable names and values with anything
// that looks like a credential redacted. Names travel because "is DEBUG
// set" is a real diagnostic question; values of secret-shaped names do not,
// because a probe answer is relayed to whoever asked.
func envSummary() []string {
	var out []string
	for _, kv := range os.Environ() {
		key, val, found := strings.Cut(kv, "=")
		if !found {
			continue
		}
		if looksSecret(key) {
			val = "<redacted>"
		}
		out = append(out, key+"="+val)
	}
	sort.Strings(out)
	return out
}

func looksSecret(key string) bool {
	upper := strings.ToUpper(key)
	for _, marker := range []string{"KEY", "SECRET", "TOKEN", "PASSWORD", "PASSWD", "CREDENTIAL", "PRIVATE"} {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}

// readProcFile is a bounded reader used by callers that want a single /proc
// line (kept small, since /proc files can block or be huge).
func readProcLine(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		return scanner.Text()
	}
	return ""
}
