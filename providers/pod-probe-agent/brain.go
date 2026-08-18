package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Brain answers a caller's question about the pod. Its shape is a
// deliberate security property, not a shortcut: it gathers a fixed set of
// read-only facts *first*, and only then — if an LLM is reachable — asks the
// model to interpret those facts for the question. The model never decides
// what to read, stat, or walk. It cannot reach a file the deterministic pass
// did not already put in front of it, so a prompt-injected or misled model
// still cannot make this agent touch anything new. Giving the model the
// tools directly is a later, larger step that trades this property away, and
// is out of scope here on purpose.
type Brain struct {
	root string // the subtree a "what changed" question walks; the pod's app dir
	llm  *LLMClient
}

func NewBrain(root string, llm *LLMClient) *Brain {
	return &Brain{root: root, llm: llm}
}

// canInterpret reports whether a model call has a real target for this run:
// a KYOK token routes through souk's relay, and an own base URL is the
// fallback. Without either there is nothing to call, so the brain returns
// the raw facts rather than attempting a request with no endpoint.
func (b *Brain) canInterpret(kyok *KyokToken) bool {
	if b.llm == nil {
		return false
	}
	return (kyok != nil && kyok.Token != "") || b.llm.baseURL != ""
}

// Answer is the AnswerFunc handed to the wire. kyok is the run-scoped token
// from the caller, if any — an LLM call made with it is paid for with the
// caller's key through souk's relay, so this binary holds no model
// credential of its own.
func (b *Brain) Answer(ctx context.Context, question string, kyok *KyokToken) (string, error) {
	report := b.gather()
	if !b.canInterpret(kyok) {
		// No model reachable for this run — neither a KYOK token from the
		// caller nor an own endpoint configured. Return the facts
		// themselves. Still a useful, honest answer, and the mode the wire
		// is verified in without any LLM credentials present.
		return report, nil
	}
	interpreted, err := b.llm.Interpret(ctx, question, report, kyok)
	if err != nil {
		// The model failed; the facts did not. Hand back the raw report
		// with a note rather than nothing.
		return report + "\n\n(could not reach a model to interpret these facts: " + err.Error() + ")", nil
	}
	return interpreted, nil
}

// gather runs the read-only probes and formats them into one plain-text
// report. Order is fixed and the content is deterministic given the pod's
// state, which is what makes the wire testable without a model.
func (b *Brain) gather() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Pod state probe of %s\n", b.root)

	if started, ok := podStarted(); ok {
		fmt.Fprintf(&sb, "Pod (pid 1) started around: %s\n", started.Format(time.RFC3339))
	} else {
		sb.WriteString("Pod start time: could not determine\n")
	}

	sb.WriteString("\nMost recently modified files (a file changed after the pod started is an edit to a running pod, not something from the image):\n")
	newest, err := newestFiles(b.root, 15)
	if err != nil {
		fmt.Fprintf(&sb, "  could not walk %s: %v\n", b.root, err)
	} else if len(newest) == 0 {
		fmt.Fprintf(&sb, "  no files found under %s\n", b.root)
	} else {
		started, hasStart := podStarted()
		for _, f := range newest {
			marker := ""
			if hasStart && f.ModTime.After(started) {
				marker = "  <-- MODIFIED AFTER POD START"
			}
			fmt.Fprintf(&sb, "  %s  %8d  %s%s\n", f.ModTime.Format(time.RFC3339), f.Size, f.Path, marker)
		}
	}

	sb.WriteString("\nProcesses:\n")
	if procs, err := processList(); err != nil {
		fmt.Fprintf(&sb, "  could not read /proc: %v\n", err)
	} else {
		for _, p := range procs {
			fmt.Fprintf(&sb, "  %s\n", p)
		}
	}

	sb.WriteString("\nEnvironment (secret-shaped values redacted):\n")
	for _, e := range envSummary() {
		fmt.Fprintf(&sb, "  %s\n", e)
	}

	return sb.String()
}
