// collab-plan: a two-person TCP tunnel that bridges terminal chat and each
// person's local agent process for a shared planning session.
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

type msg struct {
	Kind string `json:"kind"` // chat | agent | decision
	From string `json:"from"`
	Text string `json:"text"`
}

type session struct {
	name      string
	conn      net.Conn
	enc       *json.Encoder
	mu        sync.Mutex // guards enc, stdout/log writes, decisions, people
	log       io.Writer
	out       io.Writer
	agent     io.WriteCloser // nil when no local agent
	start     time.Time
	people    []string
	decisions []msg
}

func (s *session) addPerson(n string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.people, n) {
		s.people = append(s.people, n)
	}
}

func (s *session) send(m msg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(m)
}

func (s *session) show(m msg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tag := m.From
	if m.Kind == "agent" {
		tag += "/agent"
	}
	if m.Kind == "decision" {
		s.decisions = append(s.decisions, m)
		tag += "/decision"
	}
	line := fmt.Sprintf("[%s] %s\n", tag, m.Text)
	io.WriteString(s.out, line)
	if s.log != nil {
		fmt.Fprintf(s.log, "%s %s", time.Now().Format("15:04:05"), line)
	}
}

func (s *session) toAgent(text string) {
	if s.agent != nil {
		io.WriteString(s.agent, text+"\n")
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  collab-plan host [-listen :7777] [-advertise HOST] [-name N] [-agent CMD] [-fifo PIPE] [-log FILE]
  collab-plan join collab-plan://HOST:PORT/TOKEN [-name N] [-agent CMD] [-fifo PIPE] [-log FILE]
  collab-plan next -log FILE -name N [-timeout 10m]   (block until new peer lines)

host prints a link and a paste-ready prompt; the peer pastes it into their
Claude session. Lines typed on stdin or written to -fifo go to the peer.
With -agent CMD, remote chat is fed to CMD and CMD's stdout is shared.
/quit leaves. Across NAT use tailscale or "ssh -R 7777:localhost:7777 server".
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	if cmd == "next" {
		runNext(os.Args[2:])
		return
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	name := fs.String("name", defaultName(), "display name")
	agentCmd := fs.String("agent", "", "local agent command (run via sh -c)")
	logFile := fs.String("log", "", "append transcript to FILE")
	listen := fs.String("listen", ":7777", "host: listen address")
	advertise := fs.String("advertise", "", "host: host/IP put in the link (default: Tailscale IP, else first LAN IP)")
	fifo := fs.String("fifo", "", "read outgoing lines from this named pipe (for agents: echo text > FIFO)")
	out := fs.String("out", "", "decisions markdown path (default plan-<timestamp>.md)")

	var link string
	args := os.Args[2:]
	if cmd == "join" {
		if len(args) < 1 {
			usage()
		}
		link, args = args[0], args[1:]
	}
	fs.Parse(args)

	var conn net.Conn
	var err error
	switch cmd {
	case "host":
		conn, err = host(*listen, *advertise)
	case "join":
		var addr, tok string
		addr, tok, err = parseLink(link)
		if err == nil {
			conn, err = join(addr, tok)
		}
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer conn.Close()

	s := &session{name: *name, conn: conn, enc: json.NewEncoder(conn), out: os.Stdout, start: time.Now(), people: []string{*name}}
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		defer f.Close()
		s.log = f
	}
	if *agentCmd != "" {
		if err := s.startAgent(*agentCmd); err != nil {
			fmt.Fprintln(os.Stderr, "error: agent:", err)
			os.Exit(1)
		}
	}

	done := make(chan struct{})
	go func() { s.readRemote(); close(done) }()
	go s.readInput(os.Stdin, *fifo == "")
	if *fifo != "" {
		if err := s.watchFifo(*fifo); err != nil {
			fmt.Fprintln(os.Stderr, "error: fifo:", err)
			os.Exit(1)
		}
		defer os.Remove(*fifo)
	}
	<-done
	fmt.Println("session ended, tunnel closed")
	if path, err := s.writeSummary(*out); err != nil {
		fmt.Fprintln(os.Stderr, "error: summary:", err)
		os.Exit(1)
	} else {
		fmt.Println("decisions written to", path)
	}
}

func defaultName() string {
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	return "anon"
}

func (s *session) writeSummary(path string) (string, error) {
	if path == "" {
		path = "plan-" + time.Now().Format("20060102-150405") + ".md"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "# Planning session %s\n\nParticipants: %s\n\n## Decisions\n\n", s.start.Format("2006-01-02 15:04"), strings.Join(s.people, ", "))
	if len(s.decisions) == 0 {
		b.WriteString("_No decisions were recorded._\n")
	}
	for i, d := range s.decisions {
		fmt.Fprintf(&b, "%d. %s (%s)\n", i+1, d.Text, d.From)
	}
	return path, os.WriteFile(path, []byte(b.String()), 0o644)
}

func join(addr, tok string) (net.Conn, error) {
	c, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	io.WriteString(c, tok+"\n")
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	// Read byte-wise so no post-handshake data is buffered away.
	var resp []byte
	one := make([]byte, 1)
	for {
		if _, err := c.Read(one); err != nil {
			c.Close()
			return nil, fmt.Errorf("handshake failed (bad token?)")
		}
		if one[0] == '\n' {
			break
		}
		resp = append(resp, one[0])
	}
	c.SetReadDeadline(time.Time{})
	if string(resp) != "ok" {
		c.Close()
		return nil, fmt.Errorf("handshake rejected")
	}
	return c, nil
}

func (s *session) startAgent(command string) error {
	c := exec.Command("sh", "-c", command)
	c.Stderr = os.Stderr
	in, err := c.StdinPipe()
	if err != nil {
		return err
	}
	out, err := c.StdoutPipe()
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}
	s.agent = in
	go func() {
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			m := msg{Kind: "agent", From: s.name, Text: sc.Text()}
			s.show(m)
			if s.send(m) != nil {
				return
			}
		}
	}()
	return nil
}

func (s *session) readRemote() {
	sc := bufio.NewScanner(s.conn)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var m msg
		if json.Unmarshal(sc.Bytes(), &m) != nil {
			continue
		}
		s.show(m)
		s.addPerson(m.From)
		// Agents only see peer humans' chat, not peer agent output, to avoid loops.
		if m.Kind == "chat" {
			s.toAgent(fmt.Sprintf("[%s] %s", m.From, m.Text))
		}
	}
}

// readInput reads outgoing lines from r. Stdin EOF ends the session only when
// no fifo is in use, because a backgrounded agent shell has stdin at /dev/null.
func (s *session) readInput(r io.Reader, closeOnEOF bool) {
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		t := strings.TrimSpace(sc.Text())
		if t == "" {
			continue
		}
		if t == "/quit" {
			s.conn.Close()
			return
		}
		kind := "chat"
		if d, ok := strings.CutPrefix(t, "/decide "); ok {
			kind, t = "decision", strings.TrimSpace(d)
		}
		m := msg{Kind: kind, From: s.name, Text: t}
		s.show(m)
		if kind == "chat" {
			s.toAgent(t)
		}
		if s.send(m) != nil {
			return
		}
	}
	if closeOnEOF {
		s.conn.Close()
	}
}

func (s *session) watchFifo(path string) error {
	os.Remove(path)
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		return err
	}
	// O_RDWR keeps the pipe open between writers so reads never hit EOF.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	go s.readInput(f, false)
	return nil
}

func parseLink(link string) (addr, tok string, err error) {
	u, err := url.Parse(strings.TrimSpace(link))
	tok = strings.Trim(u.Path, "/")
	if err != nil || u.Scheme != "collab-plan" || u.Host == "" || tok == "" {
		return "", "", fmt.Errorf("invalid link %q, want collab-plan://HOST:PORT/TOKEN", link)
	}
	return u.Host, tok, nil
}

// runNext blocks until the log has new lines from the peer, prints them, and
// exits. The read position is kept in <log>.cursor so each call returns only
// unseen lines. It is meant to be run by an agent as a background task: the
// task finishing is what wakes the agent up.
func runNext(args []string) {
	fs := flag.NewFlagSet("next", flag.ExitOnError)
	logPath := fs.String("log", "", "transcript file written by host/join")
	name := fs.String("name", defaultName(), "own display name; lines from it are skipped")
	timeout := fs.Duration("timeout", 10*time.Minute, "give up after this long (exits 0, prints nothing)")
	fs.Parse(args)
	if *logPath == "" {
		fmt.Fprintln(os.Stderr, "error: -log is required")
		os.Exit(2)
	}
	cursor := *logPath + ".cursor"
	var off int64
	if b, err := os.ReadFile(cursor); err == nil {
		fmt.Sscan(string(b), &off)
	}
	own := []string{"[" + *name + "] ", "[" + *name + "/agent] "}
	deadline := time.Now().Add(*timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(*logPath)
		if err == nil && int64(len(data)) < off {
			off = 0 // log was truncated or replaced
		}
		if err == nil && int64(len(data)) > off {
			// Only consume complete lines.
			end := bytes.LastIndexByte(data, '\n') + 1
			if int64(end) > off {
				var out strings.Builder
				for _, l := range strings.SplitAfter(string(data[off:end]), "\n") {
					// Log lines look like "15:04:05 [name] text".
					_, rest, _ := strings.Cut(l, " ")
					if l != "" && !slices.ContainsFunc(own, func(p string) bool { return strings.HasPrefix(rest, p) }) {
						out.WriteString(l)
					}
				}
				off = int64(end)
				os.WriteFile(cursor, []byte(fmt.Sprint(off)), 0o644)
				if out.Len() > 0 {
					fmt.Print(out.String())
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// advertiseIP prefers a Tailscale address (100.64.0.0/10), then any non-loopback IPv4.
func advertiseIP() string {
	_, tsNet, _ := net.ParseCIDR("100.64.0.0/10")
	fallback := "127.0.0.1"
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok || n.IP.IsLoopback() || n.IP.To4() == nil {
			continue
		}
		if tsNet.Contains(n.IP) {
			return n.IP.String()
		}
		if fallback == "127.0.0.1" {
			fallback = n.IP.String()
		}
	}
	return fallback
}

// pastePrompt is what the host sends; the peer pastes it into their Claude session.
func pastePrompt(link string) string {
	return fmt.Sprintf(`Join a collaborative planning session with me via the collab-plan CLI.
1. Run in the background: collab-plan join %s -name <my name> -fifo /tmp/collab-plan.fifo -log /tmp/collab-plan.log
2. To send my peer a message: echo "text" > /tmp/collab-plan.fifo
3. Read my peer: run in the background: collab-plan next -log /tmp/collab-plan.log -name <my name>
   It blocks until the peer sends something new, prints only the new lines, then exits. When it exits, read the output, reply if needed, and start it again. Keep exactly one running for the whole session.
4. When we agree on something, record it: echo "/decide <one-line decision>" > /tmp/collab-plan.fifo
5. When I say we are done: echo "/quit" > /tmp/collab-plan.fifo. That closes the tunnel and writes the decisions markdown.
Discuss and refine the plan with the peer's agent; keep me updated on what is agreed.`, link)
}

func host(listen, advertise string) (net.Conn, error) {
	b := make([]byte, 8)
	rand.Read(b)
	tok := hex.EncodeToString(b)
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	if advertise == "" {
		advertise = advertiseIP()
	}
	link := fmt.Sprintf("collab-plan://%s/%s", net.JoinHostPort(advertise, port), tok)
	fmt.Printf("Send this to your peer, they paste it into their Claude session:\n\n%s\n\nwaiting for peer...\n", pastePrompt(link))
	for {
		c, err := ln.Accept()
		if err != nil {
			return nil, err
		}
		// Only the first peer that presents the token gets in.
		c.SetReadDeadline(time.Now().Add(10 * time.Second))
		line, err := bufio.NewReader(c).ReadString('\n')
		got := strings.TrimSpace(line)
		if err != nil || subtle.ConstantTimeCompare([]byte(got), []byte(tok)) != 1 {
			c.Close()
			continue
		}
		c.SetReadDeadline(time.Time{})
		io.WriteString(c, "ok\n")
		fmt.Println("peer connected")
		return c, nil
	}
}
