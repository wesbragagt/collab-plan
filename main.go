// collab-plan: a TLS tunnel for a shared planning session. One host relays
// chat, decisions and agent output between any number of joined peers, and
// every side ends up with a markdown file of the decisions and the transcript.
package main

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

type msg struct {
	Kind string `json:"kind"` // hello | chat | agent | decision | sys
	From string `json:"from"`
	Text string `json:"text"`
}

type peer struct {
	conn net.Conn
	mu   sync.Mutex // serializes writes to enc
	enc  *json.Encoder
	name string
}

func newPeer(c net.Conn, name string) *peer {
	return &peer{conn: c, enc: json.NewEncoder(c), name: name}
}

func (p *peer) send(m msg) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return p.enc.Encode(m)
}

type entry struct {
	at   time.Time
	line string
}

type session struct {
	name string
	hub  bool // host relays messages between peers; a joiner has the host as its only peer
	ln   net.Listener

	mu         sync.Mutex // guards everything below, plus out/log writes
	peers      map[*peer]bool
	people     []string
	decisions  []msg
	transcript []entry
	log        io.Writer
	out        io.Writer

	agentMu sync.Mutex
	agent   io.WriteCloser // nil when no local agent

	start time.Time
	once  sync.Once
	done  chan struct{}
}

func (s *session) end() {
	s.once.Do(func() {
		close(s.done)
		if s.ln != nil {
			s.ln.Close()
		}
		s.mu.Lock()
		for p := range s.peers {
			p.conn.Close()
		}
		s.mu.Unlock()
	})
}

func (s *session) ended() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *session) addPerson(n string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.people, n) {
		s.people = append(s.people, n)
	}
}

func (s *session) broadcast(m msg, except *peer) {
	s.mu.Lock()
	ps := make([]*peer, 0, len(s.peers))
	for p := range s.peers {
		if p != except {
			ps = append(ps, p)
		}
	}
	s.mu.Unlock()
	for _, p := range ps {
		if p.send(m) != nil {
			p.conn.Close()
		}
	}
}

func (s *session) show(m msg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tag := m.From
	switch m.Kind {
	case "agent":
		tag += "/agent"
	case "decision":
		s.decisions = append(s.decisions, m)
		tag += "/decision"
	case "sys":
		tag = "*"
	}
	if m.Kind != "sys" && !slices.Contains(s.people, m.From) {
		s.people = append(s.people, m.From)
	}
	line := fmt.Sprintf("[%s] %s", tag, m.Text)
	now := time.Now()
	s.transcript = append(s.transcript, entry{now, line})
	fmt.Fprintln(s.out, line)
	if s.log != nil {
		fmt.Fprintf(s.log, "%s %s\n", now.Format("15:04:05"), line)
	}
}

func (s *session) toAgent(text string) {
	s.agentMu.Lock()
	defer s.agentMu.Unlock()
	if s.agent != nil {
		io.WriteString(s.agent, text+"\n")
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `usage:
  collab-plan host [-listen :7777] [-advertise HOST] [-name N] [-agent CMD] [-fifo PIPE] [-log FILE] [-out FILE]
  collab-plan join collab-plan://HOST:PORT/TOKEN/FINGERPRINT [-name N] [-agent CMD] [-fifo PIPE] [-log FILE] [-out FILE]
  collab-plan next -log FILE -name N [-timeout 10m]   (block until new peer lines)

host prints a link and a paste-ready prompt. Any number of peers can paste it
into their Claude session and join. The connection is TLS; the link carries the
host certificate fingerprint, which joiners pin.
Lines typed on stdin or written to -fifo go to everyone. "/decide TEXT" records
a decision, "/quit" leaves. When the host quits the tunnel closes for everyone.
Each side then writes a markdown file with the decisions and the transcript.
With -agent CMD, chat is fed to CMD and CMD's stdout is shared.
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
	out := fs.String("out", "", "summary markdown path (default plan-<timestamp>.md)")

	var link string
	args := os.Args[2:]
	switch cmd {
	case "host":
	case "join":
		if len(args) < 1 {
			usage()
		}
		link, args = args[0], args[1:]
	default:
		usage()
	}
	fs.Parse(args)

	s := &session{
		name:   *name,
		hub:    cmd == "host",
		peers:  map[*peer]bool{},
		people: []string{*name},
		out:    os.Stdout,
		start:  time.Now(),
		done:   make(chan struct{}),
	}
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if *logFile != "" {
		f, err := os.OpenFile(*logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			fail(err)
		}
		defer f.Close()
		s.log = f
	}
	if *agentCmd != "" {
		if err := s.startAgent(*agentCmd); err != nil {
			fail(fmt.Errorf("agent: %w", err))
		}
	}

	// Start the network side last so no message arrives before the agent and log are ready.
	var run func()
	if s.hub {
		cfg, fp, tok, err := serverTLS()
		if err != nil {
			fail(err)
		}
		ln, err := tls.Listen("tcp", *listen, cfg)
		if err != nil {
			fail(err)
		}
		s.ln = ln
		_, port, _ := net.SplitHostPort(ln.Addr().String())
		if *advertise == "" {
			*advertise = advertiseIP()
		}
		l := fmt.Sprintf("collab-plan://%s/%s/%s", net.JoinHostPort(*advertise, port), tok, fp)
		fmt.Printf("Send this to each peer, they paste it into their Claude session:\n\n%s\n\nwaiting for peers...\n", pastePrompt(l))
		run = func() { s.serve(ln, tok) }
	} else {
		addr, tok, fp, err := parseLink(link)
		if err != nil {
			fail(err)
		}
		p, dec, err := dial(addr, tok, fp, *name)
		if err != nil {
			fail(err)
		}
		s.peers[p] = true
		run = func() { s.readPeer(p, dec); s.end() }
	}
	go run()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() { <-sig; s.end() }()

	go s.readInput(os.Stdin, *fifo == "")
	if *fifo != "" {
		if err := s.watchFifo(*fifo); err != nil {
			fail(fmt.Errorf("fifo: %w", err))
		}
		defer os.Remove(*fifo)
	}
	<-s.done
	fmt.Println("session ended, tunnel closed")
	path, err := s.writeSummary(*out)
	if err != nil {
		fail(fmt.Errorf("summary: %w", err))
	}
	fmt.Println("summary written to", path)
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
	b.WriteString("\n## Transcript\n\n")
	for _, e := range s.transcript {
		fmt.Fprintf(&b, "- `%s` %s\n", e.at.Format("15:04:05"), e.line)
	}
	return path, os.WriteFile(path, []byte(b.String()), 0o644)
}

// serverTLS makes a throwaway self-signed cert and a random join token.
// Joiners pin the cert by SHA-256 fingerprint instead of using a CA.
func serverTLS() (*tls.Config, string, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", "", err
	}
	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "collab-plan"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, "", "", err
	}
	sum := sha256.Sum256(der)
	tb := make([]byte, 8)
	rand.Read(tb)
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
	}
	return cfg, hex.EncodeToString(sum[:]), hex.EncodeToString(tb), nil
}

func (s *session) serve(ln net.Listener, tok string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go s.admit(c, tok)
	}
}

func (s *session) admit(c net.Conn, tok string) {
	c.SetDeadline(time.Now().Add(10 * time.Second)) // also bounds the TLS handshake
	dec := json.NewDecoder(c)
	var h msg
	if dec.Decode(&h) != nil || h.Kind != "hello" || strings.TrimSpace(h.From) == "" ||
		subtle.ConstantTimeCompare([]byte(h.Text), []byte(tok)) != 1 {
		c.Close()
		return
	}
	c.SetDeadline(time.Time{})
	p := newPeer(c, h.From)
	if p.send(msg{Kind: "hello", Text: "ok"}) != nil {
		c.Close()
		return
	}
	s.mu.Lock()
	if s.ended() {
		s.mu.Unlock()
		c.Close()
		return
	}
	s.peers[p] = true
	s.mu.Unlock()
	s.addPerson(p.name)
	s.announce(p.name + " joined")

	s.readPeer(p, dec)

	s.mu.Lock()
	delete(s.peers, p)
	s.mu.Unlock()
	c.Close()
	if !s.ended() {
		s.announce(p.name + " left")
	}
}

func (s *session) announce(text string) {
	m := msg{Kind: "sys", Text: text}
	s.show(m)
	s.broadcast(m, nil)
}

func dial(addr, tok, fp, name string) (*peer, *json.Decoder, error) {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS13,
		// The cert is self-signed; trust is the fingerprint from the link.
		InsecureSkipVerify: true,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error {
			if len(raw) == 0 {
				return errors.New("host sent no certificate")
			}
			sum := sha256.Sum256(raw[0])
			if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(sum[:])), []byte(fp)) != 1 {
				return errors.New("host certificate does not match the link fingerprint")
			}
			return nil
		},
	}
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, cfg)
	if err != nil {
		return nil, nil, err
	}
	c.SetDeadline(time.Now().Add(10 * time.Second))
	p := newPeer(c, "host")
	if err := p.send(msg{Kind: "hello", From: name, Text: tok}); err != nil {
		c.Close()
		return nil, nil, err
	}
	dec := json.NewDecoder(c)
	var r msg
	if dec.Decode(&r) != nil || r.Kind != "hello" || r.Text != "ok" {
		c.Close()
		return nil, nil, errors.New("handshake rejected (bad token?)")
	}
	c.SetDeadline(time.Time{})
	return p, dec, nil
}

func parseLink(link string) (addr, tok, fp string, err error) {
	bad := fmt.Errorf("invalid link %q, want collab-plan://HOST:PORT/TOKEN/FINGERPRINT", link)
	u, perr := url.Parse(strings.TrimSpace(link))
	if perr != nil || u.Scheme != "collab-plan" || u.Host == "" {
		return "", "", "", bad
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || len(parts[1]) != 64 {
		return "", "", "", bad
	}
	return u.Host, parts[0], parts[1], nil
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
			s.broadcast(m, nil)
		}
	}()
	return nil
}

// readPeer handles messages from one peer until its connection closes.
func (s *session) readPeer(p *peer, dec *json.Decoder) {
	for {
		var m msg
		if dec.Decode(&m) != nil {
			return
		}
		// Peers cannot inject system lines or spoof another sender.
		if m.Kind == "hello" || (s.hub && m.Kind == "sys") {
			continue
		}
		if s.hub {
			m.From = p.name
		}
		s.show(m)
		if s.hub {
			s.broadcast(m, p)
		}
		// Agents only see humans' chat, not other agents' output, to avoid loops.
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
			s.end()
			return
		}
		kind := "chat"
		if d, ok := strings.CutPrefix(t, "/decide "); ok {
			kind, t = "decision", strings.TrimSpace(d)
			if t == "" {
				continue
			}
		}
		m := msg{Kind: kind, From: s.name, Text: t}
		s.show(m)
		if kind == "chat" {
			s.toAgent(t)
		}
		s.broadcast(m, nil)
	}
	if closeOnEOF {
		s.end()
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

// runNext blocks until the log has new lines from others, prints them, and
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

// pastePrompt is what the host sends; each peer pastes it into their Claude session.
func pastePrompt(link string) string {
	return fmt.Sprintf(`Join a collaborative planning session with me via the collab-plan CLI.
1. Run in the background: collab-plan join %s -name <my name> -fifo /tmp/collab-plan.fifo -log /tmp/collab-plan.log
2. To send everyone a message: echo "text" > /tmp/collab-plan.fifo
3. Read the others: run in the background: collab-plan next -log /tmp/collab-plan.log -name <my name>
   It blocks until someone sends something new, prints only the new lines, then exits. When it exits, read the output, reply if needed, and start it again. Keep exactly one running for the whole session.
4. When we agree on something, record it: echo "/decide <one-line decision>" > /tmp/collab-plan.fifo
5. When I say we are done: echo "/quit" > /tmp/collab-plan.fifo. That leaves the session and writes the markdown summary.
Discuss and refine the plan with the other agents; keep me updated on what is agreed.`, link)
}
