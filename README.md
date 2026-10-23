# collab-plan

TLS tunnel for a shared planning session between any number of people and their agent sessions. Stdlib only.

## Build

    go build -o collab-plan .

## Use

1. Host runs `collab-plan host -fifo /tmp/collab-plan.fifo -log /tmp/collab-plan.log`.
   It prints a link (`collab-plan://HOST:PORT/TOKEN/FINGERPRINT`) and a prompt to send to each peer.
   `HOST` defaults to the Tailscale IP, else the first LAN IP. Override with `-advertise`.
2. Each peer pastes the prompt into their Claude session. Claude runs `collab-plan join <link> -fifo ... -log ...`.
   Any number of peers can join with the same link.
3. Send messages to everyone by writing to the fifo. Read the others with
   `collab-plan next -log FILE -name N`: it blocks until new lines arrive, prints them and exits,
   so an agent can run it as a background task and be woken by it.
4. `/decide <text>` records a decision on every side. `/quit` leaves.
   When the host quits, the listener and all connections close.
5. Each side writes `plan-<timestamp>.md` (override with `-out`) with the participants, the numbered decisions and the full transcript.

Other flags: `-name`, `-listen`, `-agent CMD` (feed chat to a local process and share its stdout).

## Security

The host makes a throwaway self-signed TLS 1.3 certificate per session. The link carries its SHA-256 fingerprint and joiners refuse any other certificate, so no CA is needed.
The link also carries a random token that peers must present. Anyone with the link can join, so share it only with the people you want in the session.
The host relays messages and sets the sender name of each peer, so peers cannot impersonate each other.

## Nix

Run without installing:

    nix run github:wesbragagt/collab-plan -- host

Or add it to a shell: `nix shell github:wesbragagt/collab-plan`. `nix develop` gives a Go dev shell.
