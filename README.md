# collab-plan

Two-person TCP tunnel for a shared planning session between two people and their agent sessions. Stdlib only.

## Build

    go build -o collab-plan .

## Use

1. Host runs `collab-plan host -fifo /tmp/collab-plan.fifo -log /tmp/collab-plan.log`.
   It prints a link (`collab-plan://HOST:PORT/TOKEN`) and a prompt to send to the peer.
   `HOST` defaults to the Tailscale IP, else the first LAN IP. Override with `-advertise`.
2. Peer pastes the prompt into their Claude session. Claude runs `collab-plan join <link> -fifo ... -log ...`.
3. Send messages by writing to the fifo, read the peer from the log.
   `/decide <text>` records a decision on both sides. `/quit` ends the session.
4. When the session ends the tunnel closes and each side writes `plan-<timestamp>.md` (override with `-out`) with the participants and decisions.

Other flags: `-name`, `-listen`, `-agent CMD` (feed peer chat to a local process and share its stdout).

## Security

The tunnel is plain TCP and the token is sent in clear text. Run it over Tailscale or an SSH forward, not the open internet.
