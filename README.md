# mdmailcli — Tiny Outlook/Graph CLI

A minimal command‑line tool to work with Microsoft Graph mailboxes and calendars. It supports device‑code sign‑in, stores config + refresh tokens in the OS keyring, and provides simple subcommands to list folders and messages, inspect your identity, send mail, and work with calendars/events.

- Auth: Microsoft Identity device code flow (public client)
- Storage: OS keyring via `keyring` crate
- HTTP: `reqwest` with `rustls`

## Install / Build

Prerequisites:
- Rust 1.75+ (2021 edition). Install via `rustup`.

Build and run:
- Build: `cargo build` (or `cargo build --release`)
- Help: `cargo run -- --help`
- Example: `cargo run -- whoami`

## Authentication Setup (Azure AD / Entra ID)

This tool uses the device code flow and requires an app registration configured as a public client.

1) Create an app registration
- Portal: Azure portal → Microsoft Entra ID → App registrations → New registration
- Name: any (e.g., `mdmailcli`)
- Supported account types: choose what you need
  - To use the default `tenant = "common"`, pick multi‑tenant + personal accounts ("Accounts in any organizational directory and personal Microsoft accounts").
  - If you choose single‑tenant, set `tenant` to your tenant ID or domain during init.
- Redirect URI: not required for device code flow

2) Mark it as a public client (for device code)
- App → Authentication → Advanced settings → Allow public client flows: set to `Yes`

3) Add Microsoft Graph delegated permissions
- App → API permissions → Add a permission → Microsoft Graph → Delegated permissions
- Add at least:
  - `offline_access`
  - `User.Read`
  - `Mail.Read`
  - `Mail.ReadWrite`
  - `Mail.Send`
  - `Calendars.Read`
  - `Calendars.ReadWrite`
  - `Calendars.Read.Shared`
- Click "Grant admin consent" if your tenant requires it.

4) Collect the following
- Application (client) ID (a GUID)
- Tenant ID or domain (only if you are not using `common`)

## First‑Run and Config

Run interactive init to store config and sign in:

- `cargo run -- init`

Reconfigure or add scopes later:

- Force re-prompt (tenant, client_id, scopes prefilled):
  - `cargo run -- init --force`
- Set scopes non-interactively (keeps tenant/client_id):
  - `cargo run -- init --scopes "offline_access User.Read Mail.Read Mail.ReadWrite Mail.Send Calendars.Read Calendars.ReadWrite Calendars.Read.Shared"`

Prompts:
- `tenant` → default is `common`; set to your tenant ID or domain if single‑tenant
- `client_id` → paste your app registration's Application (client) ID
- `scopes` → default: `offline_access User.Read Mail.Read Mail.ReadWrite Mail.Send Calendars.Read Calendars.ReadWrite Calendars.Read.Shared`

The tool then shows a verification URL and code. Open the URL, enter the code, and complete sign‑in. On success, it validates with `GET /me` and stores a refresh token.

Where it stores data (OS keyring):
- Service: `outlook-graph-cli`
- Accounts: `config` (JSON config), `refresh_token` (refresh token)

## Usage

- Identity: `cargo run -- whoami`
- List folders: `cargo run -- folders-list --top 20`
- List messages: `cargo run -- messages-list --folder "Inbox" --top 10`
- Get a message: `cargo run -- messages-get <message_id>`
- Send mail (text):
  - `cargo run -- send-mail --to user@example.com --subject "Hello" --body "Hi there"`
- Send mail (HTML body):
  - `cargo run -- send-mail --to user@example.com --subject "Hello" --body "<b>Hi</b>" --html`

### Calendars & Events

- List calendars: `cargo run -- calendars-list --top 20`
- List events (primary): `cargo run -- events-list --top 10`
- List events (named): `cargo run -- events-list --calendar "Team Calendar" --top 10`
- List events in a date range (primary):
  - `cargo run -- events-list --start 2025-09-10T00:00:00 --end 2025-09-11T00:00:00 --tz UTC --top 50`
- List events in a date range (named calendar):
  - `cargo run -- events-list --calendar "Team Calendar" --start 2025-09-10T00:00:00 --end 2025-09-15T00:00:00 --tz "Pacific Standard Time" --top 50`
- Create event on primary calendar:
  - `cargo run -- events-create --subject "Sync" --start 2025-09-10T09:00:00 --end 2025-09-10T09:30:00 --tz UTC --attendee alice@example.com --attendee bob@example.com --location "Conf Rm 1"`
- Create event on a named calendar:
  - `cargo run -- events-create --calendar "Team Calendar" --subject "Planning" --start 2025-09-12T13:00:00 --end 2025-09-12T14:00:00 --tz "Pacific Standard Time" --body "Quarterly planning" --html`

#### Free/Busy (Scheduling)

- Get free/busy for users (UTC):
  - `cargo run -- events-busy --start 2025-09-10T09:00:00 --end 2025-09-10T18:00:00 --tz UTC --user alice@contoso.com --user bob@contoso.com`
- Adjust interval granularity:
  - `cargo run -- events-busy --start 2025-09-10T09:00:00 --end 2025-09-10T18:00:00 --tz "Pacific Standard Time" --interval 60 --user team@contoso.com`

### Search

Two modes:
- `$search` full‑text: `cargo run -- messages-search --all --query "from:alice@contoso.com AND subject:invoice"`
- `$filter` structured: `cargo run -- messages-search --unread --since 2024-09-01T00:00:00Z`

Notes about Graph constraints:
- When using `$search`, Graph does not allow `$orderby`. The CLI omits it to avoid `SearchWithOrderBy` errors; results may be relevance‑ordered.
- For `--subject-contains` or `--from`, the CLI prefers `$search` under the hood to avoid `InefficientFilter` errors on large mailboxes. If you only use `--unread`/`--since`, the CLI uses `$filter`.

Common flags:
- `--folder <name>` (default `Inbox`) or `--all` for entire mailbox
- `--top <n>` desired number of results (final limit)
- `--page-size <n>` page size per request (default 50)
- `--max-pages <n>` cap pages fetched (default 10)
- `--sort <date-desc|date-asc>` local sort for `$search` results
- Do not mix `--query` with structured flags (`--from`, `--subject-contains`, `--unread`, `--since`).

Notes:
- If you haven’t run `init`, most commands will prompt you to authenticate.
- Access tokens auto‑refresh using the stored refresh token.
  - For events, primary calendar is used by default. Use `--calendar <name>` to target a specific calendar (case‑insensitive). Common aliases like `primary`, `default`, or `calendar` resolve to the primary calendar.
 - For date‑range listing, both `--start` and `--end` are required and use the `calendarView` endpoint. The `--tz` flag controls the returned time zone (header `Prefer: outlook.timezone="..."`).
 - To add calendar permissions after first run, either re-init with `--force` (interactive) or set scopes directly with `--scopes` and run `init` to trigger consent.
 - To read shared calendars or other users’ schedules, include `Calendars.Read.Shared` in scopes. For minimal scheduling-only access, you can use `Calendars.Read Calendars.Read.Shared` (omit mail scopes).

## Troubleshooting

- 401 Unauthorized / consent errors:
  - Ensure your app has the delegated permissions listed above and (if required) admin consent was granted.
  - Confirm Authentication → "Allow public client flows" is enabled.
  - Re‑run `cargo run -- init` to refresh config and credentials.

- Clear stored credentials/config:
  - Windows: Credential Manager → Windows Credentials → search `outlook-graph-cli` and remove entries
  - macOS: Keychain Access → search `outlook-graph-cli` → delete entries
  - Linux: Secret Service/Libsecret (e.g., Seahorse) → find `outlook-graph-cli` and delete

- Proxy / TLS:
  - `reqwest` honors standard proxy env vars (`HTTP_PROXY`, `HTTPS_PROXY`). TLS via `rustls` (no system OpenSSL required).

## Development

- Lint: `cargo clippy --all-targets --all-features -D warnings`
- Format: `cargo fmt --all`
- Test: `cargo test`

## Safety & Privacy

- Do not commit secrets. App registration IDs are public; refresh tokens are not. The tool only stores tokens in your local OS keyring.

## FAQ

- Can I use personal Microsoft accounts? Yes—register the app to allow personal accounts, and keep `tenant = common`.
- Do I need a redirect URI? Not for device code flow.
- Where are messages sent from? From the signed‑in user (`/me/sendMail`).
