use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use keyring::Entry;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::io::{self, Write};

// ---------- Constants ----------
const SERVICE_NAME: &str = "outlook-graph-cli"; // Windows Credential Manager service name
const CONFIG_ACCOUNT: &str = "config"; // key for config JSON
const RT_ACCOUNT: &str = "refresh_token"; // key for refresh token

const GRAPH_RESOURCE: &str = "https://graph.microsoft.com";

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AppConfig {
    tenant: String,    // e.g. "common", "organizations", "consumers", or your tenant GUID
    client_id: String, // your app registration's Application (client) ID
    scopes: Vec<String>, // e.g. ["offline_access","User.Read","Mail.Read","Mail.ReadWrite","Mail.Send"]
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            tenant: "common".to_string(),
            client_id: String::new(),
            scopes: vec![
                "offline_access".to_string(),
                "User.Read".to_string(),
                "Mail.Read".to_string(),
                "Mail.ReadWrite".to_string(),
                "Mail.Send".to_string(),
                // Calendar scopes for new calendar/event features
                "Calendars.Read".to_string(),
                "Calendars.ReadWrite".to_string(),
            ],
        }
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "Tiny Outlook/Graph CLI (Rust)",
    long_about = "mdmailcli is a small CLI for Microsoft Graph mail and calendars.\n\nAuthentication:\n- Uses device code flow with a public client app registration\n- Stores config and refresh token in your OS keyring\n  - Service: outlook-graph-cli\n  - Accounts: config (JSON), refresh_token\n\nQuick start:\n- Run `init` to enter tenant, client_id, and scopes, then sign in\n- Use `whoami`, `folders list`, `messages list`, `messages search`, `send`,\n  or calendar commands like `calendars list`, `events list`, `events create`.\n\nTip: This tool is often called by other apps (e.g., via MCP servers). All configuration is discoverable via this help and `init` prompts."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize auth (and store config) — prompts if missing/invalid
    Init,

    /// Show the signed-in user’s identity
    Whoami,

    /// Mail folder operations
    Folders {
        #[command(subcommand)]
        cmd: FolderCmd,
    },

    /// Message operations
    Messages {
        #[command(subcommand)]
        cmd: MessageCmd,
    },

    /// Send a simple email
    Send {
        /// One or more recipients (email addresses)
        #[arg(required = true)]
        to: Vec<String>,
        /// Subject
        #[arg(long)]
        subject: String,
        /// Body text (use --html for HTML)
        #[arg(long)]
        body: String,
        /// Treat body as HTML
        #[arg(long, default_value_t = false)]
        html: bool,
    },

    /// Calendar container operations
    Calendars {
        #[command(subcommand)]
        cmd: CalendarCmd,
    },

    /// Calendar event operations
    Events {
        #[command(subcommand)]
        cmd: EventCmd,
    },
}

#[derive(Subcommand)]
enum FolderCmd {
    /// List top mail folders
    List {
        /// Number of folders to return
        #[arg(long, default_value_t = 20)]
        top: u32,
    },
}

#[derive(Subcommand)]
enum MessageCmd {
    /// List messages in a folder (by name), newest first
    List {
        /// Folder display name, e.g. Inbox, Sent Items, Archive
        #[arg(long, default_value = "Inbox")]
        folder: String,
        /// Number of messages
        #[arg(long, default_value_t = 10)]
        top: u32,
    },
    /// Get a single message by id (prints metadata + preview)
    Get { id: String },
    /// Search messages using Graph $search or $filter
    #[command(
        about = "Search messages via $search (full-text) or $filter (structured)",
        long_about = "Search your mailbox.\n\nTwo modes:\n- $search mode with --query uses Graph full-text (subject, body, from, etc.). Requires header ConsistencyLevel=eventual.\n- $filter mode builds a structured filter from flags like --from, --subject-contains, --unread, --since. Uses ConsistencyLevel=eventual when using contains().\n\nExamples:\n- messages search --query \"from:alice@contoso.com AND subject:invoice\"\n- messages search --subject-contains invoice --unread --since 2024-09-01T00:00:00Z\n- messages search --folder Sent --from bob@contoso.com\n\nNote: Do not mix --query with the structured flags."
    )]
    Search {
        /// Folder display name (omit with --all to search entire mailbox)
        #[arg(long, default_value = "Inbox")]
        folder: String,
        /// Search entire mailbox instead of a single folder
        #[arg(long, default_value_t = false)]
        all: bool,
        /// Full-text query for $search (e.g., "from:alice AND subject:quarterly")
        #[arg(long)]
        query: Option<String>,
        /// Structured: filter by sender email (switches to $filter mode if used without --query)
        #[arg(long, value_name = "EMAIL")]
        from: Option<String>,
        /// Structured: subject contains text (uses contains(subject, ...))
        #[arg(long, value_name = "TEXT")]
        subject_contains: Option<String>,
        /// Structured: only unread messages
        #[arg(long, default_value_t = false)]
        unread: bool,
        /// Structured: received on/after this ISO8601 timestamp (UTC)
        #[arg(long, value_name = "ISO8601")]
        since: Option<String>,
        /// Number of results to return (final limit)
        #[arg(long, default_value_t = 25)]
        top: u32,
        /// Page size for Graph requests (when paging)
        #[arg(long, default_value_t = 50)]
        page_size: u32,
        /// Max pages to fetch (protects from excessive calls)
        #[arg(long, default_value_t = 10)]
        max_pages: u32,
        /// Local sort for results
        #[arg(long, value_enum, default_value_t = Sort::DateDesc)]
        sort: Sort,
    },
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Sort {
    /// receivedDateTime desc
    DateDesc,
    /// receivedDateTime asc
    DateAsc,
}

#[derive(Subcommand)]
enum CalendarCmd {
    /// List calendars (id and name)
    List {
        /// Number of calendars to return
        #[arg(long, default_value_t = 20)]
        top: u32,
    },
}

#[derive(Subcommand)]
enum EventCmd {
    /// List events from a calendar (primary by default), soonest first
    List {
        /// Calendar display name (omit to use primary)
        #[arg(long)]
        calendar: Option<String>,
        /// Start of date range (ISO8601). When set, --end is required.
        #[arg(long, value_name = "ISO8601")]
        start: Option<String>,
        /// End of date range (ISO8601). When set, --start is required.
        #[arg(long, value_name = "ISO8601")]
        end: Option<String>,
        /// Time zone for returned start/end (e.g., UTC, Pacific Standard Time)
        #[arg(long, default_value = "UTC")]
        tz: String,
        /// Number of events
        #[arg(long, default_value_t = 10)]
        top: u32,
    },
    /// Create a calendar event (on primary or named calendar)
    Create {
        /// Subject for the event
        #[arg(long)]
        subject: String,
        /// Start time (ISO8601, e.g., 2025-09-10T09:00:00)
        #[arg(long, value_name = "ISO8601")]
        start: String,
        /// End time (ISO8601, e.g., 2025-09-10T10:00:00)
        #[arg(long, value_name = "ISO8601")]
        end: String,
        /// Time zone for start/end (e.g., UTC, Pacific Standard Time)
        #[arg(long, default_value = "UTC")]
        tz: String,
        /// Optional plain or HTML body
        #[arg(long)]
        body: Option<String>,
        /// Treat body as HTML
        #[arg(long, default_value_t = false)]
        html: bool,
        /// One or more attendee email addresses
        #[arg(long = "attendee")] 
        attendees: Vec<String>,
        /// Optional location display name
        #[arg(long)]
        location: Option<String>,
        /// Target calendar by display name (omit to use primary)
        #[arg(long)]
        calendar: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            ensure_login(true).await?; // force interactive if needed
            println!("✅ Initialized and signed in.");
        }
        Commands::Calendars { cmd } => match cmd {
            CalendarCmd::List { top } => {
                let token = ensure_login(false).await?;
                let url = format!(
                    "/v1.0/me/calendars?$select=id,name,canEdit,canShare,owner&$top={}",
                    top
                );
                let json = graph_get_json(&token, &url).await?;
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
        },
        Commands::Events { cmd } => match cmd {
            EventCmd::List { calendar, start, end, tz, top } => {
                let token = ensure_login(false).await?;
                // Build headers (timezone preference)
                let prefer = format!("outlook.timezone=\"{}\"", tz);
                let headers: Vec<(&str, &str)> = vec![("Prefer", prefer.as_str())];

                // Decide endpoint: calendarView when both start+end provided, else events
                if (start.is_some() && end.is_none()) || (start.is_none() && end.is_some()) {
                    return Err(anyhow!("--start and --end must be provided together for date-range listing"));
                }

                let base = if let Some(name) = calendar {
                    let cal_id = resolve_calendar_id(&token, &name).await?;
                    (format!("/v1.0/me/calendars/{}", cal_id), true)
                } else {
                    ("/v1.0/me/calendar".to_string(), false)
                };

                let mut qp: Vec<(&str, String)> = Vec::new();
                qp.push(("$select", "subject,organizer,start,end,location,webLink,isAllDay".to_string()));
                qp.push(("$orderby", "start/dateTime asc".to_string()));
                qp.push(("$top", top.to_string()));

                let path = if start.is_some() && end.is_some() {
                    // calendarView requires both startDateTime and endDateTime
                    qp.push(("startDateTime", start.clone().unwrap()));
                    qp.push(("endDateTime", end.clone().unwrap()));
                    format!("{}/calendarView", base.0)
                } else {
                    format!("{}/events", base.0)
                };

                let json = graph_get_json_with_headers_and_query(&token, &path, &headers, &qp).await?;
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
            EventCmd::Create {
                subject,
                start,
                end,
                tz,
                body,
                html,
                attendees,
                location,
                calendar,
            } => {
                let token = ensure_login(false).await?;
                let payload = build_event_payload(
                    &subject,
                    &start,
                    &end,
                    &tz,
                    body.as_deref(),
                    html,
                    &attendees,
                    location.as_deref(),
                );
                let url = if let Some(name) = calendar {
                    let cal_id = resolve_calendar_id(&token, &name).await?;
                    format!("{}/v1.0/me/calendars/{}/events", GRAPH_RESOURCE, cal_id)
                } else {
                    format!("{}/v1.0/me/events", GRAPH_RESOURCE)
                };

                let res = reqwest::Client::new()
                    .post(&url)
                    .bearer_auth(&token)
                    .json(&payload)
                    .send()
                    .await
                    .context("POST event failed")?;

                if res.status().is_success() || res.status() == StatusCode::CREATED {
                    let created = res.json::<serde_json::Value>().await.unwrap_or_else(|_| serde_json::json!({"status":"created"}));
                    println!("{}", serde_json::to_string_pretty(&created)?);
                } else {
                    let status = res.status();
                    let text = res.text().await.unwrap_or_default();
                    return Err(anyhow!("create event error: {} — {}", status, text));
                }
            }
        },
        Commands::Whoami => {
            let token = ensure_login(false).await?;
            let me = graph_get_json(&token, "/v1.0/me").await?;
            println!("{}", serde_json::to_string_pretty(&me)?);
        }
        Commands::Folders { cmd } => match cmd {
            FolderCmd::List { top } => {
                let token = ensure_login(false).await?;
                let url = format!("/v1.0/me/mailFolders?$select=displayName,id,childFolderCount,unreadItemCount,totalItemCount&$top={}", top);
                let json = graph_get_json(&token, &url).await?;
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
        },
        Commands::Messages { cmd } => match cmd {
            MessageCmd::List { folder, top } => {
                let token = ensure_login(false).await?;
                let folder_id = resolve_folder_id(&token, &folder).await?;
                let url = format!(
                    "/v1.0/me/mailFolders/{}/messages?$select=subject,from,receivedDateTime,isRead,webLink&$orderby=receivedDateTime desc&$top={}",
                    folder_id, top
                );
                let json = graph_get_json(&token, &url).await?;
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
            MessageCmd::Get { id } => {
                let token = ensure_login(false).await?;
                let url = format!("/v1.0/me/messages/{}?$select=subject,from,receivedDateTime,bodyPreview,webLink", id);
                let json = graph_get_json(&token, &url).await?;
                println!("{}", serde_json::to_string_pretty(&json)?);
            }
            MessageCmd::Search {
                folder,
                all,
                query,
                from,
                subject_contains,
                unread,
                since,
                top,
                page_size,
                max_pages,
                sort,
            } => {
                let token = ensure_login(false).await?;
                // Prevent mixing raw --query with structured flags
                let using_structured =
                    from.is_some() || subject_contains.is_some() || unread || since.is_some();
                if query.is_some() && using_structured {
                    return Err(anyhow!(
                        "Do not combine --query with structured flags (use one mode)"
                    ));
                }

                // Determine path (all mailbox vs specific folder)
                let base_path: String = if all {
                    "/v1.0/me/messages".to_string()
                } else {
                    let folder_id = resolve_folder_id(&token, &folder).await?;
                    format!("/v1.0/me/mailFolders/{}/messages", folder_id)
                };

                // Build query params
                let mut qp: Vec<(&str, String)> = Vec::new();
                qp.push((
                    "$select",
                    "subject,from,receivedDateTime,isRead,webLink".to_string(),
                ));
                // Use page_size for server page size when paging. We'll trim to `top` after sorting/filtering.
                qp.push(("$top", page_size.to_string()));

                let mut headers: Vec<(&str, &str)> = Vec::new();

                // Decide mode: explicit $search, or implicit $search if subject_contains/from used, otherwise $filter
                let mut use_search = query.is_some();
                let mut search_terms: Vec<String> = Vec::new();
                if query.is_none() {
                    if let Some(sub) = &subject_contains {
                        use_search = true; // prefer $search for subject contains to avoid InefficientFilter
                        search_terms.push(format!("subject:{}", sub));
                    }
                    if let Some(f) = &from {
                        use_search = true; // prefer $search for sender
                        search_terms.push(format!("from:{}", f));
                    }
                }

                if use_search {
                    headers.push(("ConsistencyLevel", "eventual"));
                    qp.push(("$count", "true".to_string()));
                    let q = if let Some(q) = query {
                        q
                    } else {
                        search_terms.join(" AND ")
                    };
                    if !q.trim().is_empty() {
                        qp.push(("$search", format!("\"{}\"", q)));
                    }
                    // IMPORTANT: Do not add $orderby with $search (Graph returns SearchWithOrderBy).
                    // Page through results and apply client-side filters + sorting.
                    let mut items: Vec<serde_json::Value> = Vec::new();
                    let mut page_count = 0u32;
                    // First request
                    let mut page =
                        graph_get_json_with_headers_and_query(&token, &base_path, &headers, &qp)
                            .await?;
                    page_count += 1;
                    collect_items(&mut items, &mut page);
                    let mut next_link = page
                        .get("@odata.nextLink")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    while items.len() < (top as usize) && page_count < max_pages {
                        if let Some(link) = next_link.clone() {
                            let mut next =
                                graph_get_json_absolute_with_headers(&token, &link, &headers)
                                    .await?;
                            page_count += 1;
                            collect_items(&mut items, &mut next);
                            next_link = next
                                .get("@odata.nextLink")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string());
                            if next_link.is_none() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    // Apply client filters
                    let items = apply_client_filters_vec(items, unread, since.as_deref())?;
                    // Local sort
                    let mut items = items;
                    sort_items_by_received(&mut items, matches!(sort, Sort::DateAsc));
                    // Trim to top
                    let items: Vec<_> = items.into_iter().take(top as usize).collect();
                    let out = serde_json::json!({"value": items});
                    println!("{}", serde_json::to_string_pretty(&out)?);
                } else {
                    // $filter mode (unread/since/from only). Safe to use $orderby.
                    qp.push(("$orderby", "receivedDateTime desc".to_string()));
                    let mut filters: Vec<String> = Vec::new();
                    if unread {
                        filters.push("isRead eq false".to_string());
                    }
                    if let Some(f) = from {
                        filters.push(format!(
                            "from/emailAddress/address eq '{}'",
                            escape_single_quotes(&f)
                        ));
                    }
                    if let Some(s) = since {
                        filters.push(format!("receivedDateTime ge {}", s));
                    }
                    if !filters.is_empty() {
                        qp.push(("$filter", filters.join(" and ")));
                    }
                    let json =
                        graph_get_json_with_headers_and_query(&token, &base_path, &headers, &qp)
                            .await?;
                    println!("{}", serde_json::to_string_pretty(&json)?);
                }
            }
        },
        Commands::Send {
            to,
            subject,
            body,
            html,
        } => {
            let token = ensure_login(false).await?;
            let payload = build_send_payload(&to, &subject, &body, html);
            let url = format!("{}/v1.0/me/sendMail", GRAPH_RESOURCE);
            let res = reqwest::Client::new()
                .post(&url)
                .bearer_auth(&token)
                .json(&payload)
                .send()
                .await
                .context("POST /me/sendMail failed")?;

            if res.status().is_success() || res.status() == StatusCode::ACCEPTED {
                println!("✉️  Sent.");
            } else {
                let status = res.status();
                let text = res.text().await.unwrap_or_default();
                return Err(anyhow!("sendMail error: {} — {}", status, text));
            }
        }
    }

    Ok(())
}

// ---------- Auth & Storage ----------

async fn ensure_login(force_interactive: bool) -> Result<String> {
    // 1) Load config
    let mut cfg = load_config()?;

    // 2) Try refresh token if present and not forcing interactive
    if !force_interactive {
        if let Ok(rt) = get_refresh_token() {
            if let Ok(at) = refresh_access_token(&cfg, &rt).await {
                return Ok(at);
            }
        }
    }

    // 3) If config missing values or refresh failed, prompt and do device code
    if cfg.client_id.trim().is_empty() || cfg.tenant.trim().is_empty() {
        cfg = prompt_for_config(cfg)?;
        save_config(&cfg)?;
    }

    let (access_token, new_rt) = device_code_login(&cfg).await?;
    save_refresh_token(&new_rt)?;

    // 4) Validate by calling /me
    let me = graph_get_json(&access_token, "/v1.0/me").await?;
    let upn = me
        .get("userPrincipalName")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    println!("Signed in as {}", upn);

    Ok(access_token)
}

fn load_config() -> Result<AppConfig> {
    let entry = Entry::new(SERVICE_NAME, CONFIG_ACCOUNT)?;
    match entry.get_password() {
        Ok(json) => {
            let cfg: AppConfig = serde_json::from_str(&json).context("parse config JSON")?;
            Ok(cfg)
        }
        Err(_) => Ok(AppConfig::default()),
    }
}

fn save_config(cfg: &AppConfig) -> Result<()> {
    let json = serde_json::to_string(cfg)?;
    let entry = Entry::new(SERVICE_NAME, CONFIG_ACCOUNT)?;
    entry.set_password(&json)?;
    Ok(())
}

fn get_refresh_token() -> Result<String> {
    let entry = Entry::new(SERVICE_NAME, RT_ACCOUNT)?;
    Ok(entry.get_password()?)
}

fn save_refresh_token(rt: &str) -> Result<()> {
    let entry = Entry::new(SERVICE_NAME, RT_ACCOUNT)?;
    entry.set_password(rt)?;
    Ok(())
}

fn prompt_for_config(mut cfg: AppConfig) -> Result<AppConfig> {
    println!("Let's configure Microsoft Graph auth. Press Enter to accept the suggested default.");

    // Tenant
    println!(
        "Tenant (examples: common | organizations | consumers | your-tenant-guid). Default: {}",
        cfg.tenant
    );
    print!("tenant> ");
    io::stdout().flush().ok();
    let mut s = String::new();
    io::stdin().read_line(&mut s).ok();
    let s = s.trim();
    if !s.is_empty() {
        cfg.tenant = s.to_string();
    }

    // Client ID
    println!("Application (client) ID from Entra app registration (e.g. 11111111-2222-3333-4444-555555555555)");
    print!("client_id> ");
    io::stdout().flush().ok();
    let mut c = String::new();
    io::stdin().read_line(&mut c).ok();
    let c = c.trim();
    if c.is_empty() {
        return Err(anyhow!("client_id is required"));
    }
    cfg.client_id = c.to_string();

    // Scopes
    println!("Scopes (space-separated). Typical: offline_access User.Read Mail.Read Mail.ReadWrite Mail.Send Calendars.Read Calendars.ReadWrite");
    println!("Default: {}", cfg.scopes.join(" "));
    print!("scopes> ");
    io::stdout().flush().ok();
    let mut sc = String::new();
    io::stdin().read_line(&mut sc).ok();
    let sc = sc.trim();
    if !sc.is_empty() {
        cfg.scopes = sc.split_whitespace().map(|s| s.to_string()).collect();
    }

    Ok(cfg)
}

async fn device_code_login(cfg: &AppConfig) -> Result<(String, String)> {
    let device_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/devicecode",
        cfg.tenant
    );

    let scope = cfg.scopes.join(" ");

    #[derive(Deserialize)]
    struct DeviceResp {
        device_code: String,
        user_code: String,
        verification_uri: String,
        #[serde(default)]
        verification_uri_complete: Option<String>,
        expires_in: i64,
        interval: Option<i64>,
        message: Option<String>,
    }

    let client = reqwest::Client::new();

    let form = [
        ("client_id", cfg.client_id.as_str()),
        ("scope", scope.as_str()),
    ];

    let dev: DeviceResp = client
        .post(&device_url)
        .form(&form)
        .send()
        .await
        .context("device code request failed")?
        .error_for_status()
        .context("device code HTTP error")?
        .json()
        .await
        .context("parse device code JSON")?;

    println!("\n== Device sign-in ==");
    if let Some(msg) = &dev.message {
        println!("{}", msg);
    } else {
        println!(
            "Open {} and enter code {}",
            dev.verification_uri, dev.user_code
        );
        if let Some(complete) = &dev.verification_uri_complete {
            println!("(Direct link) {}", complete);
        }
    }

    // Poll token endpoint
    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        cfg.tenant
    );

    let mut interval = dev.interval.unwrap_or(5);
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs((dev.expires_in as u64).saturating_sub(5));

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct TokenOk {
        access_token: String,
        expires_in: i64,
        token_type: String,
        scope: Option<String>,
        refresh_token: Option<String>,
    }

    #[derive(Deserialize)]
    struct TokenErr {
        error: String,
        error_description: Option<String>,
    }

    loop {
        if std::time::Instant::now() > deadline {
            return Err(anyhow!("device code expired; run `init` again"));
        }

        let resp = client
            .post(&token_url)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", cfg.client_id.as_str()),
                ("device_code", dev.device_code.as_str()),
            ])
            .send()
            .await
            .context("token poll failed")?;

        if resp.status().is_success() {
            let ok: TokenOk = resp.json().await.context("parse token JSON")?;
            let rt = ok
                .refresh_token
                .ok_or_else(|| anyhow!("no refresh_token in response"))?;
            return Ok((ok.access_token, rt));
        } else {
            let te: TokenErr = resp.json().await.unwrap_or(TokenErr {
                error: "unknown_error".into(),
                error_description: None,
            });
            match te.error.as_str() {
                "authorization_pending" => {
                    tokio::time::sleep(std::time::Duration::from_secs(interval as u64)).await;
                    continue;
                }
                "slow_down" => {
                    interval += 2;
                    tokio::time::sleep(std::time::Duration::from_secs(interval as u64)).await;
                    continue;
                }
                other => {
                    return Err(anyhow!(
                        "device code error: {} — {:?}",
                        other,
                        te.error_description
                    ));
                }
            }
        }
    }
}

async fn refresh_access_token(cfg: &AppConfig, refresh_token: &str) -> Result<String> {
    let token_url = format!(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token",
        cfg.tenant
    );
    let scope = cfg.scopes.join(" ");

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct TokenOk {
        access_token: String,
        expires_in: i64,
        token_type: String,
        scope: Option<String>,
        refresh_token: Option<String>,
    }

    let client = reqwest::Client::new();
    let res = client
        .post(&token_url)
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", cfg.client_id.as_str()),
            ("refresh_token", refresh_token),
            ("scope", scope.as_str()), // optional, but keeps scope consistent
        ])
        .send()
        .await
        .context("refresh token request failed")?;

    if !res.status().is_success() {
        let status = res.status();
        let txt = res.text().await.unwrap_or_default();
        return Err(anyhow!("refresh failed: {} — {}", status, txt));
    }

    let tok: TokenOk = res.json().await.context("parse refresh token JSON")?;
    if let Some(rt) = &tok.refresh_token {
        // Microsoft may rotate refresh tokens — always store the latest
        save_refresh_token(rt)?;
    }

    Ok(tok.access_token)
}

// ---------- Graph helpers ----------

async fn graph_get_json(access_token: &str, path_and_query: &str) -> Result<serde_json::Value> {
    let url = format!("{}{}", GRAPH_RESOURCE, path_and_query);
    let res = reqwest::Client::new()
        .get(&url)
        .bearer_auth(access_token)
        .send()
        .await
        .with_context(|| format!("GET {} failed", path_and_query))?;

    if res.status().is_success() {
        Ok(res.json().await.context("parse JSON")?)
    } else if res.status() == StatusCode::UNAUTHORIZED {
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("401 Unauthorized: {}", text))
    } else {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("Graph error {}: {}", status, text))
    }
}

async fn graph_get_json_with_headers_and_query(
    access_token: &str,
    path: &str,
    headers: &[(&str, &str)],
    query: &[(&str, String)],
) -> Result<serde_json::Value> {
    let url = format!("{}{}", GRAPH_RESOURCE, path);
    let client = reqwest::Client::new();
    let mut req = client.get(&url).bearer_auth(access_token);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    // Convert query slice into tuples of (&str, &str) by mapping to owned pairs then passing as refs
    let query_owned: Vec<(&str, String)> = query.iter().map(|(k, v)| (*k, v.clone())).collect();
    let res = req
        .query(&query_owned)
        .send()
        .await
        .with_context(|| format!("GET {} with query failed", path))?;

    if res.status().is_success() {
        Ok(res.json().await.context("parse JSON")?)
    } else if res.status() == StatusCode::UNAUTHORIZED {
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("401 Unauthorized: {}", text))
    } else {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("Graph error {}: {}", status, text))
    }
}

fn escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

// note: client-side filtering implemented via apply_client_filters_vec

fn apply_client_filters_vec(
    items: Vec<serde_json::Value>,
    unread: bool,
    since: Option<&str>,
) -> Result<Vec<serde_json::Value>> {
    let since_str = since.unwrap_or("");
    let do_since = !since_str.is_empty();
    let mut out = Vec::with_capacity(items.len());
    for item in items.into_iter() {
        let mut keep = true;
        if unread {
            let is_read = item
                .get("isRead")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if is_read {
                keep = false;
            }
        }
        if keep && do_since {
            if let Some(dt) = item.get("receivedDateTime").and_then(|v| v.as_str()) {
                if dt < since_str {
                    keep = false;
                }
            }
        }
        if keep {
            out.push(item);
        }
    }
    Ok(out)
}

fn sort_items_by_received(items: &mut [serde_json::Value], asc: bool) {
    items.sort_by(|a, b| {
        let ad = a
            .get("receivedDateTime")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let bd = b
            .get("receivedDateTime")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if asc {
            ad.cmp(bd)
        } else {
            bd.cmp(ad)
        }
    });
}

async fn graph_get_json_absolute_with_headers(
    access_token: &str,
    absolute_url: &str,
    headers: &[(&str, &str)],
) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let mut req = client.get(absolute_url).bearer_auth(access_token);
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let res = req.send().await.context("GET nextLink failed")?;
    if res.status().is_success() {
        Ok(res.json().await.context("parse JSON")?)
    } else if res.status() == StatusCode::UNAUTHORIZED {
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("401 Unauthorized: {}", text))
    } else {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        Err(anyhow!("Graph error {}: {}", status, text))
    }
}

fn collect_items(out: &mut Vec<serde_json::Value>, json: &mut serde_json::Value) {
    if let Some(arr) = json.get_mut("value").and_then(|v| v.as_array_mut()) {
        for v in arr.drain(..) {
            out.push(v);
        }
    }
}

async fn resolve_folder_id(access_token: &str, display_name: &str) -> Result<String> {
    // First try a direct well-known name mapping for speed
    let well_known = [
        ("Inbox", "inbox"),
        ("Drafts", "drafts"),
        ("Sent Items", "sentitems"),
        ("Deleted Items", "deleteditems"),
        ("Archive", "archive"),
        ("Junk Email", "junkemail"),
    ];
    for (name, wk) in &well_known {
        if name.eq_ignore_ascii_case(display_name) {
            // You can call /me/mailFolders/wellKnownFolderName to get the real id
            let path = format!("/v1.0/me/mailFolders/{}?$select=id", wk);
            let v = graph_get_json(access_token, &path).await?;
            if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
                return Ok(id.to_string());
            }
        }
    }

    // Fallback: search top-level folders by displayName
    let v = graph_get_json(
        access_token,
        "/v1.0/me/mailFolders?$select=id,displayName&$top=100",
    )
    .await?;
    if let Some(arr) = v.get("value").and_then(|x| x.as_array()) {
        if let Some(hit) = arr.iter().find(|f| {
            f.get("displayName")
                .and_then(|s| s.as_str())
                .map(|s| s.eq_ignore_ascii_case(display_name))
                .unwrap_or(false)
        }) {
            if let Some(id) = hit.get("id").and_then(|s| s.as_str()) {
                return Ok(id.to_string());
            }
        }
    }

    Err(anyhow!("Folder '{}' not found", display_name))
}

fn build_send_payload(to: &[String], subject: &str, body: &str, html: bool) -> serde_json::Value {
    let to_list: Vec<serde_json::Value> = to
        .iter()
        .map(|addr| serde_json::json!({ "emailAddress": { "address": addr } }))
        .collect();

    let content_type = if html { "HTML" } else { "Text" };

    serde_json::json!({
        "message": {
            "subject": subject,
            "body": {
                "contentType": content_type,
                "content": body
            },
            "toRecipients": to_list
        },
        "saveToSentItems": true
    })
}

async fn resolve_calendar_id(access_token: &str, display_name: &str) -> Result<String> {
    // Treat common aliases as primary
    if display_name.eq_ignore_ascii_case("primary")
        || display_name.eq_ignore_ascii_case("default")
        || display_name.eq_ignore_ascii_case("calendar")
    {
        let v = graph_get_json(access_token, "/v1.0/me/calendar?$select=id").await?;
        if let Some(id) = v.get("id").and_then(|s| s.as_str()) {
            return Ok(id.to_string());
        }
    }

    // Fallback: enumerate calendars and match by name
    let v = graph_get_json(access_token, "/v1.0/me/calendars?$select=id,name&$top=100").await?;
    if let Some(arr) = v.get("value").and_then(|x| x.as_array()) {
        if let Some(hit) = arr.iter().find(|c| {
            c.get("name")
                .and_then(|s| s.as_str())
                .map(|s| s.eq_ignore_ascii_case(display_name))
                .unwrap_or(false)
        }) {
            if let Some(id) = hit.get("id").and_then(|s| s.as_str()) {
                return Ok(id.to_string());
            }
        }
    }

    Err(anyhow!("Calendar '{}' not found", display_name))
}

fn build_event_payload(
    subject: &str,
    start: &str,
    end: &str,
    tz: &str,
    body: Option<&str>,
    html: bool,
    attendees: &[String],
    location: Option<&str>,
) -> serde_json::Value {
    let attendee_list: Vec<serde_json::Value> = attendees
        .iter()
        .map(|addr| {
            serde_json::json!({
                "emailAddress": { "address": addr },
                "type": "required"
            })
        })
        .collect();

    let content_type = if html { "HTML" } else { "Text" };
    let body_obj = body.map(|b| serde_json::json!({ "contentType": content_type, "content": b })).unwrap_or_else(|| serde_json::json!({ "contentType": content_type, "content": "" }));

    let mut ev = serde_json::json!({
        "subject": subject,
        "body": body_obj,
        "start": { "dateTime": start, "timeZone": tz },
        "end":   { "dateTime": end,   "timeZone": tz },
        "attendees": attendee_list,
    });

    if let Some(loc) = location {
        if let Some(obj) = ev.as_object_mut() {
            obj.insert(
                "location".to_string(),
                serde_json::json!({ "displayName": loc }),
            );
        }
    }

    ev
}
