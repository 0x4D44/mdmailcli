use assert_cmd::prelude::*;
use httpmock::prelude::*;
use predicates::str::contains;
use std::process::Command;

fn common_env(cmd: &mut Command, server: &MockServer) {
    cmd.env("MDMAILCLI_GRAPH_BASE_URL", server.base_url())
        .env("MDMAILCLI_TEST_ACCESS_TOKEN", "test-token");
}

#[test]
fn whoami_works_with_mock() {
    let server = MockServer::start();
    let _m = server.mock(|when, then| {
        when.method(GET)
            .path("/v1.0/me")
            .header("authorization", "Bearer test-token");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"userPrincipalName":"user@example.com"}));
    });

    let mut cmd = Command::cargo_bin("mdmailcli").unwrap();
    common_env(&mut cmd, &server);
    cmd.arg("whoami");
    cmd.assert().success().stdout(contains("userPrincipalName"));
}

#[test]
fn calendars_list_basic() {
    let server = MockServer::start();
    let _m = server.mock(|when, then| {
        when.method(GET).path("/v1.0/me/calendars");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"value":[{"id":"cal1","name":"Calendar"}]}));
    });

    let mut cmd = Command::cargo_bin("mdmailcli").unwrap();
    common_env(&mut cmd, &server);
    cmd.args(["calendars-list", "--top", "1"]);
    cmd.assert().success().stdout(contains("cal1"));
}

#[test]
fn events_list_date_range_uses_calendar_view_and_tz_header() {
    let server = MockServer::start();
    let _m = server.mock(|when, then| {
        when.method(GET)
            .path("/v1.0/me/calendar/calendarView")
            .query_param("startDateTime", "2025-09-10T00:00:00")
            .query_param("endDateTime", "2025-09-11T00:00:00")
            .header("prefer", "outlook.timezone=\"UTC\"");
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"value":[]}));
    });

    let mut cmd = Command::cargo_bin("mdmailcli").unwrap();
    common_env(&mut cmd, &server);
    cmd.args([
        "events-list",
        "--start",
        "2025-09-10T00:00:00",
        "--end",
        "2025-09-11T00:00:00",
        "--tz",
        "UTC",
        "--top",
        "5",
    ]);
    cmd.assert().success();
}

#[test]
fn events_busy_posts_schedule() {
    let server = MockServer::start();
    let _m = server.mock(|when, then| {
        when.method(POST)
            .path("/v1.0/me/calendar/getSchedule")
            .header("prefer", "outlook.timezone=\"UTC\"")
            .json_body(serde_json::json!({
                "schedules": ["alice@example.com","bob@example.com"],
                "startTime": {"dateTime":"2025-09-10T09:00:00","timeZone":"UTC"},
                "endTime": {"dateTime":"2025-09-10T18:00:00","timeZone":"UTC"},
                "availabilityViewInterval": 60
            }));
        then.status(200)
            .header("content-type", "application/json")
            .json_body(serde_json::json!({"value":[{"scheduleId":"alice@example.com"}]}));
    });

    let mut cmd = Command::cargo_bin("mdmailcli").unwrap();
    common_env(&mut cmd, &server);
    cmd.args([
        "events-busy",
        "--start",
        "2025-09-10T09:00:00",
        "--end",
        "2025-09-10T18:00:00",
        "--tz",
        "UTC",
        "--interval",
        "60",
        "--user",
        "alice@example.com",
        "--user",
        "bob@example.com",
    ]);
    cmd.assert().success().stdout(contains("scheduleId"));
}

#[test]
fn send_mail_accepts_202() {
    let server = MockServer::start();
    let _m = server.mock(|when, then| {
        when.method(POST).path("/v1.0/me/sendMail");
        then.status(202);
    });

    let mut cmd = Command::cargo_bin("mdmailcli").unwrap();
    common_env(&mut cmd, &server);
    cmd.args([
        "send-mail",
        "--subject",
        "Hello",
        "--body",
        "Hi there",
        "user@example.com",
    ]);
    cmd.assert().success();
}
