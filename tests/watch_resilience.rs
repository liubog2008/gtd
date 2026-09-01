use std::{net::SocketAddr, path::Path, sync::Arc, time::Duration};

use diesel::{Connection, QueryDsl, RunQueryDsl, SqliteConnection};
use gtd::{
    api,
    domain::{Task, TaskList, TaskListResponse, TaskState},
    repository::{SqliteRepository, TaskRepository},
    schema::task_events,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpListener, TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::oneshot,
    task::JoinHandle,
};

struct RunningServer {
    address: SocketAddr,
    repository: Arc<SqliteRepository>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl RunningServer {
    async fn start(database: &Path) -> Self {
        let repository = Arc::new(SqliteRepository::new(database.to_str().unwrap()).unwrap());
        let (app, _) = api::router(repository.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = receiver.await;
                })
                .await
                .unwrap();
        });
        Self {
            address,
            repository,
            shutdown: Some(shutdown),
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    async fn shutdown(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        self.task.await.unwrap();
    }

    async fn crash(mut self) {
        self.shutdown.take();
        self.task.abort();
        let _ = self.task.await;
    }
}

struct WatchConnection {
    lines: tokio::io::Lines<BufReader<OwnedReadHalf>>,
    _write: OwnedWriteHalf,
}

impl WatchConnection {
    async fn connect(
        address: SocketAddr,
        revision: Option<i64>,
        last_event_id: Option<i64>,
    ) -> Self {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let query = revision
            .map(|revision| format!("&revision={revision}"))
            .unwrap_or_default();
        let last_event_id = last_event_id
            .map(|revision| format!("Last-Event-ID: {revision}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET /api/v1/tasks?watch=true{query} HTTP/1.1\r\nHost: {address}\r\nAccept: text/event-stream\r\n{last_event_id}\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();

        let (read, write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        let status = lines.next_line().await.unwrap().unwrap();
        assert!(
            status.contains(" 200 "),
            "unexpected watch response: {status}"
        );
        while let Some(line) = lines.next_line().await.unwrap() {
            if line.is_empty() {
                break;
            }
        }
        Self {
            lines,
            _write: write,
        }
    }

    async fn next_revision(&mut self) -> i64 {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let line = self
                    .lines
                    .next_line()
                    .await
                    .unwrap()
                    .expect("watch connection closed before the next event");
                if let Some(revision) = line.strip_prefix("id: ") {
                    return revision.parse().unwrap();
                }
            }
        })
        .await
        .expect("timed out waiting for a watch event")
    }
}

async fn create_task(client: &reqwest::Client, base_url: &str, description: &str) -> Task {
    client
        .post(format!("{base_url}/api/v1/tasks"))
        .json(&serde_json::json!({"description": description}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn update_task(
    client: &reqwest::Client,
    base_url: &str,
    task: &Task,
    list: TaskList,
    state: TaskState,
) -> Task {
    client
        .put(format!("{base_url}/api/v1/tasks/{}", task.metadata.id))
        .json(&serde_json::json!({
            "metadata": {"revision": task.metadata.revision},
            "description": task.description,
            "list": list,
            "state": state,
            "labels": task.labels,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn watch_replays_unacknowledged_events_after_server_restart() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("server-restart.db");
    let client = reqwest::Client::new();
    let server = RunningServer::start(&database).await;

    for number in 1..=4 {
        create_task(
            &client,
            &server.base_url(),
            &format!("before restart {number}"),
        )
        .await;
    }

    let mut watch = WatchConnection::connect(server.address, Some(1), None).await;
    assert_eq!(watch.next_revision().await, 1);
    server.crash().await;
    drop(watch);

    let server = RunningServer::start(&database).await;
    let mut resumed = WatchConnection::connect(server.address, None, Some(1)).await;
    let mut revisions = vec![1];
    for _ in 0..3 {
        revisions.push(resumed.next_revision().await);
    }
    create_task(&client, &server.base_url(), "after restart").await;
    revisions.push(resumed.next_revision().await);

    assert_eq!(revisions, vec![1, 2, 3, 4, 5]);
    drop(resumed);
    server.shutdown().await;
}

#[tokio::test]
async fn watch_client_restart_resumes_from_its_persisted_revision() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("client-restart.db");
    let client = reqwest::Client::new();
    let server = RunningServer::start(&database).await;

    for number in 1..=6 {
        create_task(&client, &server.base_url(), &format!("event {number}")).await;
    }

    let mut first_client = WatchConnection::connect(server.address, Some(1), None).await;
    assert_eq!(first_client.next_revision().await, 1);
    assert_eq!(first_client.next_revision().await, 2);
    drop(first_client);

    let mut restarted_client = WatchConnection::connect(server.address, Some(3), None).await;
    let mut resumed = Vec::new();
    for _ in 0..4 {
        resumed.push(restarted_client.next_revision().await);
    }
    assert_eq!(resumed, vec![3, 4, 5, 6]);

    drop(restarted_client);
    server.shutdown().await;
}

#[tokio::test]
async fn watch_recovers_events_committed_during_a_network_interruption() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("network-interruption.db");
    let client = reqwest::Client::new();
    let server = RunningServer::start(&database).await;

    let mut watch = WatchConnection::connect(server.address, Some(1), None).await;
    create_task(&client, &server.base_url(), "before disconnect").await;
    assert_eq!(watch.next_revision().await, 1);
    drop(watch); // Simulate a broken TCP connection without notifying the Server.

    for number in 2..=4 {
        create_task(
            &client,
            &server.base_url(),
            &format!("while disconnected {number}"),
        )
        .await;
    }

    let mut resumed = WatchConnection::connect(server.address, None, Some(1)).await;
    let mut revisions = Vec::new();
    for _ in 0..3 {
        revisions.push(resumed.next_revision().await);
    }
    assert_eq!(revisions, vec![2, 3, 4]);

    drop(resumed);
    server.shutdown().await;
}

#[tokio::test]
async fn compact_rejects_old_watchers_and_preserves_watchers_after_the_watermark() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("compact-watch.db");
    let client = reqwest::Client::new();
    let server = RunningServer::start(&database).await;

    let task = create_task(&client, &server.base_url(), "versioned task").await;
    let task = update_task(
        &client,
        &server.base_url(),
        &task,
        TaskList::Inbox,
        TaskState::Doing,
    )
    .await;
    let _task = update_task(
        &client,
        &server.base_url(),
        &task,
        TaskList::Archive,
        TaskState::Trash,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        server
            .repository
            .revision_state()
            .await
            .unwrap()
            .scheduled_revision,
        0,
        "automatic periodic compaction must remain disabled without a retention policy"
    );

    let mut active = WatchConnection::connect(server.address, Some(3), None).await;
    assert_eq!(active.next_revision().await, 3);

    let state = server.repository.compact(3).await.unwrap();
    assert_eq!(state.scheduled_revision, 3);
    assert_eq!(state.finished_revision, 3);

    let mut connection = SqliteConnection::establish(database.to_str().unwrap()).unwrap();
    let remaining = task_events::table
        .count()
        .get_result::<i64>(&mut connection)
        .unwrap();
    assert_eq!(
        remaining, 1,
        "compact must remove the two superseded events"
    );

    let response = client
        .get(format!(
            "{}/api/v1/tasks?watch=true&revision=3",
            server.base_url()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::GONE);

    create_task(&client, &server.base_url(), "revision 4").await;
    assert_eq!(active.next_revision().await, 4);
    create_task(&client, &server.base_url(), "revision 5").await;
    assert_eq!(active.next_revision().await, 5);
    drop(active);

    let mut retained = WatchConnection::connect(server.address, Some(4), None).await;
    assert_eq!(retained.next_revision().await, 4);
    assert_eq!(retained.next_revision().await, 5);
    drop(retained);

    let mut live = WatchConnection::connect(server.address, Some(6), None).await;
    server.repository.compact(5).await.unwrap();
    create_task(&client, &server.base_url(), "after compact").await;
    assert_eq!(live.next_revision().await, 6);

    let tasks: TaskListResponse = client
        .get(format!("{}/api/v1/tasks", server.base_url()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(tasks.metadata.revision, 6);
    assert_eq!(tasks.items.len(), 4);

    drop(live);
    server.shutdown().await;
}
