use std::collections::HashSet;
use std::time::Duration;

use reqwest::{
    self,
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    Client,
};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnection, Connection};
use structopt::StructOpt;

const GITUHB_REPO_URL: &str = "https://api.github.com/repositories";

#[derive(StructOpt, Debug)]
struct Opts {
    #[structopt(short, long)]
    database_url: String,
    #[structopt(short, long)]
    iterations: u32,
}

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct Repository {
    id: i32,
    name: String,
    forks_url: String,
    stargazers_url: String,
    commits_url: String,
    issues_url: String,
}

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct Issue {
    id: i32,
    title: String,
    created_at: String,
    repository_id: Option<i32>,
    comments_url: String,
    locked: bool,
    active_lock_reason: Option<String>,
    state: String,
}

trait GetGithub {
    fn get_github(&self, url: &str) -> reqwest::RequestBuilder;
}

impl GetGithub for reqwest::Client {
    fn get_github(&self, url: &str) -> reqwest::RequestBuilder {
        let github_token = std::env::var("GITHUB_TOKEN").unwrap();
        self.get(url)
            .header(ACCEPT, "application/vnd.github+json")
            .header(USER_AGENT, "toxicity-metodologia")
            .header(AUTHORIZATION, format!("Bearer {}", github_token))
    }
}

async fn get_repositories(client: &Client, url: &str) -> Vec<Repository> {
    match client.get_github(url).send().await {
        Ok(response) => response.json().await.unwrap_or(Vec::new()),
        _ => Vec::new(),
    }
}

async fn search_too_heated_issues(client: &Client, repository: &Repository) -> HashSet<Issue> {
    let issues_url = repository.issues_url.strip_suffix("{/number}").unwrap();
    let mut issues = HashSet::new();

    for page in 1..50 {
        let url = &format!("{}?page={}&per_page=100&state=closed", issues_url, page);
        println!("Searching issues: {}", url);

        let response = {
            match client.get_github(url).send().await {
                Ok(response) => response,
                _ => continue,
            }
        };

        let issues_payload: Vec<Issue> = {
            match response.json().await {
                Ok(issues) => issues,
                Err(_) => continue,
            }
        };

        if issues_payload.is_empty() {
            break;
        }

        let too_heated_issues = issues_payload
            .into_iter()
            .filter(|issues| {
                issues.locked
                    && issues.active_lock_reason == Some("too heated".to_string())
                    && &issues.state == "closed"
            })
            .map(|mut issue| {
                issue.repository_id = Some(repository.id);
                issue
            });

        issues.extend(too_heated_issues);
        std::thread::sleep(Duration::from_secs(5));
    }

    issues
}

type SeenIds = HashSet<u16>;

fn get_random_repo_url(seen_ids: &mut SeenIds) -> String {
    let random_id = {
        loop {
            let id = rand::random::<u16>();
            if !seen_ids.contains(&id) {
                break id;
            }
        }
    };
    seen_ids.insert(random_id);
    format!("{}?since={}", GITUHB_REPO_URL, random_id)
}

async fn store_respository(conn: &mut SqliteConnection, repository: Repository) {
    sqlx::query!(
        r#"
        INSERT INTO repositories (id_repo, name, forks_url, stars_url, commits_url)
        VALUES ($1, $2, $3, $4, $5)
        "#,
        repository.id,
        repository.name,
        repository.forks_url,
        repository.stargazers_url,
        repository.commits_url
    )
    .execute(&mut *conn)
    .await
    .expect("failed to store repository in database");
}

async fn store_issues(conn: &mut SqliteConnection, issues: HashSet<Issue>) {
    for issue in issues {
        sqlx::query!(
            r#"
        INSERT INTO Issues (id_issue, id_repo, created_at, title, comments_url)
        VALUES ($1, $2, $3, $4, $5)
        "#,
            issue.id,
            issue.repository_id,
            issue.created_at,
            issue.title,
            issue.comments_url
        )
        .execute(&mut *conn)
        .await
        .expect("failed to store repository in database");
    }
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

    let mut seen_ids = HashSet::new();

    let client = Client::new();
    let mut url = get_random_repo_url(&mut seen_ids);

    let mut conn = SqliteConnection::connect(&opts.database_url).await.unwrap();

    for _ in 0..opts.iterations {
        println!("Searching repositories: {}", url);
        let repositories = get_repositories(&client, &url).await;

        for repository in repositories {
            println!("Searching issues: {}", repository.name);

            let too_heated_issues = search_too_heated_issues(&client, &repository).await;
            if !too_heated_issues.is_empty() {
                println!("Found too heated issues in repository: {}", repository.name);
                store_respository(&mut conn, repository).await;
                store_issues(&mut conn, too_heated_issues).await;
            }
        }

        url = get_random_repo_url(&mut seen_ids);
        std::thread::sleep(Duration::from_secs(5));
    }
}
