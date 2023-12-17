use std::collections::HashSet;
use std::time::Duration;
use std::fs::File;
use std::io::Write;

use reqwest::{
    self,
    header::{ACCEPT, AUTHORIZATION, USER_AGENT},
    Client,
};
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnection, Connection, Row};
use structopt::StructOpt;

use chrono::{NaiveDateTime, Days};

const GITUHB_REPO_URL: &str = "https://api.github.com/repositories";

#[derive(StructOpt, Debug)]
struct Opts {
    #[structopt(short, long)]
    database_url: String,
    #[structopt(short, long, required_unless_one = &["populate-comments", "generate-csv"])]
    iterations: Option<u32>,
    #[structopt(long)]
    populate_comments: bool,
    #[structopt(long)]
    generate_csv: bool,
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

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct Comment {
    id: i32,
    body: String,
    created_at: String,
    issue_id: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq)]
struct Commit {
    url: String,
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

async fn populate_comments(conn: &mut SqliteConnection, client: &Client) {
    let issues = sqlx::query("SELECT * FROM Issues")
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    
    let mut comments = HashSet::new();
    
    for issue in issues.iter() {
        let mut page = 1;
        let comments_url: String = issue.get("comments_url");
        let id_issue: i32 = issue.get("id_issue");
        

        loop {
            let url = &format!("{}?page={}&per_page=100", comments_url, page);
            println!("Retrieving Comments: {}", url);

            let response = {
                match client.get_github(url).send().await {
                    Ok(response) => response,
                    _ => continue,
                }
            };

            let comments_payload: Vec<Comment> = {
                match response.json().await {
                    Ok(comments) => comments,
                    Err(_) => continue,
                }
            };

            if comments_payload.is_empty() {
                break;
            }

            let formated_comments = comments_payload
                .into_iter()
                .map(|mut comment| {
                    comment.issue_id = Some(id_issue);
                    comment
                });

            comments.extend(formated_comments);
            page += 1;
        }
    }

    store_comments(conn, comments).await;
}

async fn count_commits_and_forks(conn: &mut SqliteConnection, client: &Client) {

    let mut data_file = File::create("data.csv").expect("creation failed");
    data_file.write("id_comment,id_issue,commits_before,commits_after\n".as_bytes()).expect("write failed");

    let comments = sqlx::query(
        r#"
        SELECT id_comment, Issues.id_issue as id_issue, Comments.created_at as created_at, Repositories.commits_url as commits_url
        FROM Comments, Repositories, Issues 
        WHERE is_toxic = 1 and Comments.id_issue = Issues.id_issue and Issues.id_repo = Repositories.id_repo
        "#)
        .fetch_all(&mut *conn)
        .await
        .unwrap();

    for comment in comments.iter() {
        let created_at: String = comment.get("created_at");
        let mut commits_url: String = comment.get("commits_url");
        commits_url = commits_url.strip_suffix("{/sha}").unwrap().to_string();
        let id_issue: i32 = comment.get("id_issue");
        let id_comment: i32 = comment.get("id_comment");
        
        write!(data_file, "{},{},", id_comment, id_issue).unwrap();

        let (since, until) = get_since_and_until(&created_at);
        
        let mut page = 1;
        let mut count = 0;
        
        loop {
            let url = &format!("{}?page={}&per_page=100&since={}&until={}", commits_url, page, since, created_at);
            println!("Retrieving Commits: {}", url);

            let response = {
                match client.get_github(url).send().await {
                    Ok(response) => response,
                    _ => continue,
                }
            };

            let payload: Vec<Commit> = {
                match response.json().await {
                    Ok(commits) => commits,
                    Err(_) => continue,
                }
            };

            if payload.is_empty() {
                break;
            }

            count += payload.into_iter().count();
            page += 1;
        }

        page = 1;
        write!(data_file, "{},", count).unwrap();
        count = 0;

        loop {
            let url = &format!("{}?page={}&per_page=100&since={}&until={}", commits_url, page, created_at, until);
            println!("Retrieving Commits: {}", url);

            let response = {
                match client.get_github(url).send().await {
                    Ok(response) => response,
                    _ => continue,
                }
            };

            let payload: Vec<Commit> = {
                match response.json().await {
                    Ok(list) => list,
                    Err(_) => continue,
                }
            };

            if payload.is_empty() {
                break;
            }

            count += payload.into_iter().count();
            page += 1;
        }

        write!(data_file, "{}\n", count).unwrap();
    }

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
        INSERT OR IGNORE INTO repositories (id_repo, name, forks_url, stars_url, commits_url)
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
        INSERT OR IGNORE INTO Issues (id_issue, id_repo, created_at, title, comments_url)
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
        .expect("failed to store issue in database");
    }
}

async fn store_comments(conn: &mut SqliteConnection, comments: HashSet<Comment>) {
    for comment in comments {
        sqlx::query!(
            r#"
        INSERT OR IGNORE INTO Comments (id_comment, id_issue, created_at, text, is_toxic)
        VALUES ($1, $2, $3, $4, $5)
        "#,
            comment.id,
            comment.issue_id,
            comment.created_at,
            comment.body,
            0
        )
        .execute(&mut *conn)
        .await
        .expect("failed to store comment in database");
    }
}

fn get_since_and_until(input_date: &str) -> (String, String) {
    let parsed_date = NaiveDateTime::parse_from_str(input_date, "%FT%TZ").unwrap();

    let thirty_days_before = parsed_date.checked_sub_days(Days::new(30)).unwrap();
    let thirty_days_after = parsed_date.checked_add_days(Days::new(30)).unwrap();

    let since = thirty_days_before.format("%FT%TZ").to_string();
    let until = thirty_days_after.format("%FT%TZ").to_string();

    (since, until)
}

#[tokio::main]
async fn main() {
    let opts = Opts::from_args();

    let mut seen_ids = HashSet::new();

    let client = Client::new();
    let mut url = get_random_repo_url(&mut seen_ids);

    let mut conn = SqliteConnection::connect(&opts.database_url).await.unwrap();

    if opts.populate_comments {
        println!("Retrieving and storing Comments for all Issues...");
        populate_comments(&mut conn, &client).await;

    } else if opts.generate_csv {
        println!("Counting commits, forks and generating CSV...");
        count_commits_and_forks(&mut conn, &client).await;
    
    } else {

        for _ in 0..opts.iterations.unwrap() {
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

}
