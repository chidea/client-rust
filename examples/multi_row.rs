use skytable::{query, response::Rows, Config, Query, Response};

#[derive(Query, Response)]
pub struct User {
    username: String,
    password: String,
    followers: u64,
    email: Option<String>,
}

fn main() {
    let mut db = Config::new_default("user", "password").connect().unwrap();
    let users: Rows<User> = db
        .query_parse(&query!(
            "select all username, password, followers, email from myspace.mymodel limit ?",
            1000u64
        ))
        .unwrap();
    // assume the first row has username set to 'sayan'
    assert_eq!(users[0].username, "sayan");
}
