/*
    assume the model is created using:

    > create space myapp
    > create model myapp.mydb(username: string, password: string, data: [string])

    -------------

    This is an example just like `dynamic_lists_simple.rs` but using a struct instead
*/

use skytable::{
    query::{QList, SQParam},
    response::{FromResponse, RList},
    Config,
};

#[derive(Debug, PartialEq)]
struct User {
    username: String,
    password: String,
    data: Vec<String>,
}

impl User {
    fn new(username: String, password: String, data: Vec<String>) -> Self {
        Self {
            username,
            password,
            data,
        }
    }
}

impl SQParam for User {
    fn append_param(&self, q: &mut Vec<u8>) -> usize {
        self.username.append_param(q)
            + self.password.append_param(q)
            + QList::new(&self.data).append_param(q)
    }
}

impl FromResponse for User {
    fn from_response(resp: skytable::response::Response) -> skytable::ClientResult<Self> {
        let (username, password, data) = resp.parse::<(_, _, RList<String>)>()?;
        Ok(Self::new(username, password, data.into_values()))
    }
}

fn get_data_from_api() -> User {
    User {
        username: "sayan".to_owned(),
        password: "ulw06afuMCAg+1gh2lh1Y9xTIr/dUv2vqGLeZ39cVrE=".to_owned(),
        data: vec![
            "stuff".to_owned(),
            "from".to_owned(),
            "the".to_owned(),
            "api".to_owned(),
        ],
    }
}

fn main() {
    let mut db = Config::new_default("root", "password12345678")
        .connect()
        .unwrap();
    let data_from_api = get_data_from_api();
    db.query_parse::<()>(&skytable::query!(
        "insert into myapp.mydb { username: ?, password: ?, data: ? }",
        &data_from_api
    ))
    .unwrap();
    let fetched_user: User = db
        .query_parse(&skytable::query!(
            "select * from myapp.mydb where username = ?",
            "sayan"
        ))
        .unwrap();
    assert_eq!(data_from_api, fetched_user);
}
