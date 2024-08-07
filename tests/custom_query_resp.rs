use skytable::{
    query,
    query::{QList, SQParam},
    response::{FromResponse, RList, Response, Row, Value},
};

/*
    our model looks like:

    create model myapp.data(
        username: string,
        password: string,
        notes: list { type: string },
    )
*/

#[derive(Debug, PartialEq)]
struct BookmarkUser {
    username: String,
    password: String,
    notes: Vec<String>,
}

impl BookmarkUser {
    fn test_user() -> Self {
        Self {
            username: "sayan".to_owned(),
            password: "pass123".to_owned(),
            notes: vec![
                "https://example.com".to_owned(),
                "https://skytable.org".to_owned(),
                "https://docs.skytable.org".to_owned(),
            ],
        }
    }
}

impl<'a> SQParam for &'a BookmarkUser {
    fn append_param(&self, q: &mut Vec<u8>) -> usize {
        self.username.append_param(q)
            + self.password.append_param(q)
            + QList::new(&self.notes).append_param(q)
    }
}

impl FromResponse for BookmarkUser {
    fn from_response(resp: Response) -> skytable::ClientResult<Self> {
        let (username, password, notes) = resp.parse::<(String, String, RList<String>)>()?;
        Ok(Self {
            username,
            password,
            notes: notes.into_values(),
        })
    }
}

#[test]
fn dynlist_q() {
    let bu = BookmarkUser::test_user();
    let q = query!(
        "insert into myapp.data { username: ?, password: ?, notes: ? }",
        &bu
    );
    assert_eq!(q.param_cnt(), 3);
}

#[test]
fn dynlist_r() {
    // assume that this is the response we got from the server (as a row); this may look messy but in a real-world application, the library does this for you
    // under the hood, so don't worry! you'll never have to write this yourself!
    let resp_from_server = Response::Row(Row::from(vec![
        Value::String("sayan".to_owned()),
        Value::String("pass123".to_owned()),
        Value::List(vec![
            Value::String("https://example.com".to_owned()),
            Value::String("https://skytable.org".to_owned()),
            Value::String("https://docs.skytable.org".to_owned()),
        ]),
    ]));
    // now this is our "fetch code"
    let user: BookmarkUser = resp_from_server.parse().unwrap();
    assert_eq!(user, BookmarkUser::test_user());
}
