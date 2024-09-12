/*
    assume the model is created using:

    > create space myapp
    > create model myapp.mydb(username: string, password: string, data: [string])
*/

use skytable::{query::QList, response::RList, Config};

fn get_list_data_from_api() -> Vec<String> {
    vec![
        "stuff".to_owned(),
        "from".to_owned(),
        "the".to_owned(),
        "api".to_owned(),
    ]
}

fn main() {
    let mut db = Config::new_default("root", "password12345678")
        .connect()
        .unwrap();
    let data_from_api = get_list_data_from_api();
    let q = skytable::query!(
        "insert into myapp.mydb { username: ?, password: ?, data: ? }",
        "sayan",
        "ulw06afuMCAg+1gh2lh1Y9xTIr/dUv2vqGLeZ39cVrE=",
        QList::new(&data_from_api)
    );
    db.query_parse::<()>(&q).unwrap(); // expect this data to be inserted correctly
                                       // now fetch this data
    let (username, password, data): (String, String, RList<String>) = db
        .query_parse(&skytable::query!(
            "select * from myapp.mydb where username = ?",
            "sayan"
        ))
        .unwrap();
    assert_eq!(username, "sayan");
    assert_eq!(password, "ulw06afuMCAg+1gh2lh1Y9xTIr/dUv2vqGLeZ39cVrE=");
    assert_eq!(data.into_values(), data_from_api);
}
