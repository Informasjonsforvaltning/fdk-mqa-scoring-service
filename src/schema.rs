table! {
    graphs (fdk_id) {
        fdk_id -> Varchar,
        score -> Varchar,
        vocab -> Varchar,
    }
}

allow_tables_to_appear_in_same_query!(graphs,);
