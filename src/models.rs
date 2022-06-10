use super::schema::graphs;

#[derive(Insertable, Queryable, AsChangeset)]
#[diesel(table_name = graphs)]
pub struct Graph {
    pub fdk_id: String,
    pub score: String,
    pub vocab: String,
}
