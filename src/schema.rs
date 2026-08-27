diesel::table! {
    tasks (id) {
        id -> Integer,
        description -> Text,
        list_name -> Text,
        state -> Text,
        context_note -> Nullable<Text>,
        revisit_at -> Nullable<Timestamp>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

diesel::table! {
    labels (task_id, key) {
        task_id -> Integer,
        key -> Text,
        value -> Text,
    }
}

diesel::joinable!(labels -> tasks (task_id));
diesel::allow_tables_to_appear_in_same_query!(labels, tasks);
