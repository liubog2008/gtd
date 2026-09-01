diesel::table! {
    task_events (revision) {
        revision -> BigInt,
        task_id -> Text,
        prev_revision -> Nullable<BigInt>,
        event_type -> Text,
        description -> Text,
        list_name -> Text,
        state -> Text,
        labels -> Text,
        created_at -> Timestamp,
        updated_at -> Timestamp,
        prev_description -> Nullable<Text>,
        prev_list_name -> Nullable<Text>,
        prev_state -> Nullable<Text>,
        prev_labels -> Nullable<Text>,
        prev_updated_at -> Nullable<Timestamp>,
    }
}

diesel::table! {
    task_event_compaction (singleton) {
        singleton -> Integer,
        scheduled_revision -> BigInt,
        finished_revision -> BigInt,
    }
}

diesel::allow_tables_to_appear_in_same_query!(task_event_compaction, task_events);
