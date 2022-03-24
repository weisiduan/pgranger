CREATE TABLE user_profile (
    user_id uuid PRIMARY KEY,
    name varchar(256) NOT NULL
);

CREATE TABLE user_event (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL
);
CREATE INDEX userevent_userid_idx ON user_event (user_id);
