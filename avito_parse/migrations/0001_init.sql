CREATE SCHEMA advertisement;
-- id: int
--     url: str
--     price: int
--     title: str
--     address: str
--     approximate_date_string: str
--
--     date: datetime = None
--     description: str = None
CREATE TABLE advertisement.ad(
    id bigint UNIQUE NOT NULL,
    url text,
    title text,
    address text,
    price bigint,
    approximate_date_string text,
    date timestamp with time zone,
    description text
);


CREATE SCHEMA auth;

CREATE TABLE auth.user (
    id bigint UNIQUE,
    username text UNIQUE,
    first_name text,
    last_name text,
    chat_id bigint
);
