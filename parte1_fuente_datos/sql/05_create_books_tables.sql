CREATE SCHEMA IF NOT EXISTS books;

-- metadata
CREATE TABLE IF NOT EXISTS books.books_data (
    title TEXT PRIMARY KEY,
    description TEXT,
    authors TEXT,
    image TEXT,
    preview_link TEXT,
    publisher TEXT,
    published_date TEXT,
    info_link TEXT,
    categories TEXT,
    ratings_count NUMERIC
);

-- ratings
CREATE TABLE IF NOT EXISTS books.books_rating (
    id TEXT,
    title TEXT REFERENCES books.books_data(title),
    price NUMERIC,
    user_id TEXT,
    profile_name TEXT,
    review_helpfulness TEXT,
    review_score NUMERIC,
    review_time BIGINT,
    review_summary TEXT,
    review_text TEXT
);


CREATE INDEX IF NOT EXISTS idx_rating_title ON books.books_rating(title);
CREATE INDEX IF NOT EXISTS idx_rating_score ON books.books_rating(review_score);
CREATE INDEX IF NOT EXISTS idx_rating_user ON books.books_rating(user_id);