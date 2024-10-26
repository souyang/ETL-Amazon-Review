CREATE SEQUENCE IF NOT EXISTS books_id_seq;
CREATE TABLE IF NOT EXISTS books (
        id INTEGER NOT NULL DEFAULT nextval('books_id_seq'),
        title TEXT NOT NULL,
        authors TEXT,
        price NUMERIC,
        rating FLOAT,
        search_term TEXT NOT NULL,
        updated_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id, search_term, updated_datetime)
    ) PARTITION BY RANGE (updated_datetime)