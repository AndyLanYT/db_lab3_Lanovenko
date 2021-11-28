-- SELECT * FROM authors

DO $$
DECLARE
    author_id   authors.author_id%TYPE;
    author_name authors.author_name%TYPE;

BEGIN
    author_id := 10;
    author_name := 'AuthorName';
    FOR counter IN 1..10
        LOOP
            INSERT INTO authors(author_id, author_name)
            VALUES (author_id + counter, author_name || counter);
        END LOOP;
END;
$$
