-- Create the read-only PostgreSQL role used by the internal LLM.
--
-- Run as the database owner (or a superuser) against the application
-- database. The password placeholder must be replaced with a strong, unique
-- value that is stored outside this repository (a secret manager, password
-- vault, etc.). This file should NEVER contain a real password.
--
-- Usage example (after replacing the placeholder):
--     docker exec -i tiny-mirror-postgres psql -U tiny_mirror \
--         -d tiny_mirror_db -f /tmp/create_readonly_user.sql
--
-- Idempotency: the role is created only if it does not already exist; the
-- grants and revokes are safe to re-run.

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tiny_readonly') THEN
        CREATE ROLE tiny_readonly WITH LOGIN PASSWORD 'CHANGE_ME_BEFORE_RUNNING';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE tiny_mirror_db TO tiny_readonly;
GRANT USAGE ON SCHEMA public TO tiny_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tiny_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO tiny_readonly;

-- Explicitly revoke anything other than SELECT, both on existing tables and
-- on tables created in the future.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON ALL TABLES IN SCHEMA public FROM tiny_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
    ON TABLES FROM tiny_readonly;
REVOKE ALL ON SCHEMA public FROM tiny_readonly;
GRANT USAGE ON SCHEMA public TO tiny_readonly;
