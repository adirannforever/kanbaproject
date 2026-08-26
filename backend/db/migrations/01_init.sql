CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    assigned _to VARCHAR(100),
    status VARCHAR(50) DEFAULT 'pending',
    user_id VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXIST idx_tasks_user_status ON tasks (user_id, status);