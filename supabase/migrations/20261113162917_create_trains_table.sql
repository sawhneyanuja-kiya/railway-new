/*
# Create trains table for the railway reservation demo

1. New Tables
- `trains`
  - `train_id` (integer, primary key)
  - `train_name` (text, not null)
  - `available_seats` (integer, not null)
2. Seed Data
- Express 101 with 1 seat (the race-condition demo train)
- Local 202 with 5 seats
3. Security
- Enable RLS on `trains`.
- No policies and no grants to `anon` or `authenticated`: the browser never talks to this table directly. Only the backend server reads and writes it.
4. Notes
- Seeding uses ON CONFLICT DO NOTHING so re-running this migration never changes existing seat counts.
*/

CREATE TABLE IF NOT EXISTS trains (
  train_id         integer PRIMARY KEY,
  train_name       text NOT NULL,
  available_seats  integer NOT NULL
);

ALTER TABLE trains ENABLE ROW LEVEL SECURITY;

INSERT INTO trains (train_id, train_name, available_seats) VALUES
  (1, 'Express 101', 1),
  (2, 'Local 202', 5)
ON CONFLICT (train_id) DO NOTHING;
