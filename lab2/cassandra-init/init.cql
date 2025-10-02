CREATE KEYSPACE IF NOT EXISTS labks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS labks.users_by_country (
  country text,
  id uuid,
  name text,
  email text,
  created_at timestamp,
  PRIMARY KEY (country, id)
);
