CREATE TABLE public.example (timestamp TIMESTAMP NOT NULL, id VARCHAR NOT NULL, column1 DOUBLE PRECISION NULL, column2 DOUBLE PRECISION NULL);
SELECT public.create_hypertable('public.example', 'timestamp');
