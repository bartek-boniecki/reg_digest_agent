-- minimal tables; keep it simple first
create table if not exists subscribers (
  id uuid primary key default gen_random_uuid(),
  email text unique not null,
  full_name text,
  org text,
  regions text[],         -- e.g. {'EU','US'}
  active boolean not null default true,
  created_at timestamptz default now()
);

create table if not exists sources (
  id serial primary key,
  name text not null,
  url text not null,
  type text not null check (type in ('rss','html')),
  region text,            -- 'EU','US','UK', etc.
  tags text[],            -- ['AML','Data Privacy']
  last_checked timestamptz
);

create table if not exists articles (
  id uuid primary key default gen_random_uuid(),
  source_id int references sources(id),
  url text unique not null,
  title text,
  published_at timestamptz,
  raw_text text,
  hash text,              -- sha1 of url or content for dedupe
  inserted_at timestamptz default now()
);

create table if not exists digests (
  id uuid primary key default gen_random_uuid(),
  generated_at timestamptz default now(),
  period text,            -- 'weekly-2025-08-14' or 'on-demand-2025-08-12T10:05'
  html text not null
);

create table if not exists deliveries (
  id uuid primary key default gen_random_uuid(),
  digest_id uuid references digests(id),
  subscriber_id uuid references subscribers(id),
  sent_at timestamptz default now(),
  status text
);
