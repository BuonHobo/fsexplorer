drop table if exists files;
create table files (
    id serial primary key,
    path text not null,
    name text not null,
    type text not null,
    owner text not null,
    size bigint not null,
    creation date not null,
    modification date not null,
    access date not null,
    collected_date date not null default CURRENT_DATE
);

ALTER TABLE files
ADD CONSTRAINT unique_file UNIQUE (path, name, type);
