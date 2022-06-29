-- auto-generated definition
create table list
(
    id           text not null
        primary key,
    domain       text not null,
    url          text not null,
    category     text,
    title        text,
    summary      text,
    created_date date not null
);

alter table list
    owner to dev;


-- auto-generated definition
create table list
(
    id           text not null
        primary key,
    domain       text not null,
    url          text not null,
    category     text,
    title        text,
    summary      text,
    created_date date not null
);

alter table list
    owner to dev;

