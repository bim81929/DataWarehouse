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
create table article
(
    id             text not null
        primary key,
    raw_id         text not null
        constraint fk_list_to_article
            references list,
    domain         text not null,
    url            text not null,
    category       text,
    title          text not null,
    author         text,
    summary        text,
    description    text not null,
    date_submitted date,
    created_date   date not null
);

alter table article
    owner to dev;



