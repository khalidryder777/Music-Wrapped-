--###################################################################################################
-- CREATE STATEMENTS
--###################################################################################################

drop table if exists Users, Songs, Artists, Bands, Albums cascade;
drop table if exists Song_Plays, Artists_Form_Bands, Artists_Win_Awards, Albums_List_Songs, Artists_Create_Songs, Artists_Create_Albums, Bands_Create_Songs, Bands_Create_Albums, Users_Libraries cascade;

create table Users(
        name varchar(128),
        dob integer,
        since integer,
        country varchar(128),
        primary key(name, dob)
);

create table Songs(
        name varchar(128),
        release_date smallint,
        genre varchar(128),
        lang varchar(128),
        primary key(name, release_date)
);

create table Artists(
        name varchar(128),
        dob integer,
        main_skill varchar(128),
        other_skills varchar(128),
        primary key(name, dob)
);

create table Bands(
        name varchar(128),
        since smallint,
        primary key(name, since)
);

create table Albums(
        name varchar(128),
        release_date smallint,
        primary key(name, release_date)
);

create table Song_Plays(
        uname varchar(128),
        udob integer,
        sname varchar(128),
        srelease_date smallint,
        play_ts bigint,
        primary key(uname, udob, sname, srelease_date, play_ts),
        foreign key (uname, udob) references Users(name, dob),
        foreign key (sname, srelease_date) references Songs(name, release_date)
);

create table Artists_Form_Bands(
        bname varchar(128),
        bsince smallint,
        aname varchar(128),
        adob integer,
        primary key(bname, bsince, aname, adob),
        foreign key (bname, bsince) references Bands(name, since),
        foreign key (aname, adob) references Artists(name, dob)
);

create table Artists_Win_Awards(
        aname varchar(128),
        adob integer,
        award_name varchar(128),
        award_year smallint,
        primary key(aname, adob, award_name, award_year),
        foreign key (aname, adob) references Artists(name, dob) on delete cascade
);

create table Albums_List_Songs(
        aname varchar(128),
        arelease_date smallint,
        sname varchar(128),
        srelease_date smallint,
        primary key(aname, arelease_date, sname, srelease_date),
        unique (sname, srelease_date),
        foreign key(aname, arelease_date) references Albums(name, release_date),
        foreign key (sname, srelease_date) references Songs(name, release_date)
);

create table Artists_Create_Songs(
        aname varchar(128),
        adob integer,
        sname varchar(128),
        srelease_date smallint,
        creation_date smallint,
        primary key(aname, adob, sname, srelease_date),
        foreign key (aname, adob) references Artists(name, dob),
        foreign key (sname, srelease_date) references Songs(name, release_date)
);

create table Artists_Create_Albums(
        artist_name varchar(128),
        artist_dob integer,
        album_name varchar(128),
        album_release_date smallint,
        creation_date smallint,
        primary key(artist_name, artist_dob, album_name, album_release_date),
        foreign key (artist_name, artist_dob) references Artists(name, dob),
        foreign key (album_name, album_release_date) references Albums(name, release_date)
);

create table Bands_Create_Songs(
        bname varchar(128),
        bsince smallint,
        sname varchar(128),
        srelease_date smallint,
        creation_date smallint,
        primary key(bname, bsince, sname, srelease_date),
        foreign key (bname, bsince) references Bands(name, since),
        foreign key (sname, srelease_date) references Songs(name, release_date)
);

create table Bands_Create_Albums(
        bname varchar(128),
        bsince smallint,
        aname varchar(128),
        arelease_date smallint,
        creation_date smallint,
        primary key(bname, bsince, aname, arelease_date),
        foreign key (bname, bsince) references Bands(name, since),
        foreign key (aname, arelease_date) references Albums(name, release_date)
);

create table Users_Libraries(
        uname varchar(128),
        udob integer,
        lib_name varchar(128),
        sname varchar(128),
        srelease_date smallint,
        since integer,
        primary key(uname, udob, lib_name, sname, srelease_date),
        foreign key (uname, udob) references Users(name, dob) on delete cascade,
        foreign key (sname, srelease_date) references Songs(name, release_date)
);