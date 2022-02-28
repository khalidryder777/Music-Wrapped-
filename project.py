import logging
from configparser import ConfigParser

import pandas as pd
import psycopg2
import streamlit as st

logging.basicConfig(level=logging.DEBUG)


class DBHelper:

    @staticmethod
    @st.cache
    def __get_config(filename: str = "database.ini", section: str = "postgresql"):
        logging.info("DBHelper :: get_config() : start")
        logging.debug(f"filename: {filename}, section: {section}")
        
        parser = ConfigParser()
        parser.read(filename)
        configs = {k: v for k, v in parser.items(section)}
            
        logging.debug(f"configs: {configs}")
        logging.info("DBHelper :: get_config() : end")
        return configs

    @staticmethod
    @st.cache
    def query_db(sql: str):
        logging.info("DBHelper :: query_db() : start")
        logging.debug(f"sql: {sql}")

        db_info = DBHelper.__get_config()
        try:
            # Connect to an existing database
            conn = psycopg2.connect(**db_info)

            # Open a cursor to perform database operations
            cur = conn.cursor()

            # Execute a command
            cur.execute(sql)
        except Exception as e:
            st.write(e)

        # Obtain data
        data = cur.fetchall()
        logging.debug(f"data: {data}")

        column_names = [desc[0] for desc in cur.description]
        logging.debug(f"columns_names: {column_names}")

        # Make the changes to the database persistent
        conn.commit()

        # Close communication with the database
        cur.close()
        conn.close()

        df = pd.DataFrame(data=data, columns=column_names)
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBHelper :: query_db() : end")
        return df


class DBIO:
    Users = "Users"
    Songs = "Songs"
    Artists = "Artists"
    Bands = "Bands"
    Albums = "Albums"
    SongPlays = "Song_Plays"
    ArtistsCreateSongs = "Artists_Create_Songs"
    AlbumsListSongs = "Albums_List_Songs"
    BandsCreateAlbums = "Bands_Create_Albums"
    BandsCreateSongs = "Bands_Create_Songs"
    ArtistsCreateAlbums = "Artists_Create_Albums"
    UserLibraries = "Users_Libraries"
    ArtistsWinAwards = "Artists_Win_Awards"
    ArtistsFormBands = "Artists_Form_Bands"

    @staticmethod
    @st.cache
    def get_users(most_active: bool) -> pd.DataFrame:
        logging.info("DBIO :: get_users : start")

        if most_active:
            sql = f"""
                SELECT name AS name, dob AS dob, COUNT(*) AS plays
                FROM {DBIO.Users}, {DBIO.SongPlays}
                WHERE name = uname
                AND dob = udob
                GROUP BY name, dob
                ORDER BY COUNT(*) DESC, name
                LIMIT 10;
            """
        else:
            sql = f"""
                SELECT name AS name, dob AS dob, COUNT(*) AS plays
                FROM {DBIO.Users}, {DBIO.SongPlays}
                WHERE name = uname
                AND dob = udob
                GROUP BY name, dob
                ORDER BY name;
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_users : end")
        return df

    @staticmethod
    @st.cache
    def get_recently_played_songs_by_user(user: str, dob: int):
        logging.info("DBIO :: get_recently_played_songs_by_user : start")
        logging.debug(f"user: {user}")
        logging.debug(f"dob: {dob}")

        sql = f"""
                SELECT sname AS song, play_ts AS played_at
                FROM {DBIO.SongPlays}
                WHERE uname = '{user}'
                AND udob = {dob}
                ORDER BY play_ts DESC
                LIMIT 5;
        """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_recently_played_songs_by_user : end")
        return df

    @staticmethod
    @st.cache
    def get_most_played_songs_by_user(user: str, dob: int):
        logging.info("DBIO :: get_most_played_songs_by_user : start")
        logging.debug(f"user: {user}")
        logging.debug(f"dob: {dob}")

        sql = f"""
                    SELECT sname AS song, COUNT(*) AS numPlays
                    FROM {DBIO.SongPlays}
                    WHERE uname = '{user}'
                    AND udob = {dob}
                    GROUP BY sname
                    ORDER BY COUNT(*) DESC
                    LIMIT 8;
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_most_played_songs_by_user : end")
        return df

    @staticmethod
    @st.cache
    def get_most_played_genres_by_user(user: str, dob: int):
        logging.info("DBIO :: get_most_played_genres_by_user : start")
        logging.debug(f"user: {user}")
        logging.debug(f"dob: {dob}")

        sql = f"""
                SELECT genre, COUNT(*) AS numPlays
                FROM {DBIO.Songs}, {DBIO.SongPlays}
                WHERE uname = '{user}'
                AND udob = {dob}
                AND {DBIO.Songs}.name = {DBIO.SongPlays}.sname
                GROUP BY genre
                ORDER BY COUNT(*) DESC
                LIMIT 3
        """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_most_played_genres_by_user : end")
        return df

    @staticmethod
    @st.cache
    def get_recommended_songs_for_user(user: str, dob: int):
        logging.info("DBIO :: get_recommended_songs_for_user : start")

        logging.debug(f"user: {user}")
        logging.debug(f"dob: {dob}")

        sql = f"""
                    SELECT name AS song, genre
                    FROM {DBIO.Songs}
                    WHERE genre IN (
                        SELECT genre
                        FROM {DBIO.Songs}, {DBIO.SongPlays}
                        WHERE uname = '{user}'
                        AND udob = {dob}
                        AND {DBIO.Songs}.name = {DBIO.SongPlays}.sname
                        GROUP BY genre
                        ORDER BY COUNT(*) DESC
                        LIMIT 3
                    )
                    AND name NOT IN(
                        SELECT sname AS song
                        FROM {DBIO.SongPlays}
                        WHERE uname = '{user}'
                        AND udob = {dob}
                    )
                    ORDER BY name
                    LIMIT 10
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_recommended_songs_for_user : end")
        return df

    @staticmethod
    @st.cache
    def get_songs(most_played: bool) -> pd.DataFrame:
        logging.info("DBIO :: get_songs : start")

        if most_played:
            sql = f"""
                    SELECT S.name AS song, COUNT(*) AS numPlays, S.genre AS genre, S.release_date AS release
                    FROM {DBIO.Songs} S, {DBIO.SongPlays} SP
                    WHERE S.name = SP.sname
                    AND S.release_date = SP.srelease_date
                    GROUP BY S.name, S.release_date, S.genre
                    ORDER BY COUNT(*) DESC, S.name
                    LIMIT 10;
            """
        else:
            sql = f"""
                    SELECT S.name AS song, COUNT(*) AS numPlays, S.genre AS genre, S.release_date AS release
                    FROM {DBIO.Songs} S, {DBIO.SongPlays} SP
                    WHERE S.name = SP.sname
                    AND S.release_date = SP.srelease_date
                    GROUP BY S.name, S.release_date, S.genre
                    ORDER BY S.name;
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_songs : end")
        return df

    @staticmethod
    @st.cache
    def get_top_listeners_of_song(song: str, release: int):
        logging.info("DBIO :: get_top_listeners_of_song : start")
        logging.debug(f"song: {song}")
        logging.debug(f"release: {release}")

        sql = f"""
                SELECT uname AS user, COUNT(*) AS numPlays
                FROM {DBIO.SongPlays}
                WHERE sname = '{song}'
                AND srelease_date = {release}
                GROUP BY uname
                ORDER BY COUNT(*) DESC
                LIMIT 5
        """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_top_listeners_of_song : end")
        return df

    @staticmethod
    @st.cache
    def get_songs_with_common_listeners(song: str, release: int):
        logging.info("DBIO :: get_songs_with_common_listeners : start")
        logging.debug(f"song: {song}")
        logging.debug(f"release: {release}")

        sql = f"""
                    SELECT SP2.sname AS song, SP2.srelease_date AS release
                    FROM {DBIO.SongPlays} SP1, {DBIO.SongPlays} SP2
                    WHERE SP1.uname = SP2.uname
                    AND SP1.sname = '{song}'
                    AND SP1.srelease_date = {release}
                    AND SP2.sname != '{song}'
                    UNION
                    SELECT SP2.sname AS song, SP2.srelease_date AS release
                    FROM {DBIO.SongPlays} SP1, {DBIO.SongPlays} SP2
                    WHERE SP1.uname = SP2.uname
                    AND SP1.sname = '{song}'
                    AND SP1.srelease_date = {release}
                    AND SP2.srelease_date != {release}
                    ORDER BY release DESC, song
                    LIMIT 20
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_songs_with_common_listeners : end")
        return df

    @staticmethod
    @st.cache
    def get_artists_with_most_song_releases(start_year, end_year, award_won):
        logging.info("DBIO :: get_artists_with_most_song_releases : start")

        logging.debug(f"start_year: {start_year}")
        logging.debug(f"end_year: {end_year}")
        logging.debug(f"award_won: {award_won}")

        if award_won == "yes":
            sql = f"""
                    SELECT ACS.aname AS artist, ACS.adob AS dob, COUNT(*) AS numSongReleased
                    FROM {DBIO.ArtistsCreateSongs} ACS
                    WHERE ACS.aname IN (
                        SELECT aname
                        FROM {DBIO.ArtistsWinAwards}
                    )
                    AND ACS.srelease_date >= {start_year}
                    AND ACS.srelease_date <= {end_year}
                    GROUP BY ACS.aname, ACS.adob
                    ORDER BY COUNT(*) DESC, ACS.aname, ACS.adob
                    LIMIT 10
            """
        else:
            sql = f"""
                    SELECT ACS.aname AS artist, ACS.adob AS dob, COUNT(*) AS numSongReleased
                    FROM {DBIO.ArtistsCreateSongs} ACS
                    WHERE ACS.aname NOT IN (
                        SELECT aname
                        FROM {DBIO.ArtistsWinAwards}
                    )
                    AND ACS.srelease_date >= {start_year}
                    AND ACS.srelease_date <= {end_year}
                    GROUP BY ACS.aname, ACS.adob
                    ORDER BY COUNT(*) DESC, ACS.aname, ACS.adob
                    LIMIT 10
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_artists_with_most_song_releases : end")
        return df

    @staticmethod
    @st.cache
    def get_artists_with_most_album_releases(start_year, end_year, award_won):
        logging.info("DBIO :: get_artists_with_most_album_releases : start")

        logging.debug(f"start_year: {start_year}")
        logging.debug(f"end_year: {end_year}")
        logging.debug(f"award_won: {award_won}")

        if award_won == "yes":
            sql = f"""
                    SELECT ACA.artist_name AS artist, ACA.artist_dob AS dob, COUNT(*) AS numAlbumReleased
                    FROM {DBIO.ArtistsCreateAlbums} ACA
                    WHERE ACA.artist_name IN (
                        SELECT aname
                        FROM {DBIO.ArtistsWinAwards}
                    )
                    AND ACA.album_release_date >= {start_year}
                    AND ACA.album_release_date <= {end_year}
                    GROUP BY ACA.artist_name, ACA.artist_dob
                    ORDER BY COUNT(*) DESC, ACA.artist_name, ACA.artist_dob
                    LIMIT 10
            """
        else:
            sql = f"""
                    SELECT ACA.artist_name AS artist, ACA.artist_dob AS dob, COUNT(*) AS numAlbumReleased
                    FROM {DBIO.ArtistsCreateAlbums} ACA
                    WHERE ACA.artist_name NOT IN (
                        SELECT aname
                        FROM {DBIO.ArtistsWinAwards}
                    )
                    AND ACA.album_release_date >= {start_year}
                    AND ACA.album_release_date <= {end_year}
                    GROUP BY ACA.artist_name, ACA.artist_dob
                    ORDER BY COUNT(*) DESC, ACA.artist_name, ACA.artist_dob
                    LIMIT 10
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_artists_with_most_album_releases : end")
        return df

    @staticmethod
    @st.cache
    def get_artists_in_bands():
        logging.info("DBIO :: get_artists_in_bands : start")

        sql = f"""
                SELECT AFB.aname AS artist, AFB.bname AS band
                FROM {DBIO.ArtistsFormBands} AFB
        """

        logging.debug(sql)

        df = DBHelper.query_db(sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_artists_in_bands : end")
        return df

    @staticmethod
    @st.cache
    def get_bands(most_albums: bool) -> pd.DataFrame:
        logging.info("DBIO :: get_bands : start")

        if most_albums:
            sql = f"""
                    SELECT bname AS band, bsince AS since, COUNT(*) as numAlbums
                    FROM {DBIO.BandsCreateAlbums}
                    GROUP BY bname, bsince
                    ORDER BY COUNT(*) DESC
                    LIMIT 10 
            """
        else:
            sql = f"""
                    SELECT name AS band, since AS since
                    FROM {DBIO.Bands}
            """
        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("DBIO :: get_bands : end")
        return df

    @staticmethod
    @st.cache
    def get_genres():
        logging.info("get_genres : start")

        sql = f"""
                SELECT DISTINCT genre AS genre
                FROM {DBIO.Songs}
        """
        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("get_genres : end")
        return df

    @staticmethod
    @st.cache
    def get_bands_with_most_song_plays(year: int, genre: str):
        logging.info("get_bands_with_most_song_plays : start")

        sql = f"""
                SELECT BCS.bname AS band, BCS.bsince AS since, COUNT(*) AS numHits
                FROM {DBIO.Songs} S, {DBIO.SongPlays} SP, {DBIO.BandsCreateSongs} BCS
                WHERE BCS.sname = S.name
                AND BCS.srelease_date = S.release_date
                AND SP.sname = S.name
                AND SP.srelease_date = S.release_date
                AND S.genre = '{genre}'
                AND SP.play_ts > {year}0000000000
                AND SP.play_ts < {year + 1}0000000000
                GROUP BY BCS.bname, BCS.bsince
                ORDER BY COUNT(*) DESC, BCS.bname, BCS.bsince;
        """

        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("get_bands_with_most_song_plays : end")
        return df

    @staticmethod
    @st.cache
    def get_albums_most_featured_in_user_libraries(year: int, genre: str):
        logging.info("get_albums_most_featured_in_user_libraries : start")

        sql = f"""
                SELECT ALS.aname AS album, ALS.arelease_date AS release, COUNT(*) AS timesAdded
                FROM {DBIO.Songs} S, {DBIO.AlbumsListSongs} ALS, {DBIO.UserLibraries} UL
                WHERE UL.sname = ALS.sname
                AND UL.srelease_date = ALS.srelease_date
                AND UL.sname = S.name
                AND UL.srelease_date = S.release_date
                AND UL.since > {year}0000
                AND S.genre = '{genre}'
                GROUP BY ALS.aname, ALS.arelease_date
                ORDER BY COUNT(*) DESC, ALS.aname, ALS.arelease_date DESC
        """

        logging.debug(sql)

        df = DBHelper.query_db(sql=sql)

        logging.debug(f"df.columns: {df.columns}")
        logging.debug(f"df.shape: {df.shape}")

        logging.info("get_albums_most_featured_in_user_libraries : end")
        return df


def run():
    # Title of the App
    st.title("Music Wrapped!")

    main_areas = [DBIO.Users, DBIO.Songs, DBIO.Artists, DBIO.Bands, DBIO.Albums]

    # Main Selection Criteria: Users/Songs/Artists/Albums
    area = st.selectbox("Which statistics would you like to look at?", tuple(main_areas))

    # Display code for Users
    if area == DBIO.Users:
        # =============================================================================================
        # Generic Users Section
        # =============================================================================================
        st.subheader(DBIO.Users)

        # ui toggle for most active users
        most_active = st.checkbox("most active users")

        # Display users table and users list
        users_df = DBIO.get_users(most_active=most_active)
        st.write(users_df)

        users_list = list(zip(users_df["name"], users_df["dob"]))
        user, dob = st.selectbox("Select a User to view their Spotlight!", users_list)
        dob = int(dob)

        # =============================================================================================
        # Specific  User's Section: Spotlight
        # =============================================================================================
        recently_played_songs = "recently played songs"
        most_played_songs = "most played songs"
        most_played_genres = "most played genres"
        song_recommendations = "song recommendations"

        # selectbox to select what table/spotlight statistics to display
        spotlight = st.selectbox(f"Spotlight | {user}, {dob}", [recently_played_songs,
                                                                most_played_songs, most_played_genres,
                                                                song_recommendations])

        # display recently played songs by the user
        if spotlight == recently_played_songs:
            hist_songs_df = DBIO.get_recently_played_songs_by_user(user=user, dob=dob)
            st.write("Recently Played Songs")
            st.write(hist_songs_df)

        # display most played songs by the user
        elif spotlight == most_played_songs:
            mx_songs_df = DBIO.get_most_played_songs_by_user(user=user, dob=dob)
            st.write("Most Played Songs")
            st.write(mx_songs_df)

        # display most played genres by the user
        elif spotlight == most_played_genres:
            mx_genres_df = DBIO.get_most_played_genres_by_user(user=user, dob=dob)
            st.write("Most Played Genres")
            st.write(mx_genres_df)

        # display song recommendations for the user
        elif spotlight == song_recommendations:
            recomm_df = DBIO.get_recommended_songs_for_user(user=user, dob=dob)
            st.write("Song Recommendations")
            st.write(recomm_df)

    # Display code for Songs
    elif area == DBIO.Songs:
        # =============================================================================================
        # Generic Songs Section
        # =============================================================================================
        st.subheader(DBIO.Songs)

        # ui toggle for most played songs
        most_played = st.checkbox("most played songs")

        # Display songs table and songs list
        songs_df = DBIO.get_songs(most_played=most_played)
        st.write(songs_df)

        songs_list = list(zip(songs_df["song"], songs_df["release"]))
        song, release = st.selectbox("Select a Song to view its Spotlight!", songs_list)
        release = int(release)

        # =============================================================================================
        # Specific  Song's Section: Spotlight
        # =============================================================================================
        top_listeners = "top listeners"
        common_listeners = f"users who listen to '{song}({release})' also listen..."

        # selectbox to select what table/spotlight statistics to display
        spotlight = st.selectbox(f"Spotlight | {song}({release})", [top_listeners, common_listeners])

        # display Top Listeners of the Song
        if spotlight == top_listeners:
            top_list_df = DBIO.get_top_listeners_of_song(song=song, release=release)
            st.write("Top Listeners")
            st.write(top_list_df)

        # People who listen to "song" also listen...
        elif spotlight == common_listeners:
            rltd_songs_df = DBIO.get_songs_with_common_listeners(song=song, release=release)
            st.write(f"Users who listen to '{song}({release})' also listen...")
            st.write(rltd_songs_df)

    # Display code for Artists
    elif area == DBIO.Artists:
        st.subheader(DBIO.Artists)

        ###########################################################################################
        # Display Artists who appear in bands
        ###########################################################################################
        st.subheader(f"Artists who are member of a band:")
        artists_bands_df = DBIO.get_artists_in_bands()
        st.write(artists_bands_df)

        ###########################################################################################
        # Display Artists with most SOng releases b/w two years, based on award won condition
        ###########################################################################################
        st.subheader(f"Artists with most song/album releases b/w:")

        years_list = [1991 + i for i in range(31)]
        st_year = st.selectbox("Start Year", years_list)
        end_year = st.selectbox("End Year", years_list)
        award_won = st.selectbox("Award won?", ["yes", "no"])

        st_year = int(st_year)
        end_year = int(end_year)

        if st_year <= end_year:
            # song releases
            st.write("Song Releases:")
            cmplx_df = DBIO.get_artists_with_most_song_releases(start_year=st_year, end_year=end_year,
                                                                award_won=award_won)
            if cmplx_df.shape[0] == 0:
                st.write("No artists meet the given criteria")
            else:
                st.write(cmplx_df)

            # album releases
            st.write("Album Releases:")
            acmplx_df = DBIO.get_artists_with_most_album_releases(start_year=st_year, end_year=end_year,
                                                                  award_won=award_won)
            if acmplx_df.shape[0] == 0:
                st.write("No artists meet the given criteria")
            else:
                st.write(acmplx_df)
        else:
            st.error("Start Year CAN NOT be greater than the End Year")

    # Display code for Bands
    elif area == DBIO.Bands:
        # =============================================================================================
        # Generic Bands Section
        # =============================================================================================
        st.subheader(DBIO.Bands)

        # ui toggle for bands with most albums
        most_albums = st.checkbox("bands with most albums")

        # Display bands table
        bands_df = DBIO.get_bands(most_albums=most_albums)
        st.write(bands_df)

        # =============================================================================================
        # Bands with most song plays of a particular genre within a year
        # =============================================================================================
        st.subheader("Find out which bands were the most hit in a year for a particular genre:")
        genres_list = DBIO.get_genres()["genre"].tolist()
        genre = st.selectbox("Genre", genres_list)

        # currently our database has entries for these years only; ideally it should be a database
        # call to figure out what years to be presented in dropdown, but the call is trivial and
        # we have already fulfilled project requirements (SQL query + user interaction) in rest of
        # the app, therefore hard-coding values here
        year = st.selectbox("year", [2017, 2018, 2019, 2020])
        year = int(year)
        most_plays_df = DBIO.get_bands_with_most_song_plays(year=year, genre=genre)
        if most_plays_df.shape[0] == 0:
            st.write("No bands meet the given criteria!")
        else:
            st.subheader(f"Most played '{genre}' songs in year {year} belonged to the following bands:")
            st.write(most_plays_df)

    # Display code for Albums
    elif area == DBIO.Albums:
        st.subheader(DBIO.Albums)

        st.subheader("Find out which albums (with songs of a particular genre) were added the most"
                     " in user libraries since a given year")
        # genre dropdown
        genres_list = DBIO.get_genres()["genre"].tolist()
        genre = st.selectbox("Genre", genres_list)
        # year dropdown
        years_list = [2018, 2019, 2020, 2021]
        year = st.selectbox("Year", years_list)
        year = int(year)

        albums_df = DBIO.get_albums_most_featured_in_user_libraries(genre=genre, year=year)
        if albums_df.shape[0] == 0:
            st.write("No albums meet the given criteria!")
        else:
            st.subheader(f"Following albums (with some '{genre}' songs) were added the most by users"
                         f" in their personal libraries since {year}:")
            st.write(albums_df)


run()
