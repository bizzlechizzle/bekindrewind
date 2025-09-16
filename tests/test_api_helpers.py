import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from scripts.api import (
    extract_description,
    extract_airdate,
    extract_network,
    extract_genre,
    extract_rating,
    extract_cast,
    extract_release,
    extract_studio,
    extract_images,
)


def test_extract_description_movie_prefers_tmdb_overview():
    result = extract_description(
        field='dmovie',
        tvdb_data={},
        tvmaze_data={},
        imdb_data={'Plot': 'N/A'},
        tmdb_data={'overview': 'TMDB overview'},
    )

    assert result == 'TMDB overview'


def test_extract_description_series_strips_html_summary():
    result = extract_description(
        field='dseries',
        tvdb_data={'overview': 'Short'},
        tvmaze_data={'summary': '<p>Summary with <b>HTML</b></p>'},
        imdb_data={'Plot': 'N/A'},
        tmdb_data={'overview': 'Another'},
    )

    assert result == 'Summary with HTML'


def test_extract_description_season_uses_tvmaze_summary():
    result = extract_description(
        field='dseason',
        tvdb_data={'overview': 'Short'},
        tvmaze_data={'summary': '<p>Detailed season summary</p>'},
        imdb_data={},
        tmdb_data={},
    )

    assert result == 'Detailed season summary'


def test_extract_airdate_prefers_tvdb_date():
    result = extract_airdate(
        field='airdate',
        tvdb_data={'firstAired': '2021-01-01'},
        tvmaze_data={'premiered': '2020-01-01'},
        tmdb_data={'first_air_date': '2019-01-01'},
    )

    assert result == '2021-01-01'


def test_extract_network_prefers_longest_name():
    result = extract_network(
        field='network',
        tvdb_data={'primaryNetwork': {'name': 'TVDB Network'}},
        tvmaze_data={'network': {'name': 'Maze'}, 'webChannel': {'name': 'Web'}},
        tmdb_data={'networks': [{'name': 'TMDB'}]},
    )

    assert result == 'TVDB Network'


def test_extract_genre_merges_unique_values():
    result = extract_genre(
        field='genre',
        tvdb_data={'genres': [{'name': 'Drama'}, {'name': 'Comedy'}]},
        tvmaze_data={'genres': ['Drama', 'Sci-Fi']},
        imdb_data={'Genre': 'Comedy, Action'},
        tmdb_data={'genres': [{'name': 'Action'}, {'name': 'Thriller'}]},
    )

    assert result == 'Drama, Comedy, Sci-Fi, Action, Thriller'


def test_extract_rating_prefers_informative_rating():
    result = extract_rating(
        field='rating',
        tvdb_data={'rating': '8.5'},
        imdb_data={'Rated': 'PG-13'},
    )

    assert result == 'PG-13'


def test_extract_cast_limits_to_five_names():
    result = extract_cast(
        field='cast',
        imdb_data={
            'Actors': 'Actor One, Actor Two, Actor Three, Actor Four, Actor Five, Actor Six'
        },
    )

    assert result == 'Actor One, Actor Two, Actor Three, Actor Four, Actor Five'


def test_extract_release_prefers_tmdb_date():
    result = extract_release(
        field='release',
        tmdb_data={'release_date': '2022-01-01'},
        imdb_data={'Released': '2021-01-01'},
    )

    assert result == '2022-01-01'


def test_extract_studio_returns_first_company():
    result = extract_studio(
        field='studio',
        tmdb_data={'production_companies': [{'name': 'Company A'}, {'name': 'Company B'}]},
    )

    assert result == 'Company A'


def test_extract_images_returns_existing_origin_image():
    existing = 'https://images.amazon.com/poster.jpg'
    result = extract_images(
        field='imovie',
        tvdb_data={},
        tvmaze_data={},
        tmdb_data={'poster_path': '/new.jpg'},
        existing_value=existing,
        source='Amazon',
    )

    assert result == existing


def test_extract_images_episode_uses_still_path():
    result = extract_images(
        field='iepisode',
        tvdb_data={'image': 'http://example.com/tvdb.jpg'},
        tvmaze_data={'image': {'original': 'http://example.com/tvmaze.jpg'}},
        tmdb_data={'still_path': '/still.jpg'},
    )

    assert result == 'https://image.tmdb.org/t/p/w500/still.jpg'


def test_extract_images_series_uses_poster_path():
    result = extract_images(
        field='iseries',
        tvdb_data={},
        tvmaze_data={},
        tmdb_data={'poster_path': '/poster.jpg'},
    )

    assert result == 'https://image.tmdb.org/t/p/w500/poster.jpg'
