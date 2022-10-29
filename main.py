import requests
import sqlite3


def initialize_db():
    """ Connects to our database, initializes sqlite, and gets our data ready to go
        :returns db_connection and db_cursor """
    response = requests.get('https://data.nasa.gov/resource/gh4g-9sfh.json')
    json_data = response.json()
    db_connection = None
    try:
        db_connection = sqlite3.connect('meteorite_db.db')
        db_cursor = db_connection.cursor()
        db_connection.commit()
        return db_connection, db_cursor, json_data
    except sqlite3.Error as db_error:  # error handling
        print(f'Error: {db_error}')
        if db_connection:
            db_connection.close()
        exit(1)


# Dictionary used for create_tables and sort_to_tables
bounding_boxes = {
    'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
    'Europe_Meteorites': (-24.1, 36, 32, 71.1),
    'Upper_Asia_Meteorites': (32.2, 35.8, 190.4, 72.7),
    'Lower_Asia_Meteorites': (58.2, -9.9, 154, 38.6),
    'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
    'North_America_Meteorites': (-168.2, 12.8, -52, 71.5),
    'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.6)
}


def create_tables(db_cursor: sqlite3.Cursor):
    """ Creates tables for out location sorted data and initializes bounding boxes for each table
        :param db_cursor, cursor initiated from initialize_db()"""
    for key in bounding_boxes.keys():
        db_cursor.execute(f'''CREATE TABLE IF NOT EXISTS {key}(
                                name TEXT,
                                mass TEXT,
                                reclat TEXT,
                                reclong TEXT);''')
        db_cursor.execute(f'DELETE FROM {key};')


def sort_to_tables(db_cursor: sqlite3.Cursor, db_data: list):
    """ Sorts through our data, sending it to it's desired location table
        :param db_cursor, cursor initiated from initialize_db(),
        :param db_data, tuple list of json data"""
    for entry in db_data:
        try:   # handles if reclat, reclong are unknown (None)
            entry['reclat']; entry['reclong']
        except KeyError:
            continue

        for key, bounding_box in bounding_boxes.items():
            if bounding_box[1] <= float(entry['reclat']) <= bounding_box[3] and \
                    bounding_box[0] <= float(entry['reclong']) <= bounding_box[2]:
                db_cursor.execute(f'INSERT INTO {key} VALUES(?, ?, ?, ?)',
                                  (entry['name'], entry.get('mass', None), entry['reclat'], entry['reclong']))


if __name__ == '__main__':
    connection, cursor, data = initialize_db()
    create_tables(cursor)
    connection.commit()
    sort_to_tables(cursor, data)
    connection.commit()
    cursor.close()
    connection.close()
