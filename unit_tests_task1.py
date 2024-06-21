import os
import unittest
from unittest.mock import patch, MagicMock, mock_open
from sql_connect import connection_to_database, load_json_files, upload_json_files, sql_query


class TestConnection(unittest.TestCase):

    @patch('sql_connect.create_engine')
    def test_connection_to_database_success(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value = None

        db_params = {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'database': 'test_db',
        }

        engine = connection_to_database(db_params)
        self.assertEqual(engine, mock_engine)
        mock_create_engine.assert_called_once_with('postgresql://test_user:test_password@localhost:5432/test_db')
        mock_engine.connect.assert_called_once()

    @patch('sql_connect.create_engine')
    def test_connection_to_database_failure(self, mock_create_engine):
        mock_create_engine.side_effect = Exception('Connection failed')

        db_params = {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'database': 'test_db',
        }

        with self.assertRaises(Exception):
            connection_to_database(db_params)


class TestJsonFileOperations(unittest.TestCase):

    @patch('os.listdir')
    def test_load_json_files(self, mock_listdir):
        mock_listdir.return_value = ['file1.json', 'file2.json', 'file3.txt']
        directory = '/fake_directory'

        result = load_json_files(directory)
        self.assertEqual(result, ['file1.json', 'file2.json'])
        mock_listdir.assert_called_once_with(directory)

    @patch('pandas.read_json')
    @patch('sql_connect.create_engine')
    def test_upload_json_files(self, mock_create_engine, mock_read_json):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_df = MagicMock()
        mock_read_json.return_value = mock_df

        json_files = ['file1.json', 'file2.json']
        directory = '/fake_directory'
        db_params = {
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': '5432',
            'database': 'test_db',
        }

        with patch('builtins.input', side_effect=['+']):
            upload_json_files(json_files, directory, db_params, mock_engine)
            mock_read_json.assert_any_call(os.path.join(directory, 'file1.json'))
            mock_read_json.assert_any_call(os.path.join(directory, 'file2.json'))
            mock_df.to_sql.assert_any_call(name='file1', con=mock_engine, if_exists='replace', index=False)
            mock_df.to_sql.assert_any_call(name='file2', con=mock_engine, if_exists='replace', index=False)


class TestSQLQuery(unittest.TestCase):

    @patch('pandas.read_sql_query')
    @patch('builtins.input')
    def test_sql_query(self, mock_input, mock_read_sql_query):
        mock_engine = MagicMock()
        mock_df = MagicMock()
        mock_read_sql_query.return_value = mock_df
        mock_input.side_effect = ['+', 'SELECT * FROM test_table', '', 'csv', 'test_file', '-']

        with patch('builtins.open', mock_open()):
            sql_query(mock_engine)
            mock_read_sql_query.assert_called_once_with('SELECT * FROM test_table ', mock_engine)
            mock_df.to_csv.assert_called_once_with('/home/user/Desktop/BigData/Results/test_file.csv', index=True)