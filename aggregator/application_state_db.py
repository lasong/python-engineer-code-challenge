import sqlite3
import os
import json

class ApplicationStateDB:
    def __init__(self) -> None:
        self.connection = sqlite3.connect(os.path.join('db', 'application_state.db'))
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            '''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                topic TEXT,
                data TEXT,
                UNIQUE(topic)
            )
            '''
        )

    def add_message(self, topic: str, message: dict):
        self.cursor.execute(
            '''
            INSERT INTO messages (topic, data)
            VALUES (?, ?)
            ON CONFLICT(topic)
            DO UPDATE SET data = excluded.data
            ''',
            (topic, json.dumps(message, default=str).encode('utf-8'))
        )
        self.connection.commit()

    def fetch_last_message(self, topic: str):
        self.cursor.execute(
            'SELECT data FROM messages WHERE topic = ? LIMIT 1',
            (topic,)
        )
        self.connection.commit()
        result = self.cursor.fetchone()

        if not result:
            return None

        return json.loads(result[0].decode('utf-8'))

    def close(self):
        self.connection.close()

    def rollback(self):
        self.connection.rollback()
