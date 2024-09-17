import sqlite3
import random
import hashlib
import datetime
import Levenshtein
import json

def hashPassword(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

class Database:
    @staticmethod
    def get_all_videos_by_owner_id(OwnerId: str) -> list[dict]:
        videos = []
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime,id, TagsJSON FROM Videos WHERE OwnerId = ?', (OwnerId,))
            rows = cursor.fetchall()
            for row in rows:
                video = {
                    'Name': row[0],
                    'Path': row[1],
                    'ImagePath': row[2],
                    'Description': row[3],
                    'Owner': Database.get_user_data(row[4]), 
                    'DateTime':row[5],
                    'id':row[6],
                    'Tags': json.loads(row[7]) if row[7] else []
                }
                videos.append(video)
        return videos
    
    def get_user_favorite_tags(user_id: str) -> list[str]:
        tags = {}
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT VideoId FROM VideoWatches WHERE WatcherId = ?', (user_id,))
            WatchedVideos = []
            for row in cursor.fetchall():
                video = Database.get_video_by_id(row[0])
                if video:
                    WatchedVideos.append(video)
            for video in WatchedVideos:
                for tag in video['Tags']:
                    if tag in tags:
                        tags[tag] += 1
                    else:
                        tags[tag] = 1
            return tags
    
    def get_reccomended_videos_by_user_id(user_id: str, count: int) -> list[dict]:
        tags = Database.get_user_favorite_tags(user_id)
        videos = []
        with sqlite3.connect('database.db') as conn:
            for tag in tags:
                cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime, id, TagsJSON FROM Videos WHERE TagsJSON LIKE ?', (f'%{tags}%',))
                cursor = cursor.fetchall()
                if not cursor:
                    continue
                for row in cursor:
                    video = {
                        'Name': row[0],
                        'Path': row[1],
                        'ImagePath': row[2],
                        'Description': row[3],
                        'Owner':Database.get_user_data(row[4]), 
                        'DateTime':row[5],
                        'id':row[6],
                        'Tags': json.loads(row[7]) if row[7] else []
                    }
                    videos.append(video)
        while len(videos) < count:
            videos.append(Database.get_random_video())
        try:
            return random.sample(videos, count)
        except ValueError:
            return videos

    @staticmethod
    def get_video_reactions(VideoId: int) -> dict[str, int]:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('''SELECT 
                                SUM(CASE WHEN IsLike = 1 THEN 1 ELSE 0 END) AS LikesCount,
                                SUM(CASE WHEN IsLike = 0 THEN 1 ELSE 0 END) AS DislikesCount
                                FROM VideoReactions 
                                WHERE VideoId = ?''', (VideoId,))
            row = cursor.fetchone()
            if row:
                return {'Likes': row[0] if row[0] else 0, 'Dislikes':row[1] if row[1] else 0}
            return None
    
    @staticmethod
    def get_video_by_id(id: int) -> dict[str, str | int | datetime.datetime] | None:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime, id, TagsJSON FROM Videos WHERE id = ?', (id,))
            row = cursor.fetchone()
            if row:
                reactions = Database.get_video_reactions(row[6])
                if not reactions:
                    reactions = 0
                views = Database.get_video_watches(row[6])
                if not views:
                    views = 0
                try:
                    return {'id': row[6], 'Name':row[0], 'Path':row[1], 'ImagePath':row[2],'Description':row[3],'Owner':Database.get_user_data(row[4]), 'DateTime':row[5], 'Tags': json.loads(row[7]) if row[7] else [], 'Reactions':reactions, 'ViewCount':views}
                except json.JSONDecodeError:
                    return {'id': row[6], 'Name':row[0], 'Path':row[1], 'ImagePath':row[2],'Description':row[3],'Owner':Database.get_user_data(row[4]), 'DateTime':row[5], 'Tags': [], 'Reactions':reactions, 'ViewCount':views}
            return None
    
    @staticmethod
    def unreact_video(UserId: str, VideoId: int):
        with sqlite3.connect('database.db') as conn:
            conn.execute('DELETE FROM VideoReactions WHERE ReactorId = ? AND VideoId = ?', (UserId, VideoId,))

    @staticmethod
    def is_video_reacted(UserId: str, VideoId: int) -> bool:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Count() FROM VideoReactions WHERE ReactorId = ? AND VideoId = ?', (UserId, VideoId,))
            row = cursor.fetchone()
            return int(row[0]) == 1
    
    @staticmethod
    def react_video(UserId: str, VideoId: int, IsLike: int):
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO VideoReactions (VideoId, ReactorId, IsLike) VALUES (?, ?, ?)', (VideoId, UserId, IsLike,))

    @staticmethod
    def get_video_by_path(Path: str) -> dict[str, str | int | datetime.datetime] | None:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime, id FROM Videos WHERE Path = ?', (Path,))
            row = cursor.fetchone()
            try:
                if row:
                    return {'Name':row[0], 'Path':row[1], 'ImagePath':row[2],'Description':row[3],'Owner':Database.get_user_data(row[4]), 'DateTime':row[5], 'id':row[6]}
                return None
            except:
                return None
    
    @staticmethod
    def get_random_video() -> dict[str, str | int | datetime.datetime] | None:
        try:
            with sqlite3.connect('database.db') as conn:
                cursor = conn.execute('SELECT id FROM Videos ORDER BY RANDOM() LIMIT 1')
                row = cursor.fetchone()
                return Database.get_video_by_id(row[0])
        except ValueError:
            return None

    @staticmethod
    def get_video_watches(VideoId: int) -> int:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT COUNT() FROM VideoWatches Where VideoId = ?', (VideoId,))
            row = cursor.fetchone()
            return row[0]

    @staticmethod
    def add_video_watch(UserId: str, VideoId: int):
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO VideoWatches (WatcherId, VideoId) VALUES (?, ?)', (UserId, VideoId))

    @staticmethod
    def unreact_comment(UserId: str, CommentId: int):
        with sqlite3.connect('database.db') as conn:
            conn.execute('DELETE FROM CommentReactions WHERE ReactorId = ? AND CommentId = ?', (UserId, CommentId))

    @staticmethod
    def react_comment(UserId: str, CommentId: int, IsLike: bool):
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO CommentReactions (CommentId, ReactorId, IsLike) VALUES (?, ?, ?)', (CommentId, UserId, IsLike))

    @staticmethod
    def comment_reaction(UserId: str, CommentId: int):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT COUNT(), IsLike FROM CommentReactions Where CommentatorId = ? And CommentId = ?', (UserId, CommentId))
            row = cursor.fetchone()
            return {'IsReacted': row[0], 'IsLike': row[1]}

    @staticmethod
    def comment_video(UserId: str, Text: str, VideoId: int):
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO Comments (CommentatorId, Text, VideoId, DateTime) VALUES (?, ?, ?, ?)', (UserId, Text, VideoId, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    @staticmethod
    def get_all_comments(VideoId: int):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('''
                SELECT
                    CommentatorId,
                    VideoId,
                    Text,
                    DateTime
                FROM Comments 
                Where VideoId = ?  
            ''', (VideoId,))
            rows = cursor.fetchall()
            comments = []
            for row in rows:
                comment = {
                    'Commentator': Database.get_user_data(row[0]),
                    'Video': row[1],
                    'Text': row[2],
                    'DateTime': row[3]
                }
                comments.append(comment)
            return comments
    @staticmethod
    def delete_video(UserId: str, VideoId: int) -> dict:
        with sqlite3.connect('database.db') as conn:
            conn.execute('DELETE FROM Videos WHERE OwnerId = ? AND id = ?', (UserId, VideoId,))
        return {'success': True}
    @staticmethod
    def update_profile(Login: str, NewDescription: str, NewName: str) -> None:
        with sqlite3.connect('database.db') as conn:
            conn.execute("UPDATE Users SET Description = ?, Name = ? where Login = ? ", (NewDescription, NewName, Login))

    @staticmethod
    def add_video(Name: str, Path: str, Description: str, OwnerLogin: str, Tags: list) -> None:
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO Videos (Name, Path, ImagePath, Description, OwnerId, DateTime, TagsJSON) VALUES (?, ?, ?, ?, ?, ?, ?)', (Name, Path+'.mp4',Path+'.png', Description, OwnerLogin, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), json.dumps(Tags.split(',')) if Tags else None))

    @staticmethod
    def get_user_data(UserId: str):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Login, Name, Description, PfpPath FROM Users WHERE Login = ?', (UserId,))
            row = cursor.fetchone()
            if row:
                return {'Login':row[0], 'Name':row[1], 'Description':row[2], 'PfpPath':row[3]}
            return None

    @staticmethod
    def login_user(Login: str, Password: str):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Login FROM Users WHERE Login = ? and Password = ?', (Login, Password))
            row = cursor.fetchone()
            if row:
                return row[0]
            return None

    @staticmethod
    def redact_video(VideoId: int, Name: str, Path: str, Description: str, Tags: list, OwnerLogin: str) -> None:
        with sqlite3.connect('database.db') as conn:
            conn.execute('UPDATE Videos SET Name = ?, Description = ?, TagsJSON = ? WHERE VideoId = ? and OwnerId = ?', (Name, Path+'.mp4', Description, json.dumps(Tags).replace("\\", '') if type(Tags) == list else Tags.replace('\\', ''), VideoId, OwnerLogin))
        return True
    
    @staticmethod
    def reg_user(Login: str, Password: str, Nickname: str) -> None:
        with sqlite3.connect('database.db') as conn:
            conn.execute('INSERT INTO Users (Login, Password, Name, PfpPath) VALUES (?, ?, ?, ?)', (Login, Password, Nickname, Login+'.png'))

    @staticmethod
    def get_video_comments(videoid: int):
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT * FROM Comments WHERE VideoId = ?', (videoid,))
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    return {
                        'id': row[0],
                        'CommentatorId': row[1],
                        'VideoId': row[2],
                        'Text': row[3],
                        'DateTime': row[4]
                    }
            return None
    
    @staticmethod
    def search_in_database_slow(text:str, distance: int = 20) -> list:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime, id, TagsJSON FROM Videos')
            rows = cursor.fetchall()
            videos = []
            for row in rows:
                video = {
                    'Name': row[0],
                    'Path': row[1],
                    'ImagePath': row[2],
                    'Description': row[3],
                    'Owner': Database.get_user_data(row[4]),
                    'DateTime': row[5],
                    'id': row[6]
                }
                videos.append(video)
            cursor = conn.execute('SELECT Login, Name, Description, PfpPath FROM Users')
            rows = cursor.fetchall()
            channels = []
            for row in rows:
                channel = {
                    'Name': row[1],
                    'Login': row[0],
                    'Description': row[2],
                    'PfpPath': row[3]
                }
                channels.append(channel)
            filtered_videos = [video for video in videos if Levenshtein.distance(video['Name'], text) < distance or text in video['Description']]
            filtered_channels = [channel for channel in channels if Levenshtein.distance(channel['Name'], text) < distance or text in channel['Description']]
            filtered_videos.sort(key=lambda video: Levenshtein.distance(video['Name'], text))
            filtered_channels.sort(key=lambda channel: Levenshtein.distance(channel['Name'], text))
            outputvideos = []
            for i in filtered_videos:
                outputvideos.append(Database.get_video_by_id(i['id']))
            return {'videos': outputvideos, 'channels': filtered_channels}
    
    @staticmethod
    def search_in_database_fast(text:str) -> list:
        with sqlite3.connect('database.db') as conn:
            cursor = conn.execute('SELECT Name, Path, ImagePath, Description, OwnerId, DateTime, id, TagsJSON FROM Videos WHERE Name LIKE ?', (f'%{text}%',))
            rows = cursor.fetchall()
            videos = []
            for row in rows:
                video = {
                    'Name': row[0],
                    'Path': row[1],
                    'ImagePath': row[2],
                    'Description': row[3],
                    'Owner': Database.get_user_data(row[4]),
                    'DateTime': row[5],
                    'id': row[6],
                    'Tags': json.loads(row[7]) if row[7] else []
                }
                videos.append(video)
            cursor = conn.execute('SELECT Login, Name, Description, PfpPath, TagsJSON FROM Users WHERE Name LIKE ?', (f'%{text}%',))
            rows = cursor.fetchall()
            channels = []
            for row in rows:
                channels.append({
                    'Name': row[1],
                    'Login': row[0],
                    'Description': row[2],
                    'PfpPath': row[3]
                })
            output = {'videos':videos, 'channels':channels}
            return output
    
    
    @staticmethod
    def start_db() -> None:
        with sqlite3.connect('database.db') as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS Users (
                    Login TEXT NOT NULL PRIMARY KEY,
                    Password TEXT NOT NULL,
                    Name TEXT NOT NULL,
                    Description TEXT,
                    PfpPath TEXT NOT NULL
                )
                ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS Videos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL,
                    Path TEXT NOT NULL,
                    ImagePath TEXT NOT NULL,
                    Description TEXT NOT NULL,
                    OwnerId TEXT NOT NULL,
                    DateTime DATETIME NOT NULL,
                    TagsJSON TEXT,
                    FOREIGN KEY (OwnerId) REFERENCES Users (Login)
                )
                ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS VideoReactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    VideoId INTEGER NOT NULL,
                    ReactorId TEXT NOT NULL,
                    IsLike INTEGER NOT NULL,
                    FOREIGN KEY (VideoId) REFERENCES Videos (id),
                    FOREIGN KEY (ReactorId) REFERENCES Users (Login)
                )
                ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS VideoWatches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    WatcherId TEXT,
                    VideoId INTEGER NOT NULL,
                    FOREIGN KEY (WatcherId) REFERENCES Users (Login)
                    FOREIGN KEY (VideoId) REFERENCES Videos (id)
                )
                ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS Comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    CommentatorId TEXT NOT NULL,
                    VideoId INTEGER NOT NULL,
                    Text TEXT NOT NULL,
                    DateTime DATETIME NOT NULL,
                    FOREIGN KEY (CommentatorId) REFERENCES Users (Login)
                )
                ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS CommentReactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    CommentId INTEGER NOT NULL,
                    ReactorId TEXT NOT NULL,
                    IsLike INTEGER NOT NULL,
                    FOREIGN KEY (CommentId) REFERENCES Comments (id),
                    FOREIGN KEY (ReactorId) REFERENCES Users (Login)
                )
                ''')
Database.start_db()
