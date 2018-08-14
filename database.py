import logging
import sqlite3
import traceback

DATA_BASE = 'lilybbs.db'
INSERT_COMMIT_LIMIT = 64
CONTINUAL_EXCEPTION_LIMIT = 8


def renew_boards(boards):
    conn = sqlite3.connect(DATA_BASE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS boards (
  name        TEXT PRIMARY KEY,
  category    TEXT,
  description TEXT
);''')
    logging.info('已为讨论区列表创建数据表')

    old_boards = set(x[0] for x in c.execute("SELECT name FROM boards"))
    new_boards = set(x[0] for x in boards)
    c.execute('DELETE FROM boards')
    c.executemany('INSERT INTO boards VALUES (?,?,?)', boards)

    removed_boards = old_boards - new_boards
    if removed_boards:
        logging.error('发现有讨论区已经被移除：' + str(sorted(removed_boards)))

    for added_board in new_boards - old_boards:
        c.execute('''CREATE TABLE IF NOT EXISTS board_%s (
  title       TEXT,
  creator     TEXT,
  create_time INT,
  update_time INT,
  url         TEXT PRIMARY KEY,
  all_text    TEXT
);''' % added_board)
        logging.info('新增了讨论区：' + added_board)

    conn.commit()
    conn.close()


def save_board_docs(board_name, docs):
    conn = sqlite3.connect(DATA_BASE)
    c = conn.cursor()
    sql = 'INSERT OR REPLACE INTO board_%s VALUES (?,?,?,?,?,?)' % board_name
    try:
        insert_counter = 0
        exception_count = 0
        for doc in docs:
            try:
                c.execute(sql, doc)
                insert_counter = (insert_counter + 1) % INSERT_COMMIT_LIMIT
                if insert_counter == 0:
                    conn.commit()
                logging.info('保存到帖子成功')
                exception_count = 0
            except Exception:
                exception_count += 1
                logging.error('保存帖子到数据库时出错：\n%s\n%s' %
                              (doc[3], traceback.format_exc()))
                if exception_count >= CONTINUAL_EXCEPTION_LIMIT:
                    raise
    except:
        logging.error('意外退出数据库\n' + traceback.format_exc())
        raise
    finally:
        conn.commit()
        conn.close()
        logging.info('已关闭数据库连接')