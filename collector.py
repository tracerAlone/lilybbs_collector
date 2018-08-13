import logging
import sys
import time
import re
import requests
import traceback
from dateutil.parser import parse as parse_time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

import database

DOMAIN = 'http://bbs.nju.edu.cn/'
ENTRY_URL = DOMAIN + 'bbsall'
POST_FORMAT = re.compile(
    r'发信人: (\S+) .+\n'
    r'标\s+题: (.+)\n'
    r'发信站: \S+ \(([a-zA-Z0-9: ]+)\)',
    re.M)


def open_page(url):
    while True:
        try:
            resp = requests.get(url)
            # 小百合一个大坑，网页head里说是gb2312编码，而内容实则会有其编码之外的字符
            # 干脆统一改成gb18030编码，反正这个兼容前者
            encoding = 'gb18030' if resp.encoding == 'gb2312' else resp.encoding
            text = resp.content.decode(encoding, errors='ignore')
            return BeautifulSoup(text, 'lxml')
        except requests.exceptions.ConnectionError:
            time.sleep(0.5)


def generate_board_url(board_name, t_mode=True, start_from=None):
    url = DOMAIN + 'bbs%sdoc?board=%s' % ('t' if t_mode else '', board_name)
    if start_from is None:
        return url
    else:
        return url + '&start=%d' % start_from


def doc_urls_of_board(board_name):
    start_from = 0
    while True:
        list_page_url = generate_board_url(board_name, True, start_from)
        try:
            soup = open_page(list_page_url)
            for tr in soup.center.table.find_all('tr')[1:]:
                start_from += 1
                doc_entry = tr.find_all('td')[4]
                doc_url = urljoin(list_page_url, doc_entry.a['href'])
                logging.info('发现帖子：%s\n%s' % (doc_entry.text, doc_url))
                yield doc_url
            if not soup.center.find_all('a', text='下一页'):
                break
        except Exception:
            logging.error('无法获取“%s”讨论区的帖子列表，于页面：%s\n%s' %
                          (board_name, list_page_url, traceback.format_exc()))
            break


def docs_of_board(board_name):
    logging.info('开始遍历“%s”讨论区的帖子' % board_name)
    for url in doc_urls_of_board(board_name):
        try:
            texts = [post.textarea.text.replace('\r\n', '\n') for post in
                     open_page(url).find_all('table', attrs={'class': 'main'})]
        except Exception:
            logging.error('获取帖子页面失败：\n%s\n%s' %
                          (url, traceback.format_exc()))
            continue
        if not len(texts):
            logging.error('帖子为空：\n%s' % url)
            continue

        content = '\n'.join(texts)

        create_info = POST_FORMAT.search(texts[0])
        update_info = POST_FORMAT.search(texts[-1])
        if create_info and update_info:
            creator = create_info.group(1)
            title = create_info.group(2)
            create_time = int(parse_time(create_info.group(3)).timestamp())
            update_time = int(parse_time(update_info.group(3)).timestamp())
            end = create_info.end()
            logging.info('帖子解析成功：\n' +
                         content[end:end + 256].replace('\n', ' '))
        else:
            creator = title = ''
            update_time = create_time = 0
            logging.error('无法完全解析的帖子格式：\n' + url)

        result = (title, creator, create_time, update_time, url, content)
        yield result
    logging.info('“%s”讨论区的帖子已经全部遍历完成' % board_name)


if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    formatter = logging.Formatter('>>> %(levelname)s %(asctime)s\n%(message)s')

    logging_targets = ((logging.INFO, logging.StreamHandler(sys.stdout)),
                      (logging.INFO, logging.FileHandler('info.log')),
                      (logging.WARNING, logging.StreamHandler(sys.stderr)),
                      (logging.WARNING, logging.FileHandler('error.log')))
    for target in logging_targets:
        target[1].setLevel(target[0])
        target[1].setFormatter(formatter)
        logger.addHandler(target[1])

    boards = []
    for tr in open_page(ENTRY_URL).center.table.find_all('tr')[1:]:
        td_list = tr.find_all('td')
        name = td_list[1].text
        category = td_list[2].text[1:-1]
        description = td_list[3].text[2:]
        boards.append((name, category, description))

    database.save_boards(boards)
    for board in boards:
        database.save_board_docs(board[0], docs_of_board(board[0]))
