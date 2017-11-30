# -*- coding: utf-8 -*-
from model.crawl_article import CrawlArticle
from kafka import KafkaConsumer
from Controller import Controller
import json, re, requests, logging

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='logs/article.log',
                filemode='w')

class ArticleController(Controller):
    def __init__(self, topic="crawl_article"):
        super(ArticleController, self).__init__(topic)
        # self.post_sn_url = 'http://www.9dfx.com/api/content'

        self.post_data = {
            'category': '市场参考',
            'title': '',
            'content': '',
            'keyname': '',
            'description': '',
            'name': '',
            'status': 1,
            'link_id': '',
            'vip': '',
            'tpl': 1,
            'todo': 0,
            'flag': '',
            'var1': '',
            'var2': '',
            'var3': '',
            'pk1': '',
            'star': '',
            'token': self.token
        }

        self.update_data = {
            'category': '市场参考',
            'token': self.token,
            'status': 1,
            'tpl': 1,
            'todo': 0,
        }

    def run(self):
        for msg in self.consumer:
            try:
                data = json.loads(msg.value.decode('utf-8'))
                dtype= data['dtype'] if "dtype" in data else "insert"
                key = data['key'] if "key" in data else "source_id"

                del data['dtype']
                if "key" in data:
                    del data['key']

                article = CrawlArticle(**data)

                with self.session_scope(self.sess) as session:
                    if dtype == 'insert':
                        query = session.query(CrawlArticle.id).filter(
                            CrawlArticle.source_id == article.source_id
                        ).one_or_none()

                        if query is None:
                            session.add(article)
                            post_data = self.get_post_data(data)
                            result = requests.post(self.post_sn_url, post_data)
                            print "insert", result.content
                            res = result.json()
                            if 'errno' in res and res['errno'] == 0:
                                session.query(CrawlArticle).filter(
                                    CrawlArticle.source_id == article.source_id
                                ).update({'fx_id': res['data']['id']})
                        else:
                            query = session.query(CrawlArticle.id, CrawlArticle.fx_id).filter(
                                getattr(CrawlArticle, key) == getattr(article, key)
                            ).one_or_none()
                            if query:
                                post_data = self.get_post_data(data, True)
                                post_data['id'] = query[1]
                                result = requests.post(self.post_sn_url, post_data)
                                session.query(CrawlArticle).filter(
                                    CrawlArticle.id == query[0]
                                ).update(data)
                    else:
                        if hasattr(CrawlArticle, key):
                            query = session.query(CrawlArticle.id, CrawlArticle.fx_id).filter(
                                getattr(CrawlArticle, key) == getattr(article, key)
                            ).one_or_none()

                            if query:
                                post_data = self.get_post_data(data, True)
                                post_data['id'] = query[1]
                                result = requests.post(self.post_sn_url, post_data)
                                session.query(CrawlArticle).filter(
                                    CrawlArticle.id == query[0]
                                ).update(data)
            except Exception as e:
                logging.error(e)


    def get_post_data(self, data, update=False):
        key_map = {
            'title': 'title',
            'description': 'description',
            'body': 'content',
            'publish_time': 'show_time',
            'image': 'cover',
            'type': 'group',
            'crawl_id': 'source_id',
            'keywords': 'keyname'
        }

        post_data = {}
        post_data.update(self.post_data) if not update else post_data.update(self.update_data)

        time_pat = re.compile(r"\d{4}\-\d{2}\-\d{2}(\s\d{2}:\d{2}:\d{2})?")
        for d in data:
            if d in key_map:
                post_data[key_map[d]] = data[d]

        if 'show_time' in post_data and len(time_pat.findall(post_data['show_time'])) == 0:
            post_data['show_time'] = None

        # if 'group_id' in post_data:
        #     group_id = post_data['group_id'].encode('utf-8')
        #     print group_id.decode('utf-8').encode('gbk')
        #     if group_id in self.cat_map:
        #         print "inininininininini\n\n\n", self.cat_map[group_id]
        #         post_data['group_id'] = self.cat_map[group_id]

        return post_data