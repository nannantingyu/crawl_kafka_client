# -*- coding: utf-8 -*-
from model.crawl_article import CrawlArticle
from model.crawl_fx678_economic_jiedu import CrawlFx678EconomicJiedu
from Controller import Controller
import json, requests, re, logging

logging.basicConfig(level=logging.INFO,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename='logs/fx678_jiedu.log',
                filemode='w')

class Fx678jieduController(Controller):
    def __init__(self, topic="crawl_fx678_calendar_jiedu"):
        super(Fx678jieduController, self).__init__(topic)

    def run(self):
        for msg in self.consumer:
            data = json.loads(msg.value.decode('utf-8'))

            with self.session_scope(self.sess) as session:
                jiedu = CrawlFx678EconomicJiedu(**data)
                query = session.query(CrawlFx678EconomicJiedu.dataname_id).filter(
                    CrawlFx678EconomicJiedu.dataname_id == jiedu.dataname_id
                ).one_or_none()

                if query is None:
                    session.add(jiedu)
                else:
                    session.query(CrawlFx678EconomicJiedu).filter(
                        CrawlFx678EconomicJiedu.dataname_id == query[0]
                    ).update(data)