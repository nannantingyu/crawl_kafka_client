# -*- coding: utf-8 -*-
from model.crawl_article import CrawlArticle
from model.crawl_economic_calendar import CrawlEconomicCalendar
from model.crawl_economic_event import CrawlEconomicEvent
from model.crawl_economic_holiday import CrawlEconomicHoliday
from Controller import Controller
import json, requests
import re

class Jin10calendarController(Controller):
    def __init__(self, topic="crawl_jin10_calendar"):
        super(Jin10calendarController, self).__init__(topic)
        self.post_sn_url = 'http://127.0.0.1:8361/api/content'
        # self.post_sn_url = 'http://www.9dfx.com/api/content'
        self.token = '9286168e06a110cd374caa0f67f08199'

        self.post_data = {
            'category': '财经日历',
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

    def run(self):
        for msg in self.consumer:
            data = json.loads(msg.value.decode('utf-8'))
            dtype= data['dtype'] if "dtype" in data else "calendar"

            del data['dtype']
            if dtype == 'calendar':
                self.parse_calendar(data)
            elif dtype == 'event':
                self.parse_event(data)
            elif dtype == 'holiday':
                self.parse_holiday(data)

    def parse_calendar(self, data):
        key_map = {
            'former_value': 'var1',
            'predicted_value': 'var2',
            'published_value': 'var3',
            'country': 'flag',
            'importance': 'star',
            'quota_name': 'title',
            'pub_time': 'show_time',
            'source_id': 'crawl_id',
            'influence': 'pk1'
        }

        post_data = self.get_post_data(key_map, data)
        self.handle_data(CrawlEconomicCalendar, post_data, data)

    def parse_event(self, data):
        key_map = {
            'importance': 'star',
            'event': 'detail',
            'city': 'city',
            'country': 'country',
            'time': 'show_time',
            'source_id': 'crawl_id'
        }

        post_data = self.get_post_data(key_map, data)
        post_data['category'] = '财经大事'
        self.handle_data(CrawlEconomicEvent, post_data, data)

    def parse_holiday(self, data):
        key_map = {
            'detail': 'description',
            'holiday_name': 'title',
            'market': 'market',
            'country': 'country',
            'date': 'show_time',
            'source_id': 'crawl_id'
        }

        post_data = self.get_post_data(key_map, data)
        post_data['category'] = '假期休市'

        self.handle_data(CrawlEconomicHoliday, post_data, data)

    def handle_data(self, model, post_data, data):
        with self.session_scope(self.sess) as session:
            model_obj = model(**data)
            query = session.query(model.id, model.fx_id).filter(
                model.source_id == model_obj.source_id
            ).one_or_none()

            if query is None:
                # try:
                session.add(model_obj)
                result = requests.post(self.post_sn_url, post_data)
                print "insert", result.content
                res = result.json()
                if 'errno' in res and res['errno'] == 0:
                    session.query(model).filter(
                        model.source_id == model_obj.source_id
                    ).update({'fx_id': res['data']['id']})
                # except Exception, e:
                #     print e
            else:
                post_data['id'] = query[1]
                result = requests.post(self.post_sn_url, post_data)
                session.query(model).filter(
                    model.id == query[0]
                ).update(data)

            print result.content

    def get_post_data(self, key_map, data):
        post_data = {}
        post_data.update(self.post_data)

        time_pat = re.compile(r"\d{4}\-\d{2}\-\d{2}(\s\d{2}:\d{2}:\d{2})?")
        for d in data:
            if d in key_map:
                post_data[key_map[d]] = data[d]

        if 'show_time' in post_data and len(time_pat.findall(post_data['show_time'])) == 0:
            post_data['show_time'] = None

        return post_data