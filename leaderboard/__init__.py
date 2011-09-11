# -*- coding: utf-8 -*-
"""
  Differences from ruby upstream:
    Constants hanging off the ruby class are now in the package.
    Things which hit the network are methods.
    Things which don't (and take no args) are properties.
    Where raw positional booleans were passed in, kwargs are used instead, i.e. 
      use_zero_index_for_rank=True
    The return value in ruby is a raw hash. In python, they 
        *should* be (not yet) objs or named tuples by default.
    Return hash values in ruby are strings; In python, they are native types
        (float/decimal/int).
    *Should* Adds incr/decr funcs to wrap oddly named change_score_for.
    check_member?/check_member_in? -> check_member/check_member_in
    merge_leaderboards is a pretty terrible API.
      It *should* be module.merge_leaderboards(destination, [src,...])
    rank_member is an odd name for a think which adds a member and 
        assigns a score.
    *should* add a convenience func to close all LB-owned redis conns.
"""
from __future__ import division
import math

from functools import wraps
from anyjson import loads, dumps
from redis import Redis, ConnectionPool


VERSION = (2, 0, 0, 'alpha')

def get_version():
    return ".".join(map(str, VERSION[:3]))

DEFAULT_PAGE_SIZE = 25

DEFAULT_REDIS_HOST = 'localhost'
DEFAULT_REDIS_PORT = 6379

DEFAULT_LEADERBOARD_REQUEST_OPTIONS = {
    'with_scores': True, 
    'with_rank': True, 
    'page_size': None
}

# FIXME: fix connection lifecycle
# FIXME: use redis connection pool.
CONN_POOL = None

def teardown():
    global CONN_POOL
    if CONN_POOL is None:
        return
    CONN_POOL.disconnect()
    CONN_POOL = None

class Leaderboard(object):
    def __init__(self, name, 
        page_size=DEFAULT_PAGE_SIZE, 
        redis=None,
        **redis_kwargs):


        # TODO: allow pool to be sent in.
        global CONN_POOL
        if redis is None:
            if CONN_POOL is None:
                CONN_POOL = ConnectionPool(**redis_kwargs)
            self.redis = Redis(connection_pool=CONN_POOL)
        else:
            self.redis = redis

        self.name = name
        if page_size < 1:
            self._page_size = DEFAULT_PAGE_SIZE
        else:
            self._page_size = page_size

    def _get_page_size(self):
        return self._page_size
    def _set_page_size(self, value):
        if value < 1:
            self._page_size = DEFAULT_PAGE_SIZE
        else:
            self._page_size = value
    page_size = property(_get_page_size, _set_page_size)

    def delete_leaderboard(self):
        self.delete_leaderboard_named(self.name)
    def delete_leaderboard_named(self, name):
        self.redis.delete(name)

    def rank_member(self, member, score):
        self.rank_member_in(self.name, member, score)
    def rank_member_in(self, name, member, score):
        self.redis.zadd(name, **{member: score})

    def remove_member(self, member):
        self.remove_member_from(self.name, member)
    def remove_member_from(self, name, member):
        self.redis.zrem(name, member)

    def total_members(self):
        return self.total_members_in(self.name)
    def total_members_in(self, name):
        return self.redis.zcard(name)
  
    def total_pages(self):
        return self.total_pages_in(self.name)
    def total_pages_in(self, name, page_size=None):
        if page_size is None:
            page_size = self.page_size
  
        return int(math.ceil(
            self.total_members_in(name) / 
            page_size
        ))
  
    def total_members_in_score_range(self, min_score, max_score):
        return self.total_members_in_score_range_in(self.name, 
            min_score, 
            max_score)
    def total_members_in_score_range_in(self, name, min_score, max_score):
        return self.redis.zcount(name, 
            min_score, 
            max_score)
  
    def change_score_for(self, member, delta):
        return self.change_score_for_member_in(self.name,
            member,
            delta)
  
    def change_score_for_member_in(self, name, member, delta):
        return self.redis.zincrby(name, member, delta)
  
    def _conform_rank(self, rank, use_zero_index_for_rank):
        if rank is None or use_zero_index_for_rank:
            return rank
        return rank + 1

    def rank_for(self, member, use_zero_index_for_rank=False):
        return self.rank_for_in(self.name, 
            member, 
            use_zero_index_for_rank=use_zero_index_for_rank)
    def rank_for_in(self, name, member, use_zero_index_for_rank=False):
        rank = self.redis.zrevrank(name, member)
        return self._conform_rank(rank, use_zero_index_for_rank)
  
    def score_for(self, member):
        return self.score_for_in(self.name, member)
    def score_for_in(self, name, member):
        return self.redis.zscore(name, member)

    def check_member(self, member):
        return self.check_member_in(self.name, member)
    def check_member_in(self, name, member):
        return self.redis.zscore(name, member) is not None

    def score_and_rank_for(self, member, use_zero_index_for_rank=False):
        return self.score_and_rank_for_in(self.name, 
            member, 
            use_zero_index_for_rank=use_zero_index_for_rank)
    def score_and_rank_for_in(self, name, member, use_zero_index_for_rank=False):
        ret = {'member': member}
        with self.redis.pipeline() as pipe:
            pipe.zscore(name, member)
            pipe.zrevrank(name, member)
            ret['score'], ret['rank'] = pipe.execute()
            
        ret['rank'] = self._conform_rank(ret['rank'], use_zero_index_for_rank)
        return ret

    def remove_members_in_score_range(self, min_score, max_score):
        return self.remove_members_in_score_range_in(self.name, 
            min_score, 
            max_score)
    def remove_members_in_score_range_in(self, name, min_score, max_score):
        return self.redis.zremrangebyscore(name, 
            min_score, 
            max_score)

    def _conform_page_size(self, **kwargs):
        page_size = kwargs.get('page_size', self.page_size)
        if page_size and page_size < 1:
          page_size = DEFAULT_PAGE_SIZE
        return page_size

    def leaders(self, current_page, 
        **kwargs):
        return self.leaders_in(self.name, current_page, **kwargs)
    def leaders_in(self, name, current_page=None, 
        **kwargs):

        if current_page < 1:
          current_page = 1

        page_size = self._conform_page_size(**kwargs)
    
        total_pages = self.total_pages_in(name, page_size)
        if current_page > total_pages:
            current_page = total_pages
    
        index_for_redis = current_page - 1

        starting_offset = (index_for_redis * page_size)
        if starting_offset < 0:
          starting_offset = 0

        ending_offset = (starting_offset + page_size) - 1
        
        raw_leader_data = self.redis.zrevrange(name, 
            starting_offset, 
            ending_offset, 
            False)
        if not raw_leader_data:
            return []

        return self.ranked_in_list_in(name, 
            raw_leader_data, **kwargs)
  
    def around_me(self, member, **kwargs):
        return self.around_me_in(self.name, member, **kwargs)
    def around_me_in(self, name, member, **kwargs):
        reverse_rank_for_member = self.redis.zrevrank(name, member)
    
        page_size = self._conform_page_size(**kwargs)
    
        starting_offset = reverse_rank_for_member - int(page_size / 2)
        if starting_offset < 0:
            starting_offset = 0

        ending_offset = (starting_offset + page_size) - 1
    
        raw_leader_data = self.redis.zrevrange(name, 
            starting_offset, 
            ending_offset, 
            False)
        if not raw_leader_data:
            return []
        return self.ranked_in_list_in(name, 
            raw_leader_data, 
            **kwargs)
  
    def ranked_in_list(self, members, **kwargs):
        return self.ranked_in_list_in(self.name, members, **kwargs)
    def ranked_in_list_in(self, name, members, **kwargs):
        with_rank = kwargs.get('with_rank',
            DEFAULT_LEADERBOARD_REQUEST_OPTIONS['with_rank'])
        with_scores = kwargs.get('with_scores',
            DEFAULT_LEADERBOARD_REQUEST_OPTIONS['with_scores'])
        use_zero_index_for_rank = kwargs.get('use_zero_index_for_rank',
            False)
        results = [{'member': member} for member in members]
        if not (with_rank or with_scores):
            return results

        responses = []
        if with_rank and with_scores:
            step = 2
            def process_result(i, rank, score):
                results[i]['rank'] = self._conform_rank(rank, 
                    use_zero_index_for_rank)
                results[i]['score'] = score
        elif with_rank:
            step = 1
            def process_result(i, rank):
                results[i]['rank'] = self._conform_rank(rank, 
                    use_zero_index_for_rank)
        else: # with_score
            step = 1
            def process_result(i, score):
                results[i]['score'] = score

        with self.redis.pipeline() as pipe:
            for member in members:
                if with_rank:
                    pipe.zrevrank(name, member)
                if with_scores:
                    pipe.zscore(name, member)
            responses = pipe.execute()
            for i in range(len(results)):
                process_result(i, *responses[i*step:(i+1)*step])
        return results
    
    # Merge leaderboards given by keys with this leaderboard into destination
    def merge_leaderboards(self, destination, keys, aggregate="sum"):
        return self.redis.zunionstore(destination, 
            keys + [self.name], aggregate)
  
      # Intersect leaderboards given by keys with this leaderboard into destination
    def intersect_leaderboards(self, destination, keys, aggregate="sum"):
        return self.redis.zinterstore(destination, 
            keys + [self.name], aggregate)
