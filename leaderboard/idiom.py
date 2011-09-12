from .port import Leaderboard as PortLeaderboard, DEFAULT_PAGE_SIZE

class Leaderboard(object):
    def __init__(self, name, 
        page_size=DEFAULT_PAGE_SIZE, 
        redis=None,
        **redis_kwargs):
        self.name = "leaderboard:%s" % name

        self.port = PortLeaderboard(self.name,
            page_size, 
            redis,
            **redis_kwargs)

    
    def _conform_key(self, key):
        return str(key)
             
    def set_member_score(self, member, score):
        return self.port.rank_member(
            self._conform_key(member), 
            score
        )
    def remove_member(self, member):
        return self.port.remove_member(
            self._conform_key(member)
        )
    def total_members(self):
        return self.port.total_members()
    def total_pages(self):
        return self.port.total_pages()
    def incr(self, member, delta=1):
        return self.port.change_score_for(
            self._conform_key(member), 
            delta
        )
    def decr(self, member, delta=1):
        return self.incr(
            self._conform_key(member), 
            -1*delta
        )
    def get_rank_and_score(self, member):
        result = self.port.score_and_rank_for(
            self._conform_key(member)
        )
        return result['rank'], result['score']
    def leaders(self, page=1):
        return self.port.leaders(page)
