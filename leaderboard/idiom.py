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
     
    def set_member_score(self, member, score):
        return self.port.rank_member(member, score)
    def remove_member(self, member):
        return self.port.remove_member(member)
    def total_members(self):
        return self.port.total_members()
    def total_pages(self):
        return self.port.total_pages()
    def incr(self, member, delta=1):
        return self.port.change_score_for(member, delta)
    def decr(self, member, delta=1):
        return self.incr(member, -1*delta)
    def get_rank_and_score(self, member):
        result = self.port.score_and_rank_for(member)
        return result['rank'], result['score']
    def leaders(self, page=1):
        return self.port.leaders(page)
