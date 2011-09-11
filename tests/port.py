import unittest

import redis

import leaderboard.port as lb

"""
todo:
 pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
 
 make opts .update

 make module-scoped key prefix


"""

class TestLeaderboard(unittest.TestCase):
    def setUp(self):
        self.leaderboard = lb.Leaderboard("name")
        self.conn = redis.Redis()
    
    def tearDown(self):
        self.conn.flushdb()
        lb.teardown()
        self.conn = None
    
    def _rank_members_in_leaderboard(self, members_to_add=5):
        for i in range(1, members_to_add+1):
            self.leaderboard.rank_member("member_%d" % i, i)

    def test_version(self):
        self.assertEqual('2.0.0', lb.get_version())

    def test_initialize_with_defaults(self):
        self.assertEqual('name', self.leaderboard.name)
        self.assertEqual(lb.DEFAULT_PAGE_SIZE, self.leaderboard.page_size)
  
    def test_disconnect(self):
        # FIXME: what is assert nil really doing in ruby?
        lb.teardown()
  
    def test_will_automatically_reconnect_after_a_disconnect(self):
        self.assertEqual(0, self.leaderboard.total_members())
        self._rank_members_in_leaderboard(5)
        self.assertEqual(5, self.leaderboard.total_members())
        lb.teardown()
        self.assertEqual(5, self.leaderboard.total_members())
  
    def test_page_size_is_default_page_size_if_set_to_invalid_value(self):
        # FIXME: shouldn't this raise instead?
        some_leaderboard = lb.Leaderboard('name', page_size=0)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE,
            some_leaderboard.page_size)
  
    def test_delete_leaderboard(self):
        self._rank_members_in_leaderboard()
    
        self.assertEqual(True, self.conn.exists('name'))

        self.leaderboard.delete_leaderboard()
        self.assertEqual(False, self.conn.exists('name'))
  
    def test_can_pass_existing_redis_connection_to_initializer(self):
        self.leaderboard = lb.Leaderboard('name', redis=self.conn)
    
        self._rank_members_in_leaderboard()

        self.assertEqual(1, self.conn.info()['connected_clients'])
  
    def test_rank_member_and_total_members(self):
        self.leaderboard.rank_member('member', 1)

        self.assertEqual(1, self.leaderboard.total_members())
  
    def test_total_members_in_score_range(self):
        self._rank_members_in_leaderboard(5)
    
        self.assertEqual(3, self.leaderboard.total_members_in_score_range(2, 4))
  
    def test_rank_for(self):
        self._rank_members_in_leaderboard(5)

        self.assertEqual(2, self.leaderboard.rank_for('member_4'))
        self.assertEqual(1, self.leaderboard.rank_for('member_4', 
            use_zero_index_for_rank=True))
        # TODO: what should the ret value be for bad member?
  
    def test_score_for(self):
        self._rank_members_in_leaderboard(5)
        self.assertEqual(4, self.leaderboard.score_for('member_4'))
  
    def test_total_pages(self):
        self.assertTrue(10 < lb.DEFAULT_PAGE_SIZE)

        self._rank_members_in_leaderboard(10)

        self.assertEqual(1, self.leaderboard.total_pages())

        self.conn.flushdb()

        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE + 1)

        self.assertEqual(2, self.leaderboard.total_pages())
  
    def test_leaders(self):
        self._rank_members_in_leaderboard(25)

        self.assertEqual(25, self.leaderboard.total_members())

        # FIXME: test that .leaders() (no param) returns the first page.
        leaders = self.leaderboard.leaders(1)
            
        self.assertEqual(25, len(leaders))
        self.assertEqual('member_25', leaders[0]['member'])
        self.assertEqual('member_2', leaders[-2]['member'])
        self.assertEqual('member_1', leaders[-1]['member'])
        self.assertEqual(1, leaders[-1]['score'])
  
    def test_leaders_with_multiple_pages(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE * 3 + 1)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE * 3 + 1, 
            self.leaderboard.total_members())

        for i in range(1,4):
            leaders = self.leaderboard.leaders(i)
            self.assertEqual(self.leaderboard.page_size, 
                len(leaders))

        leaders = self.leaderboard.leaders(4)
        self.assertEqual(1, len(leaders))

        # FIXME: seems like negative paging should either be an error or wrap.
        #  in upstream, they go to page 1.
        leaders = self.leaderboard.leaders(-5)
        self.assertEqual(self.leaderboard.page_size, 
            len(leaders))

        # FIXME: What the heck is going on here.  4 pages, checking 10th, 
        #  and getting *1* back?!
        leaders = self.leaderboard.leaders(10)
        self.assertEqual(1, 
            len(leaders))
  
    def test_leaders_without_retrieving_scores_and_ranks(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE,
            self.leaderboard.total_members())

        leaders = self.leaderboard.leaders(1, 
            with_scores=False, 
            with_rank=False)

        member_25 = {'member': 'member_25'}
        self.assertEqual(member_25, leaders[0])

        member_1 = {'member': 'member_1'}
        self.assertEqual(member_1, leaders[24])
  
    def test_around_me(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE * 3 + 1)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE * 3 + 1, 
            self.leaderboard.total_members())

        leaders_around_me = self.leaderboard.around_me('member_30')
        self.assertEqual(self.leaderboard.page_size / 2, 
            len(leaders_around_me) / 2)

        leaders_around_me = self.leaderboard.around_me('member_1')
        self.assertEqual(self.leaderboard.page_size / 2 + 1, 
            len(leaders_around_me))

        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 25)
        leaders_around_me = self.leaderboard.around_me('member_76')
        self.assertEqual(self.leaderboard.page_size / 2, 
            len(leaders_around_me) / 2)
  
    def test_ranked_in_list(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 
            self.leaderboard.total_members())

        members = ['member_1', 'member_5', 'member_10']

        ranked_members = self.leaderboard.ranked_in_list(members)

        self.assertEqual(3, len(ranked_members))

        self.assertEqual(25, ranked_members[0]['rank'])
        self.assertEqual(1, ranked_members[0]['score'])

        self.assertEqual(21, ranked_members[1]['rank'])
        self.assertEqual(5, ranked_members[1]['score'])

        self.assertEqual(16, ranked_members[2]['rank'])
        self.assertEqual(10, ranked_members[2]['score'])    
  
    def test_ranked_in_list_without_scores(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 
            self.leaderboard.total_members())

        members = ['member_1', 'member_5', 'member_10']
        ranked_members = self.leaderboard.ranked_in_list(members, 
            with_scores=False, 
            with_rank=True,
            use_zero_index_for_rank=False)

        self.assertEqual(3, len(ranked_members))

        self.assertEqual(25, ranked_members[0]['rank'])

        self.assertEqual(21, ranked_members[1]['rank'])

        self.assertEqual(16, ranked_members[2]['rank'])
  
    def test_remove_member(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 
            self.leaderboard.total_members())

        self.leaderboard.remove_member('member_1')

        self.assertEqual(lb.DEFAULT_PAGE_SIZE - 1, 
            self.leaderboard.total_members())
        self.assertEqual(None, self.leaderboard.rank_for('member_1'))
  
    def test_change_score_for(self):
        self.leaderboard.rank_member('member_1', 5)    
        self.assertEqual(5, self.leaderboard.score_for('member_1'))

        self.leaderboard.change_score_for('member_1', 5)
        self.assertEqual(10, self.leaderboard.score_for('member_1'))

        self.leaderboard.change_score_for('member_1', -5)    
        self.assertEqual(5, self.leaderboard.score_for('member_1'))
  
    def test_check_member(self):
        self.leaderboard.rank_member('member_1', 10)

        self.assertEqual(True, self.leaderboard.check_member('member_1'))
        self.assertEqual(False, self.leaderboard.check_member('member_2'))
  
    def test_can_change_page_size_and_have_it_reflected_in_size_of_result_set(self):
        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 25)
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.leaderboard.page_size = 5

        self.assertEqual(5, self.leaderboard.total_pages())
        self.assertEqual(5, len(self.leaderboard.leaders(1)))

    def test_cannot_set_page_size_to_invalid_page_size(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE)

        self.leaderboard.page_size = 0
        self.assertEqual(1, self.leaderboard.total_pages())
        self.assertEqual(lb.DEFAULT_PAGE_SIZE, 
            len(self.leaderboard.leaders(1)))
  
    def test_score_and_rank_for(self):
        self._rank_members_in_leaderboard()

        data = self.leaderboard.score_and_rank_for('member_1')
        self.assertEqual('member_1', data['member'])
        self.assertEqual(1, data['score'])
        self.assertEqual(5, data['rank'])
  
    def test_remove_members_in_score_range(self):
        self._rank_members_in_leaderboard()

        self.assertEqual(5, 
            self.leaderboard.total_members())

        self.leaderboard.rank_member('cheater_1', 100)
        self.leaderboard.rank_member('cheater_2', 101)
        self.leaderboard.rank_member('cheater_3', 102)

        self.assertEqual(8, self.leaderboard.total_members())

        self.leaderboard.remove_members_in_score_range(100, 102)

        self.assertEqual(5, self.leaderboard.total_members())

        for leader in self.leaderboard.leaders(1):
            self.assertTrue(leader['score'] < 100)
  
    def test_merge_leaderboards(self):
        foo = lb.Leaderboard('foo')    
        bar = lb.Leaderboard('bar')

        foo.rank_member('foo_1', 1)
        foo.rank_member('foo_2', 2)
        bar.rank_member('bar_1', 3)
        bar.rank_member('bar_2', 4)
        bar.rank_member('bar_3', 5)

        num_foobar_keys = foo.merge_leaderboards('foobar', ['bar'])
        self.assertEqual(5, num_foobar_keys)

        foobar = lb.Leaderboard('foobar')  
        self.assertEqual(5, foobar.total_members())

        first_leader_in_foobar = foobar.leaders(1)[0]
        self.assertEqual(1, first_leader_in_foobar['rank'])
        self.assertEqual('bar_3', first_leader_in_foobar['member'])
        self.assertEqual(5, first_leader_in_foobar['score'])

        lb.teardown()
  
    def test_intersect_leaderboards(self):
        foo = lb.Leaderboard('foo')
        bar = lb.Leaderboard('bar')

        foo.rank_member('foo_1', 1)
        foo.rank_member('foo_2', 2)
        foo.rank_member('bar_3', 6)
        bar.rank_member('bar_1', 3)
        bar.rank_member('foo_1', 4)
        bar.rank_member('bar_3', 5)

        # FIXME: add a constant for available aggregates
        num_foobar_keys = foo.intersect_leaderboards('foobar', ['bar'], 
            aggregate="max")
        self.assertEqual(2, num_foobar_keys)

        foobar = lb.Leaderboard('foobar')
        self.assertEqual(2, foobar.total_members())

        first_leader_in_foobar = foobar.leaders(1)[0]
        self.assertEqual(1, first_leader_in_foobar['rank'])
        self.assertEqual('bar_3', first_leader_in_foobar['member'])
        self.assertEqual(6, first_leader_in_foobar['score'])

        lb.teardown()
  
    def test_massage_leader_data_respects_with_scores(self):
        self._rank_members_in_leaderboard(25)

        self.assertEqual(25, self.leaderboard.total_members())

        leaders = self.leaderboard.leaders(1, 
            with_scores=False, 
            with_rank=False)

        self.assertNotEqual(None, leaders[0]['member'])
        self.assertFalse('score' in leaders[0])
        self.assertFalse('rank' in leaders[0])

        self.leaderboard.page_size = 25
        leaders = self.leaderboard.leaders(1, 
            with_scores=False, 
            with_rank=False)
        self.assertEqual(25, len(leaders))

        self.leaderboard.page_size = lb.DEFAULT_PAGE_SIZE
        leaders = self.leaderboard.leaders(1)
        self.assertNotEqual(None, leaders[0]['member'])
        self.assertNotEqual(None, leaders[0]['score'])
        self.assertNotEqual(None, leaders[0]['rank'])

        self.leaderboard.page_size = 25
        leaders = self.leaderboard.leaders(1)
        self.assertEqual(25, len(leaders))
  
    def test_total_pages_in_with_new_page_size(self):
        self._rank_members_in_leaderboard(25)

        self.assertEqual(1, 
            self.leaderboard.total_pages_in(self.leaderboard.name)
        )
        self.assertEqual(5, 
            self.leaderboard.total_pages_in(self.leaderboard.name, 5)
        )

    def test_leaders_call_with_new_page_size(self):
        self._rank_members_in_leaderboard(25)

        self.assertEqual(5, 
            len(self.leaderboard.leaders(1, page_size=5))
        )
        self.assertEqual(10, 
            len(self.leaderboard.leaders(1, page_size=10))
        )
        self.assertEqual(10, 
            len(self.leaderboard.leaders(2, page_size=10))
        )
        self.assertEqual(5, 
            len(self.leaderboard.leaders(3, page_size=10))
        )

    def test_around_me_call_with_new_page_size(self):
        self._rank_members_in_leaderboard(lb.DEFAULT_PAGE_SIZE * 3 + 1)

        leaders_around_me = self.leaderboard.around_me('member_30', page_size=3)
        self.assertEqual(3, len(leaders_around_me))
        self.assertEqual('member_31', leaders_around_me[0]['member'])
        self.assertEqual('member_29', leaders_around_me[2]['member'])

if __name__ == '__main__':
    unittest.main()