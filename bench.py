from __future__ import division
import random
from time import time
from leaderboard import Leaderboard
def time_inserts(num=100000):
	start = time()
	for i in range(num):
		highscore_lb.rank_member("member_%d" % i, i)
	end = time()
	print "%d inserted, avg %f seconds" % (num, (end-start)/num)

def time_leader_fetch(num=50000):
	highscore_lb = Leaderboard('highscores')
	page = random.randint(0, highscore_lb.total_pages())
	lb_request_time = 0
	for i in range(num):
		start = time()
		highscore_lb.leaders(page)
		lb_request_time += time()-start
	print "%d fetches, avg %f seconds" % (num, lb_request_time / num)

if __name__ == '__main__':
	highscore_lb = Leaderboard('highscores')
	highscore_lb.redis.flushdb()
	print "inserts"
	time_inserts()
	print "fetch"
	time_leader_fetch()
