#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import sys, os
if 'PYTHONIOENCODING' in os.environ:
  for line in sys.stdin:
    chars = list(line.rstrip())
    print('☆'.join(chars))
else:
  os.environ['PYTHONIOENCODING'] = 'UTF-8'
  sys.argv.insert(0, sys.executable)
  os.execvp(sys.argv[0], sys.argv)

import sqlite3
from datetime import datetime as dt
import random
import codecs


class OooGenerator(object):
	"""docstring for OooGenerator"""
	def __init__(self):
		super(OooGenerator, self).__init__()
		self.dbManager = OooDBManaber()

	def initialize(self):
		return self.dbManager.initializeDatabase()

	def caliculate_schedule(self, filename):
		self.dbManager.connectToDatabase(filename)
		members = self.dbManager.members
		while True:
			self.dbManager.createPared()
			if self.dbManager.isCompleted:
				break;

	def result(self):
		return self.dbManager.result()

	def export(self, db_path):
		self.dbManager.connectToDatabase(db_path)
		return self.dbManager.export()


class Member(object):
	"""docstring for Member"""
	def __init__(self, id, name):
		super(Member, self).__init__()
		self.id = id
		self.name = name

class OooDBManaber(object):
	"""docstring for OooDBManaber"""
	memberFileName = 'members.dat'

	def __init__(self):
		super(OooDBManaber, self).__init__()

	@property
	def members(self):
		c = self.conneciton.cursor()
		c.execute(u'SELECT * FROM Member')
		return [Member(m[0], m[1]) for m in c]

	@property
	def scheduleIds(self):
		c = self.conneciton.cursor()
		c.execute(u'SELECT * FROM Schedule')
		return [s[0] for s in c]

	@property
	def notScheduledMemberIds(self):
		c = self.conneciton.cursor()
		c.execute(u'SELECT * FROM Pair WHERE ScheduleId = {0}'.format(self.scheduleId))
		ids = [(m[1], m[2]) for m in c];
		memberIds = set([m.id for m in self.members])
		if len(ids) > 0:
			ids1, ids2 = zip(*ids)
			memberIds = memberIds - set(ids1)
			memberIds = memberIds - set(ids2)
		print('memberIds: {0}'.format(memberIds))
		return memberIds

	@property
	def isCompleted(self):
		members = self.members
		memberNum = len(members) - 1
		for m in members:
			c = self.conneciton.cursor()
			c.execute(u'select count(*) from Pair where MemberId1 = {0} OR MemberId2 = {0}'.format(m.id))
			count = c.fetchone()[0]
			# print('count: {0}'.format(count))
			if count < memberNum:
				return False
		return True

	def export(self):
		f = codecs.open("result.csv", "w", "utf-8")
		members = self.members
		memberIds = set([m.id for m in members])
		memberNames = [m.name for m in members]
		print(','.join(memberNames))
		f.write(','.join(memberNames) + '\n')
		for sid in self.scheduleIds:
			print('{0}日目'.format(sid))
			c = self.conneciton.cursor()
			c.execute(u'SELECT * FROM Pair WHERE ScheduleId = {0}'.format(sid))
			selectedMemberIds = set()
			pairDict = {}
			for pair in c:
				pairDict[pair[1]] = pair[2]
				pairDict[pair[2]] = pair[1]
				m1 = [x for x in members if x.id == pair[1]][0]
				m2 = [x for x in members if x.id == pair[2]][0]
				print('{0} and {1}'.format(m1.name, m2.name))
				selectedMemberIds.add(m1.id)
				selectedMemberIds.add(m2.id)
			print(pairDict)
			print(memberIds - selectedMemberIds)
			formatted = ','.join([memberNames[pairDict[mid]] if mid in selectedMemberIds and mid < pairDict[mid] else '←' if mid in selectedMemberIds else '-' for mid in memberIds])
			print(formatted)
			f.write(formatted + '\n')
		f.close()
			

	def result(self):
		c = self.conneciton.cursor()
		c.execute(u'select count(*) from Pair group by ScheduleId')
		count = 0
		for x in c:
			count += 1
		return count;

	def createPared(self):
		members = self.members
		notScheduledMemberIds = self.notScheduledMemberIds
		if len(notScheduledMemberIds) < 2:
			self.scheduleId += 1
			self._insertSchedule(self.scheduleId)
			print(self.scheduleId)
			return
		sourceId = random.sample(notScheduledMemberIds, 1)[0]
		notScheduledMemberIds = notScheduledMemberIds - set([sourceId])

		c = self.conneciton.cursor()
		c.execute(u'SELECT * FROM Pair where MemberId1 = {0} OR MemberId2 = {0}'.format(sourceId))
		pairedIds = set([pair[2] if pair[1] == sourceId else pair[1] for pair in c])
		notPairedIds = notScheduledMemberIds - pairedIds - set([sourceId])
		if len(notPairedIds) == 0:
			print('notPairedIds: {0} with {1}'.format(list(notPairedIds), sourceId))
			self.scheduleId += 1
			self._insertSchedule(self.scheduleId)
			return
		self._insertPair(sourceId, random.sample(notPairedIds, 1)[0])

	def initializeDatabase(self):
		d = dt.now()
		d_str = "data_{0}.db".format(d.strftime('%Y%m%d%H%M%s%f'))
		self.conneciton = sqlite3.connect(d_str)
		self._createMemberTable()
		self._insertAllMembers()
		self._createScheduleTable()
		self._createPairTable()
		self.scheduleId = 0 if len(self.scheduleIds) == 0 else max(self.scheduleIds)
		self._insertSchedule(self.scheduleId)
		self.close()
		return d_str

	def connectToDatabase(self, path):
		self.conneciton = sqlite3.connect(path)
		self.scheduleId = max(self.scheduleIds)

	def close(self):
		self.conneciton.close()

	def _createMemberTable(self):
		sql = u'''
		CREATE TABLE Member (
			Id integer PRIMARY KEY,
			Name varchar(10)
		);
		'''
		self.conneciton.execute(sql)

	def _createScheduleTable(self):
		sql = u'''
		CREATE TABLE Schedule (
			Id integer PRIMARY KEY		
		);
		'''
		self.conneciton.execute(sql)

	def _createPairTable(self):
		sql = u'''
		CREATE TABLE Pair (
			Id integer PRIMARY KEY AUTOINCREMENT,
			MemberId1 integer,
			MemberId2 integer,
			ScheduleId integer,
			FOREIGN KEY(MemberId1) REFERENCES Member(Id),
			FOREIGN KEY(MemberId2) REFERENCES Member(Id),
			FOREIGN KEY(ScheduleId) REFERENCES Schedule(Id)
		);
		'''
		self.conneciton.execute(sql)

	def _insertSchedule(self, id):
		self.conneciton.execute(u'Insert INTO Schedule VALUES ({0})'.format(id));
		self.conneciton.commit()

	def _insertAllMembers(self):
		members = [m.strip() for m in open(OooDBManaber.memberFileName, encoding='utf-8').readlines()]
		sql_values = [(i, m) for i, m in enumerate(members)]
		self.conneciton.executemany(u'INSERT INTO Member VALUES (?, ?)', sql_values)
		self.conneciton.commit()

	def _insertPair(self, memberId1, memberId2):
		self.conneciton.execute(u'INSERT INTO Pair(MemberId1, MemberId2, ScheduleId) VALUES ({0}, {1}, {2})'.format(memberId1, memberId2, self.scheduleId));
		print(memberId1)
		print(memberId2)
		self.conneciton.commit()
	
'''
num = 1000	
best = None
for i in range(10000):
	generator = OooGenerator()
	filename = generator.initialize();
	print(filename)
	generator.caliculate_schedule(filename)
	result = generator.result()
	if result < num:
		num = result
		best = filename
print(num)
print(best)
'''

generator = OooGenerator()
generator.export('data.db')

