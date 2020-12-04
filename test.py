# -*- encoding: utf-8 -*-
'''
@File        :test.py
@Time        :2020/12/03 08:34:06
@Author      :xiaoqifeng
@Version     :1.0
@Contact:    :unknown
'''

import ahocorasick


actree = ahocorasick.Automaton()
test = ['肖其峰', '自然语言处理']
for idx, word in enumerate(test):
    actree.add_word(word, (idx, word))
    actree.make_automaton()

print(actree)