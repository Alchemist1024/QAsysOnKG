# -*- encoding: utf-8 -*-
'''
@File        :chatbot_graph.py
@Time        :2020/12/04 15:35:02
@Author      :xiaoqifeng
@Version     :1.0
@Contact:    :unknown
'''

from question_classifier import *
from question_parser import *
from answer_search import *


class ChatBotGraph:
    '''问答类
    '''
    def __init__(self):
        self.classifier = QuestionClassifier()
        self.parser = QuestionPaser()
        self.searcher = AnswerSearcher()

    def chat_main(self, sen):
        answer = '您好，我是小勇医药智能助理，希望可以帮到您。'
        res_classify = self.classifier.classify(sen)
        print(res_classify)
        if not res_classify:
            return answer
        res_sql = self.parser.parser_main(res_classify)
        print(res_sql)
        final_answers = self.searcher.search_main(res_sql)
        if not final_answers:
            return answer
        else:
            return '\n'.join(final_answers)


if __name__ == '__main__':
    handler = ChatBotGraph()
    while 1:
        question = input('用户:')
        answer = handler.chat_main(question)
        print('bot:', answer)