# -*- coding: utf-8 -*-
# Author: YanHua <it-yanh@all-reach.com>


class Singleton(type):

    def __init__(self, name, bases, class_dict):

        super(Singleton, self).__init__(name, bases, class_dict)
        self._instance = None

    def __call__(self, *args, **kwargs):

        if self._instance is None:
            self._instance = super(Singleton, self).__call__(*args, **kwargs)

        return self._instance
