# -*- coding: utf-8 -*-

import time


class Progress(object):
    """Prints progress information when iterating over any loop.

       Output like:
       [###################################...............] 70%  00:05:18

       Is printed every completed percent or every 'interval' seconds.
       A full bar, 100%, and the elapsed time is printed at the end.
    """
    def __init__(self, n, interval=0.5, lenght=50, fill='#', empty='.'):
        self.length = n
        self.interval = interval
        self.barlenth = lenght
        self.__fill = fill
        self.__empty = empty
        self.__fillbars = 0
        self.__emptybars = 0
        self.elapsed = lambda: time.time() - self.t0

    def __enter__(self):
        self.t0 = time.time()
        self.previoustime = 0
        self.previouspercent = 0
        self.completed = 0.0

        return self

    def __exit__(self, *args):
        print(' [{}] 100%  {:02}:{:02}:{:02}'.
            format(self.__fill * self.barlenth,
                   *self.divmods(self.elapsed())))

    def update(self, i):
        percent = 100 * i // self.length

        if percent != self.previouspercent or \
           self.elapsed() > self.previoustime + self.interval:
            self.previoustime = self.elapsed()
            self.previouspercent = percent
            self.completed = i / self.length

            self.__fillbars = int(self.barlenth * self.completed)
            self.__emptybars = self.barlenth - self.__fillbars

            print(' [{}{}] {:0.0f}%  {:02}:{:02}:{:02}'.
                format(self.__fill * self.__fillbars,
                       self.__empty * self.__emptybars,
                       self.completed * 100,
                       *self.divmods(self.elapsed())), end='\r')

    def divmods(self, t):
        """Convert Time Seconds to h:m:s

        Source:
        http://stackoverflow.com/questions/775049/python-time-seconds-to-hms
        """
        s = int(t)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return (h, m, s)


if __name__ == '__main__':
    nrows = 1000

    with Progress(nrows) as p:      # Start the progress bar
        for i in range(nrows):      # Then start a loop
            p.update(i)             # Update with current progress
            time.sleep(0.01)        # Do something
