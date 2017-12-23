from time import time, sleep, strftime, gmtime


class ExceptionHelper:
    def __init__(self, program, idx, sleep_time, update_time):
        if sleep_time is None:
            self.sleep_time = 0
        else:
            self.sleep_time = float(sleep_time)
        if update_time is None:
            self.update_time = 0
        else:
            self.update_time = float(update_time)
        self.idx = idx
        self.program = program
        self.iteration = 0
        self.last_error = -1

    def exchange_malfunction(self):
        if self.last_error >= (self.iteration - 1):
            return True
        else:
            return False

    def record_exception(self, e, attempt=0):
        if self.idx is None:
            file_name = 'logs/' + self.program + '.log'
        else:
            file_name = 'logs/' + self.idx + '.log'
        with open(file_name, 'a') as f:
            f.write('[' + self.program + ']: ' + 'Iteration: ' + str(self.iteration) + ' Datetime: ' +
                    strftime("%Y-%m-%d %H:%M:%S",gmtime(time())) + ' Exception: ' + str(e).strip()[:25])
            f.write('\n')
        if attempt == 3:
            with open(file_name, 'a') as f:
                f.write('[' + self.program + ']: ' + 'Too many attempts to reconnect to exchange, timeout for 5 minutes')
                f.write('\n')
            sleep(300)
        self.last_error = self.iteration

    def record_sql_exception(self, e):
        with open('logs/sql.log', 'a') as f:
            f.write('[' + self.program + ']: '+ 'Iteration: ' + str(self.iteration) + ' Datetime: ' +
                    strftime("%Y-%m-%d %H:%M:%S", gmtime(time())) + ' Exception: ' + str(e).strip()[:25])
            f.write('\n')