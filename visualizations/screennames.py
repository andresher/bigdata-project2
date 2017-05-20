import plotly.plotly as py
from plotly.graph_objs import *
import datetime

f = open('../out/screennames.txt', 'r')

screennames = {}
for line in f:
    line = line[:-1]
    line = eval(line)
    for record in line:
        if record['sn'] not in screennames:
            screennames[record['sn']] = [(record['timestamp'], record['count'])]
        else:
            screennames[record['sn']].append((record['timestamp'], record['count']))

f.close()

trace_list = []
for key, value in screennames.items():
    x_list = []
    y_list = []
    for tup in value:
        x_list.append(tup[0])
        y_list.append(tup[1])
    trace_list.append(Scatter(
        x=x_list,
        y=y_list,
        name=key
    ))

data = Data(trace_list)

py.plot(data, filename = 'screennames-trace')

