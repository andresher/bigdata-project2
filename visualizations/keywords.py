import plotly.plotly as py
from plotly.graph_objs import *
import datetime

f = open('../out/keywords.txt', 'r')

keywords = {}
for line in f:
    line = line[:-1]
    line = eval(line)
    for record in line:
        if record['keyword'] not in keywords:
            keywords[record['keyword']] = [(record['timestamp'], record['count'])]
        else:
            keywords[record['keyword']].append((record['timestamp'], record['count']))

f.close()

trace_list = []
for key, value in keywords.items():
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

py.plot(data, filename = 'keywords-trace')

