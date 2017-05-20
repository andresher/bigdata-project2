import plotly.plotly as py
from plotly.graph_objs import *
import datetime

f = open('../out/texts.txt', 'r')

texts = {}
for line in f:
    line = line[:-1]
    line = eval(line)
    for record in line:
        if record['text'] not in texts:
            texts[record['text']] = [(record['timestamp'], record['count'])]
        else:
            texts[record['text']].append((record['timestamp'], record['count']))

f.close()

trace_list = []
for key, value in texts.items():
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

py.plot(data, filename = 'texts-trace')

