'''
Created on Jul 28, 2017

@author: stata001c
'''

import plotly.offline as offline
import plotly.graph_objs as go


x1=['2016', '2017', '2018']
y1=[1, 20, 3]

'''
# Working block
#print (x1)
offline.plot({'data': [go.Scatter(x=x1,y=y1)],'layout': {'title': 'Scatter', 'font': dict(size=16)}}, filename='my-sctter.html')

offline.plot({'data': [go.Bar(x=x1,y=y1)],'layout': {'title': 'Bar', 'font': dict(size=16)}}, filename='my-Bar.html')

offline.plot({'data': [go.Surface(x=x1,y=y1)],'layout': {'title': 'Surface', 'font': dict(size=16)}}, filename='my-Surface.html')
 
offline.plot({'data': [go.Scatter3d(x=x1,y=y1)],'layout': {'title': 'Scatter3d', 'font': dict(size=16)}}, filename='my-Scatter3d.html')
 
 '''

#offline.plot({'data': [go.Area(x=x1,y=y1)],'layout': {'title': 'Area', 'font': dict(size=16)}}, filename='my-Area.html')
offline.plot({'data': [go.Bar(x=x1,y=y1)],'layout': {'title': 'Bar', 'font': dict(size=16)}}, filename='my-Bar.html')
offline.plot({'data': [go.Box(x=x1,y=y1)],'layout': {'title': 'Box', 'font': dict(size=16)}}, filename='my-Box.html')
#offline.plot({'data': [go.Candlestick(x=x1,y=y1)],'layout': {'title': 'Candlestick', 'font': dict(size=16)}}, filename='my-Candlestick.html')
offline.plot({'data': [go.Carpet(x=x1,y=y1)],'layout': {'title': 'Carpet', 'font': dict(size=16)}}, filename='my-Carpet.html')
#offline.plot({'data': [go.Choropleth(x=x1,y=y1)],'layout': {'title': 'Choropleth', 'font': dict(size=16)}}, filename='my-Choropleth.html')
offline.plot({'data': [go.Contour(x=x1,y=y1)],'layout': {'title': 'Contour', 'font': dict(size=16)}}, filename='my-Contour.html')
#offline.plot({'data': [go.Contourcarpet(x=x1,y=y1)],'layout': {'title': 'Contourcarpet', 'font': dict(size=16)}}, filename='my-Contourcarpet.html')
offline.plot({'data': [go.Heatmap(x=x1,y=y1)],'layout': {'title': 'Heatmap', 'font': dict(size=16)}}, filename='my-Heatmap.html')
offline.plot({'data': [go.Heatmapgl(x=x1,y=y1)],'layout': {'title': 'Heatmapgl', 'font': dict(size=16)}}, filename='my-Heatmapgl.html')
offline.plot({'data': [go.Histogram(x=x1,y=y1)],'layout': {'title': 'Histogram', 'font': dict(size=16)}}, filename='my-Histogram.html')
offline.plot({'data': [go.Histogram2d(x=x1,y=y1)],'layout': {'title': 'Histogram2d', 'font': dict(size=16)}}, filename='my-Histogram2d.html')
offline.plot({'data': [go.Histogram2dcontour(x=x1,y=y1)],'layout': {'title': 'Histogram2dcontour', 'font': dict(size=16)}}, filename='my-Histogram2dcontour.html')
offline.plot({'data': [go.Mesh3d(x=x1,y=y1)],'layout': {'title': 'Mesh3d', 'font': dict(size=16)}}, filename='my-Mesh3d.html')
#offline.plot({'data': [go.Ohlc(x=x1,y=y1)],'layout': {'title': 'Ohlc', 'font': dict(size=16)}}, filename='my-Ohlc.html')
#offline.plot({'data': [go.Parcoords(x=x1,y=y1)],'layout': {'title': 'Parcoords', 'font': dict(size=16)}}, filename='my-Parcoords.html')
#offline.plot({'data': [go.Pie(x=x1,y=y1)],'layout': {'title': 'Pie', 'font': dict(size=16)}}, filename='my-Pie.html')
offline.plot({'data': [go.Pointcloud(x=x1,y=y1)],'layout': {'title': 'Pointcloud', 'font': dict(size=16)}}, filename='my-Pointcloud.html')
#offline.plot({'data': [go.Sankey(x=x1,y=y1)],'layout': {'title': 'Sankey', 'font': dict(size=16)}}, filename='my-Sankey.html')
offline.plot({'data': [go.Scatter(x=x1,y=y1)],'layout': {'title': 'Scatter', 'font': dict(size=16)}}, filename='my-Scatter.html')
offline.plot({'data': [go.Scatter3d(x=x1,y=y1)],'layout': {'title': 'Scatter3d', 'font': dict(size=16)}}, filename='my-Scatter3d.html')
#offline.plot({'data': [go.Scattercarpet(x=x1,y=y1)],'layout': {'title': 'Scattercarpet', 'font': dict(size=16)}}, filename='my-Scattercarpet.html')
#offline.plot({'data': [go.Scattergeo(x=x1,y=y1)],'layout': {'title': 'Scattergeo', 'font': dict(size=16)}}, filename='my-Scattergeo.html')
offline.plot({'data': [go.Scattergl(x=x1,y=y1)],'layout': {'title': 'Scattergl', 'font': dict(size=16)}}, filename='my-Scattergl.html')
#offline.plot({'data': [go.Scattermapbox(x=x1,y=y1)],'layout': {'title': 'Scattermapbox', 'font': dict(size=16)}}, filename='my-Scattermapbox.html')
#offline.plot({'data': [go.Scatterternary(x=x1,y=y1)],'layout': {'title': 'Scatterternary', 'font': dict(size=16)}}, filename='my-Scatterternary.html')
#offline.plot({'data': [go.Surface(x=x1,y=y1)],'layout': {'title': 'Surface', 'font': dict(size=16)}}, filename='my-Surface.html')


 

#offline.plot({'data': [{'y': [4, 2, 3, 4]}],'layout': {'title': 'Test Plot', 'font': dict(size=16)}},image='png')




