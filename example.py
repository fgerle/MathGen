from MathGen import *

"""
Generate the genealogical graph of the four Fields medalists of 2022:
- Hugo Duminil-Copin
- June Huh
- James Maynard
- Maryna Viazovska
"""


DuminilCopin_ID   = 168435
Huh_ID             = 185855
Maynard_ID         = 178890
Viazovska_ID       = 201884

IDs = [DuminilCopin_ID, Huh_ID, Maynard_ID, Viazovska_ID]

# Define different color pallettes for graph drawing
cols = ["#b3cde3","#ccebc5","#decbe4","#fed9a6","#ffffcc","#e5d8bd","#fddaec","#fbb4ae"]
cols2 = ["#00202e","#003f5c","#2c4875","#8a508f","#bc5090","#ff6361","#ff8531","#ffa600","#ffd380"]
cols3 = ["#ffadad","#ffd6a5","#fdffb6","#caffbf","#9bf6ff","#a0c4ff","#bdb2ff","#ffc6ff"]
cols4 = ['#8dd3c7','#ffffb3','#bebada','#fb8072','#80b1d3','#fdb462','#b3de69','#fccde5','#d9d9d9']

def test():
    # initialize the genealogy
    G = mathGenealogy("example.db")

    # add all ancestors and descendants of the starting vertices to the graph

    for ID in IDs:
        G.add_ancestors(ID)       
        G.add_descendants(ID)

    # pin all starting vertices at the same level    
    G.fixed_level(IDs)

    # Color the graph 
    G.color_graph_CSS(cols1)
    # Draw the graph
    G.draw_graph("example.dot", "pdf", clean=False)
    return(G)

def redraw(color, graph):
    graph.color_graph_CSS(color)
    graph.draw_graph("example.dot")

if __name__ == "__main__":
#    testDB = mathDB("test.db")
    G = test()
