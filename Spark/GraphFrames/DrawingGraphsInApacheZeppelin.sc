import org.apache.spark.graphx._

// Drawing GraphFrame in Apache Zeppelin.
def drawGraphFrame(graph:GraphFrame) = {
val g = graph.toGraphX
val u = java.util.UUID.randomUUID
val v = g.vertices.collect
val v2 = g.vertices.collect.map(_._1)
println("""%html
<div id='a""" + u + """' style='width:100%; height:100%'></div>
<style>
.link { stroke: #ccc; }
.nodetext { pointer-events: none; font: 10px sans-serif; }
.linktext { pointer-events: none; font: 10px sans-serif; }
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/5.9.2/d3.min.js"></script>
<script>
var w = 960,
    h = 500
var color = d3.scale.category20();
var vis = d3.select("#a""" + u + """").append("svg:svg")
    .attr("width", w)
    .attr("height", h);
var nodes = [""" + v.map(x => "{id:" + x._1 + ",name:\""+(x._2.asInstanceOf[Row])(0)+"\"}").mkString(",") + """];
var links = [""" + g.edges.collect.map(
e => "{source:nodes[" + v2.indexWhere(_ == e.srcId) + "],target:nodes[" +
v2.indexWhere(_ == e.dstId) + "],label:'"+(e.attr.asInstanceOf[Row]).get(2)+"'}").mkString(",") + """];
var force = self.force = d3.layout.force()
        .nodes(nodes)
        .links(links)
        .gravity(.05)
        .distance(100)
        .charge(-100)
        .size([w, h])
        .start();
    var link = vis.selectAll("line.link")
        .data(links)
        .enter()
        .append("g")
        .attr("class","link-group")
        .append("svg:line")
        .attr("class", "link")
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });
    var linkText = vis.selectAll(".link-group")
        .append("svg:text")
        .attr("class","linktext")
        .data(links)
        .text(function(d) { return d.label })
        .attr("x", function(d) { return (d.source.x + (d.target.x - d.source.x) * 0.5); })
        .attr("y", function(d) { return (d.source.y + (d.target.y - d.source.y) * 0.5); })
        .attr("dx", 12)
        .attr("dy", ".25em")
        .attr("text-anchor", "middle");
    var node_drag = d3.behavior.drag()
        .on("dragstart", dragstart)
        .on("drag", dragmove)
        .on("dragend", dragend);
    function dragstart(d, i) {
        force.stop() // stops the force auto positioning before you start dragging
    }
    function dragmove(d, i) {
        d.px += d3.event.dx;
        d.py += d3.event.dy;
        d.x += d3.event.dx;
        d.y += d3.event.dy; 
        tick(); // this is the key to make it work together with updating both px,py,x,y on d !
    }
    function dragend(d, i) {
        d.fixed = true; // of course set the node to fixed so the force doesn't include the node in its auto positioning stuff
        tick();
        force.resume();
    }
    var node = vis.selectAll("g.node")
        .data(nodes)
      .enter().append("svg:g")
        .attr("class", "node")
        .call(node_drag);
    node.append("circle")
          .attr("r", 5)
          .attr("fill",function(d,i){return color(i);});
    node.append("svg:text")
        .attr("class", "nodetext")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(function(d) { return d.name });
    force.on("tick", tick);
    function tick() {
      link.attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });
      node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
      linkText
        .attr("x", function(d) { return (d.source.x + (d.target.x - d.source.x) * 0.5); })
        .attr("y", function(d) { return (d.source.y + (d.target.y - d.source.y) * 0.5); });
    };
</script>
""")
}

import org.apache.spark.graphx._
import scala.reflect.ClassTag
// Drawing GraphX made from GraphFrame in Apache Zeppelin.
def drawGraphX_ExGraphFrame[VD:ClassTag,ED:ClassTag](g:Graph[VD,ED]) = {
val u = java.util.UUID.randomUUID
val v = g.vertices.collect
val v2 = g.vertices.collect.map(_._1)
println("""%html
<div id='a""" + u + """' style='width:100%; height:100%'></div>
<style>
.link { stroke: #ccc; }
.nodetext { pointer-events: none; font: 10px sans-serif; }
.linktext { pointer-events: none; font: 10px sans-serif; }
</style>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/5.9.2/d3.min.js"></script>
<script>
var w = 960,
    h = 500

var color = d3.scale.category20();
var vis = d3.select("#a""" + u + """").append("svg:svg")
    .attr("width", w)
    .attr("height", h);

var nodes = [""" + v.map(x => "{id:" + x._1 + ",name:\""+(x._2.asInstanceOf[Row])(0)+"\"}").mkString(",") + """];
var links = [""" + g.edges.collect.map(
e => "{source:nodes[" + v2.indexWhere(_ == e.srcId) + "],target:nodes[" +
v2.indexWhere(_ == e.dstId) + "],label:'"+(e.attr.asInstanceOf[Row]).get(2)+"'}").mkString(",") + """];

var force = self.force = d3.layout.force()
        .nodes(nodes)
        .links(links)
        .gravity(.05)
        .distance(100)
        .charge(-100)
        .size([w, h])
        .start();

    var link = vis.selectAll("line.link")
        .data(links)
        .enter()
        .append("g")
        .attr("class","link-group")
        .append("svg:line")
        .attr("class", "link")
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });
        
    var linkText = vis.selectAll(".link-group")
        .append("svg:text")
        .attr("class","linktext")
        .data(links)
        .text(function(d) { return d.label })
        .attr("x", function(d) { return (d.source.x + (d.target.x - d.source.x) * 0.5); })
        .attr("y", function(d) { return (d.source.y + (d.target.y - d.source.y) * 0.5); })
        .attr("dx", 12)
        .attr("dy", ".25em")
        .attr("text-anchor", "middle");

    var node_drag = d3.behavior.drag()
        .on("dragstart", dragstart)
        .on("drag", dragmove)
        .on("dragend", dragend);

    function dragstart(d, i) {
        force.stop() // stops the force auto positioning before you start dragging
    }

    function dragmove(d, i) {
        d.px += d3.event.dx;
        d.py += d3.event.dy;
        d.x += d3.event.dx;
        d.y += d3.event.dy; 
        tick(); // this is the key to make it work together with updating both px,py,x,y on d !
    }

    function dragend(d, i) {
        d.fixed = true; // of course set the node to fixed so the force doesn't include the node in its auto positioning stuff
        tick();
        force.resume();
    }

    var node = vis.selectAll("g.node")
        .data(nodes)
      .enter().append("svg:g")
        .attr("class", "node")
        .call(node_drag);

    node.append("circle")
          .attr("r", 5)
          .attr("fill",function(d,i){return color(i);});
        
    node.append("svg:text")
        .attr("class", "nodetext")
        .attr("dx", 12)
        .attr("dy", ".35em")
        .text(function(d) { return d.name });

    force.on("tick", tick);

    function tick() {
      link.attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; })
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });

      node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
      linkText
        .attr("x", function(d) { return (d.source.x + (d.target.x - d.source.x) * 0.5); })
        .attr("y", function(d) { return (d.source.y + (d.target.y - d.source.y) * 0.5); });

    };
</script>
""")
}