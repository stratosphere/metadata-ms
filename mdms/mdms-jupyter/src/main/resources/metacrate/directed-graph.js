requirejs(["d3"], function (d3) {

    // Initialize the data structures.
    var nodes = $nodes,
        links = $links;
    for (var i = 0; i < links.length; i++) {
        var link = links[i];
        link.source = nodes.find(function (n) { return n.name === link.source; });
        link.target = nodes.find(function (n) { return n.name === link.target; });
        // Handle missing nodes.
        if (link.source === undefined || link.target === undefined) links.splice(i--, 1);
    }

    var svg = d3.select("#metacrate-svg-$id");
    var parentDiv = svg[0][0].parentNode;
    var nodeColors = d3.scale.category10();

    var force = d3.layout.force()
        .nodes(d3.values(nodes))
        .links(links)
        .size([parentDiv.clientWidth, parentDiv.clientHeight])
        .linkDistance($linkDistance)
        .charge(-$charge)
        .on("tick", tick)
        .start();

    // Per-type markers, as they don't inherit styles.
    svg.append("defs").selectAll("marker")
        .data(["default"])
        .enter().append("marker")
        .attr("id", function(d) { return d; })
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 15)
        .attr("refY", -1.5)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M0,-5L10,0L0,5");

    var path = svg.append("g").selectAll("path")
        .data(force.links())
        .enter().append("path")
        .attr("class", "$pathStyle")
        .attr("marker-end", function(d) { return "url(#default)"; });

    var circle = svg.append("g").selectAll("circle")
        .data(force.nodes())
        .enter().append("circle")
        .attr("class", "$circleStyle")
        .attr("r", function(d) { return d.size; })
        .attr("fill", function (d) { return nodeColors(d.color); })
        .call(force.drag);

    var text = svg.append("g").selectAll("text")
        .data(force.nodes())
        .enter().append("text")
        .attr("class", "$textStyle")
        .attr("x", 8)
        .attr("y", ".31em")
        .text(function(d) { return d.name; });

    d3.select("#freeze-button-$id").on("click", buttonClicked);
    d3.select("#distance-slider-$id").on("change", distanceSliderChanged);
    d3.select("#charge-slider-$id").on("change", chargeSliderChanged);

    // Use elliptical arc path segments to doubly-encode directionality.
    function tick() {
        path.attr("d", linkArc);
        circle.attr("transform", transform);
        text.attr("transform", transform);
    }

    function linkArc(d) {
        var dx = d.target.x - d.source.x,
            dy = d.target.y - d.source.y,
            dr = Math.sqrt(dx * dx + dy * dy);
        return "M" + d.source.x + "," + d.source.y + "A" + dr + "," + dr + " 0 0,1 " + d.target.x + "," + d.target.y;
    }

    function transform(d) {
        return "translate(" + d.x + "," + d.y + ")";
    }

    function buttonClicked() {
        if (this.firstChild.data == "Freeze") {
            force.stop()
            this.firstChild.data = "Continue"
        } else {
            force.start()
            this.firstChild.data = "Freeze"
        }
    }

    function distanceSliderChanged() {
        force.stop();
        force.linkDistance(this.value);
        force.start();
    }

    function chargeSliderChanged() {
        force.stop();
        force.charge(-this.value);
        force.start();
    }

});