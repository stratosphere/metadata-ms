requirejs(["d3"], function (d3) {

    var nodes = $nodes; // nodes should be of the form {"id":..., "name":...}
    var links = $links; // links should be of the form {"source":..., "target":..., "value":...}

    // Convert the node IDs to indices.
    var id2index = {};
    nodes.forEach(function (node, index) {
        id2index[node.id] = index;
        node.index = index;
    });
    links.forEach(function (link) {
        link.source = id2index[link.source];
        link.target = id2index[link.target];
    });

    var margin = {top: 120, right: 100, bottom: 10, left: 120},
        width = $width,
        height = $width;

    var x = d3.scale.ordinal().rangeBands([0, width]),
        // opacity = d3.scale.linear().domain([0, 1]).range([0, 1]).clamp(true),
        opacity = function (d) {
            return 1;
        },
        color = d3.scale.linear().domain([0, 1]).range(['#e2e2e2', '#2b65c4']).clamp(true);

    var svg = d3.select("#$svgId")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .style("margin-left", margin.left + "px")
        .style("margin-top", margin.top + "px")
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Create the matrix.
    var matrix = [],
        n = nodes.length;
    nodes.forEach(function (node, i) {
        matrix[i] = d3.range(n).map(function (j) {
            return {x: j, y: i, z: i == j ? 1 : 0};
        });
    });

    // Convert links to matrix; count character occurrences.
    links.forEach(function (link) {
        matrix[link.source][link.target].z = link.value;
        matrix[link.target][link.source].z = link.value;
    });

    // Precompute the orders.
    var orders = {
        name: d3.range(n).sort(function (a, b) {
            return d3.ascending(nodes[a].name, nodes[b].name);
        }),
        id: d3.range(n).sort(function (a, b) {
            return nodes[a].id - nodes[b].id;
        })
    };

    // The default sort order.
    x.domain(orders.name);

    svg.append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height);

    var row = svg.selectAll(".row")
        .data(matrix)
        .enter().append("g")
        .attr("class", "row")
        .attr("transform", function (d, i) {
            return "translate(0," + x(i) + ")";
        })
        .each(createRow);

    row.append("line")
        .attr("x2", width);

    row.append("text")
        .attr("x", -6)
        .attr("y", x.rangeBand() / 5)
        .attr("dy", ".32em")
        .attr("text-anchor", "end")
        .text(function (d, i) {
            return nodes[i].name;
        });

    var column = svg.selectAll(".column")
        .data(matrix)
        .enter().append("g")
        .attr("class", "column")
        .attr("transform", function (d, i) {
            return "translate(" + x(i) + ")rotate(-90)";
        });

    column.append("line")
        .attr("x1", -width);

    column.append("text")
        .attr("x", 6)
        .attr("y", x.rangeBand() / 5)
        .attr("dy", ".32em")
        .attr("text-anchor", "start")
        .text(function (d, i) {
            return nodes[i].name;
        });

    function createRow(row) {
        var cell = d3.select(this).selectAll(".cell")
            .data(row.filter(function (d) {
                return d.z;
            }))
            .enter().append("rect")
            .attr("class", "cell")
            .attr("x", function (d) {
                return x(d.x);
            })
            .attr("width", x.rangeBand())
            .attr("height", x.rangeBand())
            .style("fill-opacity", function (d) {
                return opacity(d.z);
            })
            .style("fill", function (d) {
                return color(d.z);
            })
            .on("mouseover", mouseover)
            .on("mouseout", mouseout);
    }

    function mouseover(p) {
        // d3.selectAll(".row text").classed("active", function(d, i) { return i == p.y; });
        // d3.selectAll(".column text").classed("active", function(d, i) { return i == p.x; });
        d3.selectAll(".row text").style("visibility", function (d, i) {
            return i == p.y ? "visible" : "hidden";
        });
        d3.selectAll(".column text").style("visibility", function (d, i) {
            return i == p.x ? "visible" : "hidden";
        });
    }

    function mouseout() {
        d3.selectAll(".row text").style("visibility", "visible");
        d3.selectAll(".column text").style("visibility", "visible");
    }
});