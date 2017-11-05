requirejs(["d3"], function (d3) {

    var paths = $paths; // [["A", "B"], ["A", "C", "D"], ...]
    var valueSet = d3.set();
    paths.forEach(function (path) { path.forEach(function (val) { valueSet.add(val); }); });
    var values = valueSet.values();

    // Create a function to assign colors to attributes.
    var color = d3.scale.category10();

    // Dimensions of sunburst.
    var width = $width;
    var height = $height;
    var radius = Math.min(width, height) / 2;

    // Breadcrumb dimensions: width, height, spacing, width of tip/tail.
    var b = {
        w: 200,
        h: 30,
        s: 3,
        t: 10
    };

    // Total size of all segments; we set this later, after loading the data.
    var totalSize = 0;

    var vis = d3.select("#chart-$id").append("svg")
        .attr("width", width)
        .attr("height", height)
        .append("g")
        .attr("id", "container-$id")
        .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

    var partition = d3.layout.partition()
        .size([2 * Math.PI, radius])
        .value(function (d) {
            return d.size;
        });

    var arc = d3.svg.arc()
        .startAngle(function (d) {
            return d.x;
        })
        .endAngle(function (d) {
            return d.x + d.dx;
        })
        .innerRadius(function (d) {
            return d.y// Math.sqrt(d.y);
        })
        .outerRadius(function (d) {
            return d.y + d.dy//Math.sqrt(d.y + d.dy);
        });


    var tree = treeify(paths);
    createVisualization(tree);

    if ($showLegend) {
        d3.select("sidebar-$id").style("display", "")
    }

    // Main function to draw and set up the visualization, once we have the data.
    function createVisualization(json) {

        // Basic setup of page elements.
        initializeBreadcrumbTrail();
        drawLegend();
        d3.select("#togglelegend-$id").on("click", toggleLegend);

        // Bounding circle underneath the sunburst, to make it easier to detect
        // when the mouse leaves the parent g.
        vis.append("svg:circle")
            .attr("r", radius)
            .style("opacity", 0);

        // For efficiency, filter nodes to keep only those large enough to see.
        var nodes = partition.nodes(json)
            .filter(function (d) {
                return true; // return (d.dx > 0.005); // 0.005 radians = 0.29 degrees
            });

        var path = vis.data([json]).selectAll("path")
            .data(nodes)
            .enter().append("svg:path")
            .attr("display", function (d) {
                return d.depth ? null : "none";
            })
            .attr("d", arc)
            .attr("fill-rule", "evenodd")
            .style("fill", function (d) {
                return color(d.name);
            })
            .style("opacity", 1)
            .on("mouseover", mouseover);

        // Add the mouseleave handler to the bounding circle.
        d3.select("#container-$id").on("mouseleave", mouseleave);

        // Get total size of the tree = value of root node from partition.
        totalSize = path.node().__data__.value;
    };

    // Fade all but the current sequence, and show it in the breadcrumb trail.
    function mouseover(d) {

        var sequenceArray = getAncestors(d);
        updateBreadcrumbs(sequenceArray);

        // Fade all the segments.
        d3.selectAll("path")
            .style("opacity", 0.3);

        // Then highlight only those that are an ancestor of the current segment.
        vis.selectAll("path")
            .filter(function (node) {
                return (sequenceArray.indexOf(node) >= 0);
            })
            .style("opacity", 1);
    }

    // Restore everything to full opacity when moving off the visualization.
    function mouseleave(d) {

        // Hide the breadcrumb trail
        d3.select("#trail-$id")
            .style("visibility", "hidden");

        // Deactivate all segments during transition.
        d3.selectAll("path").on("mouseover", null);

        // Transition each segment to full opacity and then reactivate it.
        d3.selectAll("path")
            .transition()
            .duration(1000)
            .style("opacity", 1)
            .each("end", function () {
                d3.select(this).on("mouseover", mouseover);
            });
    }

    // Given a node in a partition layout, return an array of all of its ancestor
    // nodes, highest first, but excluding the root.
    function getAncestors(node) {
        var path = [];
        var current = node;
        while (current.parent) {
            path.unshift(current);
            current = current.parent;
        }
        return path;
    }

    function initializeBreadcrumbTrail() {
        // Add the svg area.
        var trail = d3.select("#sequence-$id").append("svg:svg")
            .attr("width", "100%")
            .attr("height", 50)
            .attr("id", "trail-$id");
    }

    // Generate a string that describes the points of a breadcrumb polygon.
    function breadcrumbPoints(d, i) {
        var points = [];
        points.push("0,0");
        points.push(b.w + ",0");
        points.push(b.w + b.t + "," + (b.h / 2));
        points.push(b.w + "," + b.h);
        points.push("0," + b.h);
        if (i > 0) { // Leftmost breadcrumb; don't include 6th vertex.
            points.push(b.t + "," + (b.h / 2));
        }
        return points.join(" ");
    }

    // Update the breadcrumb trail to show the current sequence.
    function updateBreadcrumbs(nodeArray) {
        if ($reverseLegend) nodeArray.reverse();

        // Data join; key function combines name and depth (= position in sequence).
        var g = d3.select("#trail-$id")
            .selectAll("g")
            .data(nodeArray, function (d) {
                return d.name + d.depth;
            });

        // Add breadcrumb and label for entering nodes.
        var entering = g.enter().append("svg:g");

        entering.append("svg:polygon")
            .style("fill", function (d) {
                return color(d.name);
            });

        entering.append("svg:text")
            .attr("x", (b.w + b.t) / 2)
            .attr("y", b.h / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .text(function (d) {
                return d.name;
            });

        // Set shape for entering and updating nodes.
        g.select("polygon").attr("points", breadcrumbPoints);

        // Set position for entering and updating nodes.
        g.attr("transform", function (d, i) {
            return "translate(" + i * (b.w + b.s) + ", 0)";
        });

        // Remove exiting nodes.
        g.exit().remove();

        // Make the breadcrumb trail visible, if it's hidden.
        d3.select("#trail-$id")
            .style("visibility", "");

    }

    function drawLegend() {

        // Dimensions of legend item: width, height, spacing, radius of rounded rect.
        var li = {
            w: 75,
            h: 30,
            s: 3,
            r: 3
        };

        var legend = d3.select("#legend-$id").append("svg:svg")
            .attr("width", li.w)
            .attr("height", values.length * (li.h + li.s));

        var g = legend.selectAll("g")
            .data(values)
            .enter().append("svg:g")
            .attr("transform", function (d, i) {
                return "translate(0," + i * (li.h + li.s) + ")";
            });

        g.append("svg:rect")
            .attr("rx", li.r)
            .attr("ry", li.r)
            .attr("width", li.w)
            .attr("height", li.h)
            .style("fill", function (d) {
                return color(d.name);
            });

        g.append("svg:text")
            .attr("x", li.w / 2)
            .attr("y", li.h / 2)
            .attr("dy", "0.35em")
            .attr("text-anchor", "middle")
            .text(function (d) {
                return d.name;
            });
    }

    function toggleLegend() {
        var legend = d3.select("#legend-$id");
        if (legend.style("visibility") == "hidden") {
            legend.style("visibility", "");
        } else {
            legend.style("visibility", "hidden");
        }
    }

    function treeify(paths) {
        var root = {
            "name": "root",
            "children": []
        };
        for (var i = 0; i < paths.length; i++) {
            var path = paths[i];
            var node = root;
            for (var j = 0; j < path.length; j++) {
                node = getOrAddChild(node, path[j]);
            }
        }
        return root;
    }

    function getOrAddChild(node, name) {
        if (node['children']) {
            for (var i = 0; i < node.children.length; i++) {
                if (node.children[i].name === name) return node.children[i];
            }
        } else {
            node.children = [];
        }
        var child = {
            name: name,
            index: node.children.length,
            size: 1
        };
        node.children.push(child);
        return child;
    }
});