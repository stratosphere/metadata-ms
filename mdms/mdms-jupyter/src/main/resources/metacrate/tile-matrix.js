requirejs(["d3"], function (d3) {
    // Collect larger inputs.
    var tiles = $tiles,
        rows = $rows,
        columns = $columns;

    // Order and index the columns and rows.
    var compareCategories = function (a, b) {
        var result = d3.ascending(a.order, b.order);
        if (result) return result;
        return d3.ascending(a.name, b.name);
    }
    rows.sort(compareCategories);
    var rowsByName = {};
    rows.forEach(function (d, i) {
        rowsByName[d.name] = d;
        d.index = i;
    });
    columns.sort(compareCategories);
    var columnsByName = {};
    columns.forEach(function (d, i) {
        columnsByName[d.name] = d;
        d.index = i;
    });

    // Create the matrix.
    var matrix = [];
    for (var y = 0; y < rows.length; y++) {
        matrix[y] = []
        for (var x = 0; x < columns.length; x++) {
            matrix[y].push(null);
        }
    }
    tiles.forEach(function (tile) {
        tile.y = rowsByName[tile.row].index;
        tile.x = columnsByName[tile.column].index;
        matrix[tile.y][tile.x] = tile;
        matrix[tile.y].title = tile.row;
        matrix[tile.y][tile.x].title = tile.column;
    });

    var margin = {top: $marginTop, right: 0, bottom: 0, left: $marginLeft},
        width = $width,
        height = $height;

    var svg = d3.select("#$svgId")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


    // The default sort order.
    var x = d3.scale.ordinal().rangeBands([0, width]).domain(d3.range(0, rows.length)),
        y = d3.scale.ordinal().rangeBands([0, height]).domain(d3.range(0, columns.length)),
        color = d3.scale.category10();


    svg.append("rect")
        .attr("class", "background")
        .attr("width", width)
        .attr("height", height);

    var row = svg.selectAll(".row")
        .data(matrix)
        .enter().append("g")
        .attr("class", "row")
        .attr("transform", function (d, i) {
            return "translate(0," + y(i) + ")";
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
            return rows[i].name;
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
            return columns[i].name;
        });

    function createRow(row) {
        d3.select(this).selectAll(".cell")
            .data(row.filter(function (d) {
                return d !== null;
            }))
            .enter().append("rect")
            .attr("class", "cell")
            .attr("x", function (d) {
                return x(d.x);
            })
            .attr("width", x.rangeBand())
            .attr("height", y.rangeBand())
            .style("fill-opacity", function (d) {
                return d.opacity;
            })
            .style("fill", function (d) {
                return color(d.color);
            })
            .on("mouseover", mouseover)
            .on("mouseout", mouseout);
    }

    function mouseover(p) {
        // d3.selectAll(".row text").classed("active", function(d, i) { return i == p.y; });
        // d3.selectAll(".column text").classed("active", function(d, i) { return i == p.x; });
        d3.select("#$svgId").selectAll(".row text").style("visibility", function (d, i) {
            return i == p.y ? "visible" : "hidden";
        });
        d3.select("#$svgId").selectAll(".column text").style("visibility", function (d, i) {
            return i == p.x ? "visible" : "hidden";
        });
    }

    function mouseout() {
        d3.select("#$svgId").selectAll(".row text").style("visibility", "visible");
        d3.select("#$svgId").selectAll(".column text").style("visibility", "visible");
    }
});