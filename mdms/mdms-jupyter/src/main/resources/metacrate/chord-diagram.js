// Input section.
var groups = $groups,
    transitions = $transitions,
    diameter = $diameter,
    svgId = "$svgId";

// Calculate the transition matrix.
var matrix = [];
for (var y = 0; y < groups.length; y++) {
    var source = groups[y];
    var row = [];
    for (var x = 0; x < groups.length; x++) {
        var destination = groups[x];
        var sum = 0;
        for (var i = 0; i < transitions.length; i++) {
            var transition = transitions[i];
            if (transition.source === source.name && transition.destination === destination.name) sum += transition.value;
        }
        row.push(sum);
    }
    matrix.push(row);
}

var width = diameter,
    height = diameter,
    outerRadius = Math.min(width, height) / 2 - 10,
    innerRadius = outerRadius - 24;

var color = d3.scale.category10();

var formatTransition = d3.format(".1f");

var arc = d3.svg.arc()
    .innerRadius(innerRadius)
    .outerRadius(outerRadius);

var layout = d3.layout.chord()
    .padding(.01)
    .sortSubgroups(d3.descending)
    .sortChords(d3.ascending);

var path = d3.svg.chord()
    .radius(innerRadius);

var svg = d3.select("#" + svgId)
    .attr("width", width)
    .attr("height", height)
    .append("g")
    .attr("id", "circle")
    .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");

svg.append("circle")
    .attr("r", outerRadius);


// Compute the chord layout.
layout.matrix(matrix);

// Add a group per neighborhood.
var group = svg.selectAll(".group")
    .data(layout.groups)
    .enter().append("g")
    .attr("class", "group")
    .on("mouseover", mouseover)
    .on("mouseout", mouseout);

// Add a mouseover title.
group.append("title").text(function(d, i) {
    return groups[i].name + ": " + formatTransition(d.value);
});

// Add the group arc.
var groupPath = group.append("path")
    .attr("id", function(d, i) { return "group" + i; })
    .attr("d", arc)
    .style("fill", function(d, i) { return color(groups[i].name); });

// Add a text label.
var groupText = group.append("text")
    .attr("x", 6)
    .attr("dy", 15);

groupText.append("textPath")
    .attr("xlink:href", function(d, i) { return "#group" + i; })
    .text(function(d, i) { return groups[i].name; });

// Remove the labels that don't fit. :(
groupText.filter(function(d, i) { return groupPath[0][i].getTotalLength() / 2 - 16 < this.getComputedTextLength(); })
    .remove();

// Add the chords.
var chord = svg.selectAll(".chord")
    .data(layout.chords)
    .enter().append("path")
    .attr("class", "chord")
    .style("fill", function(d) { return color(groups[d.source.index].name); })
    .attr("d", path);

// Add an elaborate mouseover title for each chord.
chord.append("title").text(function(d) {
    return groups[d.source.index].name
        + " → " + groups[d.target.index].name
        + ": " + formatTransition(d.source.value)
        + "\n" + groups[d.target.index].name
        + " → " + groups[d.source.index].name
        + ": " + formatTransition(d.target.value);
});

function mouseover(d, i) {
    chord.classed("fade", function(p) {
        return p.source.index != i
            && p.target.index != i;
    });
}

function mouseout(d, i) {
    chord.classed("fade", false);
}