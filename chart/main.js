(function ($) {

  var margin = {top: 20, right: 20, bottom: 30, left: 75},
      width = $(document).width() - margin.left - margin.right,
      height = $(document).height() - margin.top - margin.bottom;

  var dx = [0, 1000, 2000, 5000, 10000, 100000, 10000000];
  var dy = [1000000, 100000, 10000, 5000, 2500, 1000, 0];

  function sections(n, length, padding) {
    var result = [];
    for (var i = 0; i < length; i++) {
      result.push(n * i / length + padding);
    }

    return result;
  }

  var x = d3.scale.linear()
      .domain(dx)
      .range(sections(width, dx.length, 0));

  var y = d3.scale.linear()
      .domain(dy)
      .range(sections(height, dy.length, 50));

  var xAxis = d3.svg.axis()
      .scale(x)
      .tickValues(dx)
      .orient("bottom");

  var yAxis = d3.svg.axis()
      .scale(y)
      .tickValues(dy)
      .orient("left");

  var line = d3.svg.line()
      .x(function(d) { return x(d.index); })
      .y(function(d) { return y(d.number); });

  var svg = d3.select("body").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

  d3.tsv("data.tsv", function(error, data) {
    if (error) throw error;

    data.forEach(function(d) {
      d.index = (+d.index) * 500;
      d.number = +d.number;
    });

    svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + (height - 7)  + ")")
      .call(xAxis);

    svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
      .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Num Repos");

    svg.append("path")
      .datum(data)
      .attr("class", "line")
      .attr("d", line);
  });

}(jQuery));
