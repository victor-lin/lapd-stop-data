// send request to URL extension
function sendRequest(ext) {
    $.post(ext, function(data) {
        console.log("Sending POST to " + ext);
    });
}

$(document).on('click', '#createSchema:enabled', function(){
    sendRequest('/_create_schema/');
});

$(document).on('click', '#addConstraints:enabled', function(){
    sendRequest('/_add_constraints/');
});

$(document).on('click', '#insertOfficers:enabled', function(){
    sendRequest("/_populate/Officer/");
});

$(document).on('click', '#insertStops:enabled', function(){
    sendRequest("/_populate/PoliceStop/");
});

$(document).on('click', '#insertOffenders:enabled', function(){
    sendRequest("/_populate/Offender/");
});

//drop all tables
$(document).on('click', '#dropTables:enabled', function(){
    sendRequest('/_drop_tables/');
});

// return string in RGBa notation
function hsvToRgb(h, s, v) {
  var r, g, b;
  var i;
  var f, p, q, t;

  // Make sure our arguments stay in-range
  h = Math.max(0, Math.min(360, h));
  s = Math.max(0, Math.min(100, s));
  v = Math.max(0, Math.min(100, v));

  // We accept saturation and value arguments from 0 to 100 because that's
  // how Photoshop represents those values. Internally, however, the
  // saturation and value are calculated from a range of 0 to 1. We make
  // That conversion here.
  s /= 100;
  v /= 100;

  if (s == 0) {
    // Achromatic (grey)
    r = g = b = v;
    return [Math.round(r * 255), Math.round(g * 255), Math.round(b * 255)];
  }

  h /= 60; // sector 0 to 5
  i = Math.floor(h);
  f = h - i; // factorial part of h
  p = v * (1 - s);
  q = v * (1 - s * f);
  t = v * (1 - s * (1 - f));

  switch (i) {
    case 0:
      r = v;
      g = t;
      b = p;
      break;

    case 1:
      r = q;
      g = v;
      b = p;
      break;

    case 2:
      r = p;
      g = v;
      b = t;
      break;

    case 3:
      r = p;
      g = q;
      b = v;
      break;

    case 4:
      r = t;
      g = p;
      b = v;
      break;

    default: // case 5:
      r = v;
      g = p;
      b = q;
  }

  rgb_str = "rgba(";
  rgb_str += Math.round(r * 255) + ",";
  rgb_str += Math.round(g * 255) + ",";
  rgb_str += Math.round(b * 255) + ",";
  rgb_str += "1)"
  return rgb_str;
};

// generate n contrasting colors
// return: list color strings in RGBa notation
function generateColors(num_colors) {
    var interval = 360 / (num_colors - 1);
    var colors = [];
    for (var i = 0; i < num_colors; i++) {
        colors.push(hsvToRgb(interval * i, 80, 90));
    }
    return colors;
}
