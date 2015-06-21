
//define connection string here
//if myria is down, how can we check?
connectionString = "";

//Dataset to use
var DEFAUL_USER = "public";

var massConversion = 18479300000000000.0;

var massAttr = "mass";
var currentTimeAttr = "currentTime";
var timeStepAttr = "timeStep";
var nowGroupAttr = "nowGroup";

// Set this to allow an option in range selection for custom user ranges
var is_input_valid = false;
var ALLOW_CUSTOM_MASS_RANGE = true;
var minMassRange = 50000000000.0;
var maxMassRange = 10000000000000.0;
var selectedMinMassRange = -1.0;
var selectedMinMassRange = -1.0;

var minTime = 0;
var maxTime = 7;
var massRatio = 5.0;

var selectedGroup = -1;
var mergerTree;
var USERNAME;
var PROGRAM; //"vulcan"
var NODES_RELATION; //"haloTableCompleteFinal"
var EDGES_RELATION; //"edgesTreeFinal"
var raw_times = []
//currently timeranges is not in use
var timeRanges = ["0.6", "1.3", "1.9", "2.3", "2.6", "3.4", "3.9", "4.5", "5.1", "5.8", "6.0", "6.4", "7.1", "7.7", "8.3", "8.8", "9.0", "10.3", "10.9", "10.9 ", "11.6", "12.2", "12.2 ", "12.5", "12.8", "13.5", "13.7"];

var setUp = function() {
  // Hide any loading gifs
  //showVizLoadingIcon(false);
  //showGroupLoadingIcon(false);

  // Populate selections for the various user inputs
  populateMassRangeDropdown();
  //populateMergerTime1Dropdown();
  //populateMergerTime2Dropdown();

  // Init placeholders with numerical values
  //document.getElementById("customMassMin").placeholder = minMassRange;
  //document.getElementById("customMassMax").placeholder = maxMassRange;
};

var validateUserInput = function(callback) {
  console.log("Has user input been validated (ValidateUserInput) " + is_input_valid);
  // if no change to user input, just use callback
  if (is_input_valid) {
    callback();
    return;
  }
  // must verify input again because it changed
  USERNAME = document.getElementById('mergerTreeUsername').value;
  NODES_RELATION = document.getElementById('mergerTreeNodesTable').value;
  EDGES_RELATION = document.getElementById('mergerTreeEdgesTable').value;
  console.log(USERNAME, NODES_RELATION, EDGES_RELATION);
  if (USERNAME == '' || NODES_RELATION == '' || EDGES_RELATION == '') {
      alert("Must enter a user name, nodes, and edges table");
      return;
  }
  if (USERNAME.indexOf(' ') != -1  || NODES_RELATION.indexOf(' ') != -1  || EDGES_RELATION.indexOf(' ') != -1 ) {
      alert("User name, nodes, and edges table can't contain spaces");
      return;
  }
  if (NODES_RELATION.split(':').length != 3 || EDGES_RELATION.split(':').length != 3) {
      alert("Nodes and edges relation must be of the form [username]:[program]:[table]. See examples at service.myria.cs.washington.edu.");
      return;
  }
  is_input_valid = true;
  showLoadingIcon(true);
  //callback (get groupIds) will turn icon off
  return $.get('/verifytables', {nodestable: NODES_RELATION, edgestable: EDGES_RELATION}, function(res) {
    if (res.verified == 'TRUE'){
      $.get('/myriaquery', {querytype: "init_timesteps", nodesTable: NODES_RELATION}, function(res2) {
        if (res2.query_status == 'ERROR') {
          alert("Something went wrong when finding the unique timesteps available");
        } else {
          console.log(res2.timesteps);
          setUniqueTimestamps(res2.timesteps);
          callback();
        }
      });
    } else {
      alert("Must enter a valid nodes and edges table (one of those tables doesn't exist). Check the datasets on service.myria.cs.washington.edu.");
    }
    showLoadingIcon(false);
  });
}

var userInputChange = function() {
  is_input_valid = false;
}

// Sets up the query for the merger tree from form data
var getSelectedMergerTree = function() {
  document.getElementById('mergerTreeViz').style.display = 'none';
  clearPreviousMergerTreeDisplay();
  selectedGroup = document.getElementById('mergerTreeGroups').value;
  if (selectedGroup == '') {
    alert("Must selected a group id");
    return;
  }
  console.log("sending get_mergertree");
  showLoadingIcon(true);
  return $.get('/myriaquery', {querytype: "get_mergertree", user: USERNAME, nodesTable: NODES_RELATION, edgesTable: EDGES_RELATION, nowGroupAttr: nowGroupAttr, group: selectedGroup}, function(res) {
    showLoadingIcon(false);
    if (res.query_status == 'ERROR') {
      alert("Something went wrong when retrieving the merger tree " + selectedGroup);
      return;
    } else {
      console.log(res);
      if (res.nodes.length == 0 || res.edges.length == 0) {
        alert('No result for group ' + selectedGroup + '. Must change computation parameters.');
        return;
      }
      document.getElementById('mergerTreeViz').style.display = 'block';
      populateMergerTree(res.nodes, res.edges);
    }
  });
};


// Run when user clicks the "generate groups" button
// Organizes when to go through which function - depends on which parameters are filled in
var getGroupIds = function() {
  var massSelection = document.getElementById("massSelection");
  var massChoice = massSelection.options[massSelection.selectedIndex].text;
  if (massChoice == "Custom") {
    console.log("Custom Mass");
    if (document.getElementById('customMassMin').value == '' || document.getElementById('customMassMax').value == '') {
      displayErrorMessage('Must enter an upper and lower bound mass range or select a default');
      return;
    }
    selectedMinMassRange = Number(document.getElementById('customMassMin').value);
    selectedMaxMassRange = Number(document.getElementById('customMassMax').value);

    if (selectedMinMassRange < 0 || selectedMinMassRange >= selectedMaxMassRange) {
      displayErrorMessage('Must enter a valid mass range');
      return;
    }
  } else { //not custom choice
    massRangeTuple = document.getElementById('massSelection').value;
    massRangeTuple = massRangeTuple.split(","); // Tuple is in string form, we split and next parse to int
    selectedMinMassRange = Number(massRangeTuple[0]).toFixed(2); // toFixed removes scientific notation
    selectedMaxMassRange = Number(massRangeTuple[1]).toFixed(2);
  }
  // Clears previous visualization results
  document.getElementById('mergerTreeViz').style.display = 'none';
  clearPreviousMergerTreeDisplay()
  showLoadingIcon(true);
  console.log("Sending Query, range: " + selectedMinMassRange +  " -> " +  selectedMaxMassRange);
  $.get('/myriaquery', {querytype: "get_nowgroups_by_mass", user: USERNAME, nodesTable: NODES_RELATION, timeStepAttr: timeStepAttr, massAttr: massAttr, nowGroupAttr: nowGroupAttr, maxMass: +selectedMaxMassRange, minMass: +selectedMinMassRange}, function(res) {
      populateGroupIdMenu(res);
    });
}

//This function queries and populates the list of available group ids
var populateGroupIdMenu = function(res) {
  console.log("inside populateGroupIdMenu");
  showLoadingIcon(false);
  if (res.query_status == 'ERROR') {
    alert('Error in retrieving groups');
    return;
  }
  groups = res.nowGroups;
  if (groups.length == 0) {
    alert('No groups in selected range');
    return;
  }
  if (groups.length > 250) {
    alert('There are ' + groups.length + ' possible groups, which is more than the browser can handle. We show only the first 250 groups by group number.');
    groups = groups.slice(0, 250);
  }
  groups.sort(sortNumber); //order from smallest to largest

  d3.select("#mergerTreeGroups")
    .selectAll("option")
    .data(groups)
    .enter().append("option")
    .attr("value", function(d){ return d[nowGroupAttr]; })
    .text(function(d) { return d[nowGroupAttr]; });
};

var setUniqueTimestamps = function(res) {
  res.forEach(function(d) {
    raw_times.push({"db": d.timeStep, "time": d.timeStep});
  });
}

var populateMergerTree = function(data1, data2) {
  data2.sort(function(obj1, obj2) {
    return obj1[currentTimeAttr] - obj2[currentTimeAttr]});
  data1.sort(function(obj1, obj2) {
    return obj1[timeStepAttr] - obj2[timeStepAttr];
  });
//displayMergerTree is in d3/interaction_collapse.js
mergerTree = new displayMergerTree(data2, data1, raw_times, selectedGroup);
};

var clearPreviousMergerTreeDisplay = function() {
  var previousContent = document.getElementById("svgContent");
  while (previousContent.firstChild) {
    previousContent.removeChild(previousContent.firstChild);
  }
  var massPanelPrevContent = document.getElementById("massPanel");
  var particlePanelPrevContent = document.getElementById("particlePanel");
  var legendPrevContent = document.getElementById("legend");
  while (massPanelPrevContent.firstChild) {
    massPanelPrevContent.removeChild(massPanelPrevContent.firstChild)
  }
  while (particlePanelPrevContent.firstChild) {
    particlePanelPrevContent.removeChild(particlePanelPrevContent.firstChild)
  }
  while (legendPrevContent.firstChild) {
    legendPrevContent.removeChild(legendPrevContent.firstChild)
  }
};

var updateMassRatio = function() {
  mergerTree.updateMassRatio();
}

var download = function() {
  mergerTree.download();
}

var resetTree = function() {
  mergerTree.resetTree();
}

var toggleGraphs = function() {
  mergerTree.toggleGraphs();
}

var toggleTooltips = function() {
  mergerTree.toggleTooltips();
}

//-------------------------- PRIVATE UI HELPERS ------------------------------//

var displayErrorMessage = function(message) {
  var errorSection = document.getElementById('error');
  errorSection.innerHTML = message;
};

var showLoadingIcon = function(show) {
  var loadingIcon = document.getElementById('loadingImg');
  if (show) {
    loadingIcon.style.display = "block";
  } else {
    loadingIcon.style.display = "none";
  }
};


// For drop down results sorting
function sortNumber(a,b) {
  return a[nowGroupAttr] - b[nowGroupAttr];
}

// When a user changes any of the options used to compute the groups
// We want to clear the groups they can select
var groupOptionsChanged = function() {
  document.getElementById("mergerTreeGroups").innerHTML="";
}

// When mass custom option is selected, show/hide custom mass inputs
var massRangeOptionsChanged = function() {
  document.getElementById("mergerTreeGroups").innerHTML="";

  var massSelection = document.getElementById("massSelection");
  var massChoice = massSelection.options[massSelection.selectedIndex].text;
  if (massChoice == "Custom") {
    document.getElementById("customMassRangeOptions").style.visibility = 'visible';
  } else {
    document.getElementById("customMassRangeOptions").style.visibility = 'hidden';
  }
}

// Show/hide major merger controls when checkbox is selected/deselected
var mergerCheckboxClicked = function() {
  if(document.getElementById('calculateMergerCheckbox').checked) {
    document.getElementById("mergerTreeMergerOptions").style.display = 'block';
  } else {
    document.getElementById("mergerTreeMergerOptions").style.display = 'none';
  }
}

var populateMassRangeDropdown = function() {
  var massRanges = [
  "5e9 to 1e10   (Dwarf Galaxies)",
  "1e10 to 5e11",
  "5e11 to 5e12   (Milky Way)",
  "5e12 to 1e13"
  ];

  // Add a custom option to drop down if applicable
  if (ALLOW_CUSTOM_MASS_RANGE) {
    massRanges.push("Custom")
  }

  var select = document.getElementById("massSelection");

  // Parses out each item in the above array and creates
  // an elmement in the mass range selection input for that range
  for (var i = 0; i < massRanges.length; i++) {
    var testString = massRanges[i];
    var element = document.createElement("option");
    element.textContent = massRanges[i];
    var massRangeValues = massRanges[i].split(' to ');
    if(massRangeValues[1] == undefined) {
      massRangeValues[1] = ' ';
    }
    element.value = [massRangeValues[0], massRangeValues[1].split(" ")[0]]; // Gets just the number
    // Default to Milky Way
    if (testString.indexOf("Milky Way") != -1) {
      element.selected = true;
    }
    select.appendChild(element);
  }
}


var YearsToTimestep = function(year) {
  return maxTime - _.indexOf(timeRanges, year);
}

//calendar
var updateCalendarWarning = function() {
  var apiKey = "AIzaSyCIB8MWWVeix26boS_WLJGmW41A9oNj8fw";
  var calId = "cs.washington.edu_i1gk4il65dj31mcfgid1t9t1o8@group.calendar.google.com";

  var now = new Date(),
  soon = (new Date()).addHours(6),
  later = (new Date()).addDays(2);

// warn if there are experiments running
$.ajax({
  url: "https://www.googleapis.com/calendar/v3/freeBusy?key=" + apiKey,
  type: "POST",
  data: JSON.stringify({
    "timeMin": (now).toISOString(),
    "timeMax": (later).toISOString(),
    "timeZone": "UTC",
    "items": [
    {
      "id": calId
    }
    ]
  }),
  contentType: "application/json; charset=utf-8",
  dataType: "json",
  success: function(data){
    var message = '',
    start = later,
    end = now;

// filter for events happening now
var busyNow = _.filter(data.calendars[calId].busy, function(b) {
  var busy = new Date(b.start) < now && new Date(b.end) > now;
  if (busy && new Date(b.end) > new Date(end))
    end = b.end;
  return busy;
}).length > 0;

// filter by overlap with now and soon
var busySoon = _.filter(data.calendars[calId].busy, function(b) {
  if (new Date(b.start) < new Date(start))
    start = b.start;
  return new Date(b.start) < soon && new Date(b.end) > now;
}).length > 0;

// filter by overlap with now and later
var busyLater = _.filter(data.calendars[calId].busy, function(b) {
  if (new Date(b.start) < new Date(start))
    start = b.start;
  return new Date(b.start) < later && new Date(b.end) > now;
}).length > 0;

$("#calendar-alert").remove();

if (busyNow) {
  message = '<div id="calendar-alert" class="alert alert-danger" role="alert"><strong>The Myria cluster is reserved for research experiments right now</strong>. Please don\'t use it! It will be available <abbr class="timeago" title="' + end + '">' + end + '</abbr>.'
} else if (busySoon) {
  message = '<div id="calendar-alert" class="alert alert-warning" role="alert"><strong>Myria will be reserved for research experiments soon</strong>. The reservation will begin <abbr class="timeago" title="' + start + '">' + start + '</abbr>. Please only submit queries that will finish well before that time.'
} else if (busyLater) {
  message = '<div id="calendar-alert" class="alert alert-info" role="alert"><strong>There is an upcoming reservation for research experiments</strong>. The reservation will begin <abbr class="timeago" title="' + start + '">' + start + '</abbr>.'
} else {
  return;
}

$("#page-body").prepend(message + ' For more information, please check the <a target="_blank" href="https://www.google.com/calendar/embed?src=cs.washington.edu_i1gk4il65dj31mcfgid1t9t1o8%40group.calendar.google.com&ctz=America/Los_Angeles&mode=week">calendar</a>.</div>');
jQuery("abbr.timeago").timeago();
}
});
}

$(function() {
  jQuery.timeago.settings.allowFuture = true;

  Date.prototype.addHours= function(h){
    this.setHours(this.getHours()+h);
    return this;
  }

  Date.prototype.addDays= function(d){
    this.setHours(this.getHours()+24*d);
    return this;
  }

//warn if not in Chrome
if (!window.chrome) {
  $("#page-body").prepend('<div class="alert alert-danger alert-dismissible" role="alert"><button type="button" class="close" data-dismiss="alert"><span aria-hidden="true">&times;</span><span class="sr-only">Close</span></button><strong>Warning!</strong> Myria is developed and tested in Google Chrome, and other browsers may not support all the features.</div>');
}

//warn if backend is not available
if (connectionString.indexOf('error') === 0) {
  $("#page-body").prepend('<div class="alert alert-danger alert-dismissible" role="alert"><strong>Error!</strong> Unable to connect to Myria. Most functionality will not work.</div>');
}

window.setInterval(updateCalendarWarning, 5 * 60 * 1000);
updateCalendarWarning();

//back to top button
var offset = 220;
var duration = 300;
$('.back-to-top').hide();
$(window).scroll(function() {
  if ($(this).scrollTop() > offset) {
    $('.back-to-top').fadeIn(duration);
  } else {
    $('.back-to-top').fadeOut(duration);
  }
});

$('.back-to-top').click(function(event) {
  event.preventDefault();
  $('html, body').animate({scrollTop: 0}, duration);
  return false;
});

$("[data-toggle=tooltip]").tooltip();

$("abbr.timeago").timeago();
});
