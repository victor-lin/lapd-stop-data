// send request to URL extension
function sendRequest(ext) {
    $.post(ext, function(data) {
        console.log("Sending POST to " + ext);
        return data;
    });
}

$(document).on('click', '#createSchema:enabled', function(){
    sendRequest('/_create_schema/');
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
