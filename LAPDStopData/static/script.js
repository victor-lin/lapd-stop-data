function initDB() {
    $.post("/_initdb/", function(data) {
        console.log("Sending POST to /_initdb/")
        // data = $.parseJSON(data);
    });
}

function dropTables() {
    $.post("/_droptables/", function(data) {
        console.log("Sending POST to /_droptables/")
        // data = $.parseJSON(data);
    });
}
