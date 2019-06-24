const pool = require('./pg');

var arr = require('../json/vendors/atf1.json');

/*
arr.forEach(function(vendor){
    //console.log(vendor.name + ' : ' + vendor.abbreviation);
})
*/
/*
var values = '';

arr.forEach(function(vendor){
    values = values + "(" + vendor.id + " , '" + vendor.abbreviation + "' , `" + vendor.name + "`), ";
});

pool.query("INSERT INTO vendor (id, code, name) VALUES " + values,
    (err, res) => {
        console.log(err, res)
        pool.end()
    }
);


console.log(values);
*/


const query = 'INSERT INTO vendor(id, code, name) VALUES($1, $2, $3)';

arr.forEach(async function(vendor){
    
    var values = [vendor.id, vendor.abbreviation, vendor.name];

    try {
        var res = await pool.query(query, values);
        console.log("Inserted "+vendor.name);
    } catch(err) {
        console.log(err.stack)
    }
});

pool.end();
