//import Bottleneck from 'bottleneck';

var Bottleneck = require("bottleneck/es5");

var FetchStream = require("fetch").FetchStream,
    fs = require("fs"),
    out;

const limiter = new Bottleneck({
    maxConcurrent: 1,
    minTime: 15000,
    reservoir: 1
});

var page = 1;

var url = '';

var trips = 1;

async function rover(){

    url = "https://alltheflavors.com/api/v2/vendors?page[number]=" + page + "&page[size]=100";

    out = fs.createWriteStream('./cache/vendors/atf'+ page +'.json');
    
    new FetchStream(url).pipe(out);

    console.log('URL: ' + url);

    page = page +1;
}
/*
limiter.schedule(() => rover())
.then((result) => {
  console.log('All done!');
});*/

async function main () {
    // sleeeeeeeeping  for a second 💤
    //result = await limiter.schedule(() => rover());
    const wrapped = await limiter.wrap(rover);
    
    while(trips > 0){
    
        const result = await wrapped();

        trips = trips -1;

        if(trips == 0){

            return result;
        }
    }
}

main()
.then(console.log)
.catch(console.error);

/*
while(trips > 0){

    limiter.schedule(() => rover());

    trips = trips -1;
}*/
/*var FetchStream = require("fetch").FetchStream,
    fs = require("fs"),
    out;

out = fs.createWriteStream('./cache/flavors/atf4.json');
new FetchStream("https://alltheflavors.com/api/v2/flavors?page=4").pipe(out);*/
/*
var trips = 10;

var page = 1;

var url = "";
*/
/*function myFunc(arg) {
    
    console.log(`arg was => ${arg}`);
}

while(trips > 0){
    
    if(trips !== 10){
        page = page + 1;
    }

    url = "https://alltheflavors.com/api/v2/flavors?page[number]=" + page + "&page[size]=100";

    setTimeout(myFunc, 5000, url);

    trips = trips -1;
}*/


//console.log('URL: ' + url);


