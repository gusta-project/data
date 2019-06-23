var Bottleneck = require("bottleneck/es5");

var FetchStream = require("fetch").FetchStream,
    fs = require("fs"),
    out;

const limiter = new Bottleneck({
    maxConcurrent: 1,
    minTime: 10000,
    reservoir: 80
});

var page = 1;

var url = '';

var trips = 80;

async function rover(){

    url = "https://alltheflavors.com/api/v2/flavors?page[number]=" + page + "&page[size]=100";

    out = fs.createWriteStream('./cache/flavors/atf'+ page +'.json');
    
    new FetchStream(url).pipe(out);

    console.log('URL: ' + url);

    page = page +1;
}

async function main () {

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
