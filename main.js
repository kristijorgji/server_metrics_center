const fs = require('fs');
const mysql = require('mysql');
const { walkDirFilesSyncRecursive } = require("./utils");

const con = mysql.createConnection({
    host: "docker.dev",
    user: "root",
    password: "Test123@",
    database: "sever_metrics"
});

const DEBUG = true;
const VERBOSE = false;
const hrstart = process.hrtime();

const DELIMITER = ';';
const dateTimePattern = /(\d{4}-\d{2}-\d{2}.{15})/;
const dataPattern = /Mem: *(\d+) *(\d+) *(\d+) *(\d+) *(\d+) *(\d+)/;

const DIRECTORY_PATH = './metrics/memory';
const CSV_PATH = 'parsed.csv';


// const content = fs.readFileSync(RAW_PATH, 'utf8');
// console.log(content.split('\n').length);
// return;
const snooze = ms => new Promise(resolve => setTimeout(resolve, ms));

con.connect(function(err) {
    if (err) {
        throw err;
    }

    if (DEBUG) console.log("Connected to mysql!");
    start();
});

async function start() {
    const processingPromises = [];
    let i = 0;
    let linesNr = 0;
    let insertedRowsIntoDb = 0;

    const files = walkDirFilesSyncRecursive(DIRECTORY_PATH);
    files.forEach(file => {
        const promiseKey = i;
        processingPromises[promiseKey] = processFile(file.fullPath);
        processingPromises[promiseKey].then(result => {
            delete processingPromises[promiseKey];
            linesNr += result.linesNr;
            insertedRowsIntoDb += result.insertedRowsIntoDb;
        });
        i++;
    });

    while (Object.keys(processingPromises).length > 0) {
        if (DEBUG) {
            console.log('waiting for processing files', Object.keys(processingPromises).length);
        }
        await snooze(500);
    }

    con.destroy();

    const hrend = process.hrtime(hrstart);
    console.info('Total Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000);
    console.log('Total Lines processed:', linesNr);
    console.log('Total Rows inserted to mysql', insertedRowsIntoDb);
}

function processFile(path) {
    return new Promise((resolve, reject) => {
        if (DEBUG) console.log(`Processing ${path}`);
        const readStream = fs.createReadStream(path, {encoding: 'utf8'});
        // const writeStream = fs.createWriteStream(CSV_PATH, {
        //     flags: 'w'
        // });

        let linesNr = 0;
        let formedIndex = 0;
        let lastPreviousLine;
        let insertedRowsIntoDb = 0;
        const temp = {};
        const insertPromises = {};
        let queued = [];

        readStream.on('data', handleData);

        function handleData(data) {
            if (DEBUG && VERBOSE) { console.log('handleData')}
            const lines = data.split('\n');
            const currentLinesNr = lines.length;
            if (lastPreviousLine) {
                lines[0] = `${lastPreviousLine}${lines[0]}`;
            }
            lastPreviousLine = lines[currentLinesNr - 1];
            delete lines[currentLinesNr - 1];
            handleLines(lines);
            if (DEBUG && VERBOSE) { console.log('after handleLines')}
        }

        function handleLines(lines) {
            if (DEBUG && VERBOSE) { console.log(`handleLines ${formedIndex}`);}
            const count = lines.length;
            for (let i = 0; i < count; i++) {
                const line = lines[i];
                const dateTimeMatch = dateTimePattern.exec(line);
                if (dateTimeMatch) {
                    temp[formedIndex] = {
                        time: Date.parse(dateTimeMatch[0]) / 1000,
                        date: new Date(dateTimeMatch[0]).toISOString().slice(0, 19).replace('T', ' '),
                    };
                } else {
                    const dataMatch = dataPattern.exec(line);
                    if (dataMatch) {
                        if ((!temp[formedIndex])) {
                            throw new Error(formedIndex);
                        }
                        temp[formedIndex].used = dataMatch[2];
                        temp[formedIndex].free = dataMatch[3];
                        //writeStream.write(Object.values(temp[formedIndex]).join(DELIMITER) + '\n');
                        queued.push(temp[formedIndex]);
                        if (queued.length === 1000) {
                            const promiseKey = formedIndex;
                            insertPromises[formedIndex] = insertBatch(queued);
                            insertPromises[formedIndex].then(() => {
                                delete insertPromises[promiseKey]
                            });
                            if (DEBUG && VERBOSE) console.log('after insertBatch');
                            queued = [];
                        }
                        delete temp[formedIndex];
                        formedIndex++;
                    }
                }
            }
            linesNr += count;
        }

        readStream.on('end', async () => {
            if (lastPreviousLine) {
                await handleLines([].push(lastPreviousLine), true);
            }

            if (DEBUG) {
                console.log('pending inserts', Object.keys(insertPromises).length);
            }

            while (Object.keys(insertPromises).length > 0) {
                if (DEBUG) {
                    console.log('waiting for pending inserts', Object.keys(insertPromises).length);
                }
                await snooze(500);
            }
            await insertBatch(queued);

            readStream.destroy();
            //writeStream.end();

            if (DEBUG) {
                const hrend = process.hrtime(hrstart);
                console.info('Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000);
                console.log('Lines processed:', linesNr);
                console.log('Rows inserted to mysql', insertedRowsIntoDb);
            }

            resolve({
                filePath: path,
                linesNr,
                insertedRowsIntoDb,
            });
        });

        function insertBatch(batch) {
            if (DEBUG && VERBOSE) { console.log('insertBatch')}
            return new Promise(function(resolve, reject) {
                const sql = `INSERT INTO metrics (date, used_memory, free_memory) VALUES ${batch.map(row =>  `('${row.date}', ${row.used}, ${row.free})`).join(',')}`;
                const recordsToBeInserted = batch.length;
                con.query(sql, function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        if (DEBUG) {  console.log(`${recordsToBeInserted} records inserted`); }
                        insertedRowsIntoDb += recordsToBeInserted;
                        resolve();
                    }
                });
            });
            //
            // return new Promise(function(resolve, reject) {
            //     insertedRowsIntoDb += batch.length;
            //     resolve();
            // });
        }
    });
}
