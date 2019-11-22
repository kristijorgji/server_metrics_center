require('dotenv').config();
const fs = require('fs');
const mysql = require('mysql');
const { walkDirFilesSyncRecursive } = require("./utils");
const logger = require('pino')({
    level: process.env.LOG_LEVEL || 'info',
    prettyPrint: {
        colorize: true,
        translateTime: true,
        ignore: 'pid,hostname'
    }
});

const hrstart = process.hrtime();

const argv = require('yargs')
    .usage('Usage: $0 <command> [options]')
    .example('$0 --inputPath logs/', 'Parse the logs and  set into storage')
    .nargs('inputPath', 1)
    .describe('inputPath', 'Folder path of the logs')
    .describe('batchSize', 'The number of rows to insert in one go')
    .describe('deleteOnEnd', 'If set to true, after processing all the logs will be deleted')
    .demandOption(['inputPath'])
    .help('h')
    .alias('h', 'help')
    .epilog('@Kristi Jorgji - 2019')
    .argv;

const BATCH_SIZE = argv.batchSize ||  1000;
const SHOULD_DELETE_ON_END = argv.deleteOnEnd || false;
const INPUT_PATH = argv.inputPath;

logger.info(`Starting with batch size of ${BATCH_SIZE}, input path ${INPUT_PATH}, deleteOnEnd is set to ${SHOULD_DELETE_ON_END}`);

const dateTimePattern = /(\d{4}-\d{2}-\d{2}.{15})/;
const dataPattern = /Mem: *(\d+) *(\d+) *(\d+) *(\d+) *(\d+) *(\d+)/;

const con = mysql.createConnection({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE
});

const snooze = ms => new Promise(resolve => setTimeout(resolve, ms));

con.connect(function(err) {
    if (err) {
        throw err;
    }

    logger.debug("Connected to mysql!");
    start();
});

async function start() {
    const processingPromises = [];
    let i = 0;
    let linesNr = 0;
    let insertedRowsIntoDb = 0;

    const files = walkDirFilesSyncRecursive(INPUT_PATH);
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
        logger.debug('waiting for processing files', Object.keys(processingPromises).length);
        await snooze(500);
    }

    con.destroy();

    if (SHOULD_DELETE_ON_END) {
        logger.info('Will delete log files');
        for (file of files) {
            fs.unlinkSync(file.fullPath);
            logger.debug(`Deleted ${file.fullPath}`);
        }
    }

    const hrend = process.hrtime(hrstart);
    logger.info('Total Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000);
    logger.info('Total Lines processed:', linesNr);
    logger.info('Total Rows inserted to mysql', insertedRowsIntoDb);
}

function processFile(path) {
    return new Promise((resolve, reject) => {
        logger.debug(`Processing ${path}`);
        const readStream = fs.createReadStream(path, {encoding: 'utf8'});

        let linesNr = 0;
        let formedIndex = 0;
        let lastPreviousLine;
        let insertedRowsIntoDb = 0;
        const temp = {};
        const insertPromises = {};
        let queued = [];

        readStream.on('data', handleData);

        function handleData(data) {
            logger.trace('handleData');
            const lines = data.split('\n');
            const currentLinesNr = lines.length;
            if (lastPreviousLine) {
                lines[0] = `${lastPreviousLine}${lines[0]}`;
            }
            lastPreviousLine = lines[currentLinesNr - 1];
            delete lines[currentLinesNr - 1];
            handleLines(lines);
            logger.trace('after handleLines');
        }

        function handleLines(lines) {
            logger.trace(`handleLines ${formedIndex}`);
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

                        queued.push(temp[formedIndex]);
                        if (queued.length === BATCH_SIZE) {
                            const promiseKey = formedIndex;
                            insertPromises[formedIndex] = insertBatch(queued);
                            insertPromises[formedIndex].then(() => {
                                delete insertPromises[promiseKey]
                            });
                            logger.trace('after insertBatch');
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

            logger.debug('pending inserts', Object.keys(insertPromises).length);

            while (Object.keys(insertPromises).length > 0) {
                logger.debug('waiting for pending inserts', Object.keys(insertPromises).length);
                await snooze(500);
            }
            await insertBatch(queued);

            readStream.destroy();

            const hrend = process.hrtime(hrstart);
            logger.debug('Execution time (hr): %ds %dms', hrend[0], hrend[1] / 1000000);
            logger.debug('Lines processed:', linesNr);
            logger.debug('Rows inserted to mysql', insertedRowsIntoDb);

            resolve({
                filePath: path,
                linesNr,
                insertedRowsIntoDb,
            });
        });

        function insertBatch(batch) {
            logger.trace('insertBatch');
            return new Promise(function(resolve, reject) {
                const sql = `INSERT INTO metrics (date, used_memory, free_memory) VALUES ${batch.map(row =>  `('${row.date}', ${row.used}, ${row.free})`).join(',')}`;
                const recordsToBeInserted = batch.length;
                con.query(sql, function (err, result) {
                    if (err) {
                        reject(err);
                    } else {
                        logger.debug(`${recordsToBeInserted} records inserted`);
                        insertedRowsIntoDb += recordsToBeInserted;
                        resolve();
                    }
                });
            });
        }
    });
}
