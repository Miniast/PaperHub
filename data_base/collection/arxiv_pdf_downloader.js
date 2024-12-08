import Crawler from "crawler";
import fs from "fs";
import path from "path";
import { getLogger } from "./utils/tslog.js";
import dayjs from "dayjs";
import Papa from "papaparse";

const fileName = "arxiv_csv_downloader";
const resultDir = "./result";
const pdfDir = path.resolve(resultDir, "pdf");
const logDir = "./log";
const logFile = path.resolve(logDir, `${fileName}_${dayjs().format("YYYY-MM-DD")}.log`);
const log = getLogger(fileName, logFile);
const ProxyManager = {
    index: 0,
    proxies: JSON.parse(fs.readFileSync("./utils/proxies_dd.json")),
    setProxy: function (options) {
        let proxy = this.proxies[this.index];
        this.index = (++this.index) % this.proxies.length;
        options.proxy = proxy;
        options.rateLimiterId = Math.floor(Math.random() * 25);
    }
}

let startDate, endDate;

class Task {
    constructor() {
        [startDate, endDate] = process.argv.slice(2);
        if (!startDate || !endDate || !dayjs(startDate).isValid() || !dayjs(endDate).isValid() || dayjs(startDate).isAfter(dayjs(endDate))) {
            log.error(`Use node arxiv_csv_downloader.js <startDate> <endDate>, the format of date should be YYYY-MM-DD`);
            process.exit(1);
        }
        this.crawler = new Crawler({
            rateLimit: 1000,
            timeout: 90000,
            headers: {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "content-type": "application/json",
            },
        });
        this.crawler.on("drain", () => {
            log.info(`Crawing complete.`);
        }).on("schedule", options => ProxyManager.setProxy(options));
    }

    start() {
        if (!fs.existsSync(pdfDir)) fs.mkdirSync(pdfDir, { recursive: true });
        if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });
        const csvFile = `${resultDir}/arxiv_info_${startDate}_to_${endDate}.csv`;

        log.info(`Job start. Getting pdf from list ${csvFile}..`);
        log.info("See complete logs in file: " + logFile);

        const pdfStore = new Set();
        fs.readdirSync(pdfDir).map(file => {
            if (file.endsWith(".pdf")) {
                pdfStore.add(file.substring(0, file.length - ".pdf".length));
            }
        });

        const codeSet = new Set();
        const records = Papa.parse(fs.readFileSync(csvFile, "utf8"), { header: true }).data;
        records.forEach(row => {
            codeSet.add(row.arxiv_code);
        });
        log.info(`Total ${codeSet.size} pdfs to download.`);

        codeSet.forEach(code => {
            const url = `https://arxiv.org/pdf/${code}.pdf`;

            if (!pdfStore.has(code)) {
                this.crawler.add({
                    url,
                    jQuery: false,
                    callback: this.download_pdf,
                    encoding: null,
                    userParams: { code },
                });
            } else {
                log.info(`"${code}.pdf" already exist.`);
            }
        });
    }

    download_pdf = (err, res, done) => {
        const { code } = res.options.userParams;
        if (err) {
            if (err.code === "ETIMEDOUT") {
                this.crawler.add(res.options);
            }
            return done();
        }
        fs.writeFileSync(`${pdfDir}/${code}.pdf`, res.body);
        log.info(`"${code}.pdf" downloaded.`);
        return done();
    }
}

const task = new Task();
task.start();
