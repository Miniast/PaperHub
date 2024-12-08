import Crawler from "crawler";
import fs from "fs";
import path from "path";
import { getLogger } from "./utils/tslog.js";
import dayjs from "dayjs";
import papa from "papaparse";

const fileName = "arxiv_info";
const resultDir = "./result";
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

const headers = ['arxiv_code', 'title', 'tags', 'authors', 'submit_date', 'comments', 'url', 'abstract'];
const fields = [
    "Artificial Intelligence",
    "Computation and Language",
    "Computational Engineering, Finance, and Science",
    "Computer Vision and Pattern Recognition",
    "Databases",
    "Distributed, Parallel, and Cluster Computing",
    "Graphics",
    "Information Retrieval",
    "Machine Learning",
    "Networking and Internet Architecture",
];

let startDate, endDate;
let resultFile = path.resolve(resultDir, `${fileName}`);

class Task {
    constructor() {
        [startDate, endDate] = process.argv.slice(2);
        if (!startDate || !endDate || !dayjs(startDate).isValid() || !dayjs(endDate).isValid() || dayjs(startDate).isAfter(dayjs(endDate))) {
            log.error(`Use node arxiv_info.js <startDate> <endDate>, the format of date should be YYYY-MM-DD`);
            process.exit(1);
        }
        resultFile += `_${startDate}_to_${endDate}.csv`;
        this.taskNumber = 0;
        this.crawler = new Crawler({
            http2: true,
            rejectUnauthorized: false,
            rateLimit: 1000,
            timeout: 60000,
            headers: {
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            },
        });
        this.crawler.on("drain", () => {
            log.info(`Crawing complete.`);
        }).on("schedule", options => {
            ProxyManager.setProxy(options)
        }); 
    }

    start() {
        if (!fs.existsSync(resultDir)) fs.mkdirSync(resultDir, { recursive: true });
        if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });
        log.info("Job start. See complete logs in file: " + logFile);
        log.info("See result in file: " + resultFile);

        if (!fs.existsSync(resultFile) || fs.readFileSync(resultFile, "utf-8").trim() === "") {
            fs.writeFileSync(
                resultFile,
                papa.unparse([headers]) + "\n"
            );
        } else {
            const records = fs.readFileSync(resultFile, "utf-8").trim().split("\n").length - 1;
            log.warn(
                `File "${resultFile}" already exists and has ${records} records, now it is not allowed.`
            );
            process.exit(1);
        }
        log.info(`Crawling the data from ${startDate} to ${endDate} on: ${fields.join(", ")}`);

        for (let field of fields) {
            this.getPaparsByFields(field);
        }
    }

    getPaparsByFields(field) {
        const urlPrefix = "https://arxiv.org/search/advanced";
        const searchParams = {
            "advanced": "",
            "terms-0-operator": "AND",
            "terms-0-term": field,
            "terms-0-field": "cross_list_category",
            "classification-computer_science": "y",
            "classification-physics_archives": "all",
            "classification-include_cross_list": "exclude",
            "date-year": "",
            "date-filter_by": "date_range",
            "date-from_date": startDate,
            "date-to_date": endDate,
            "date-date_type": "submitted_date",
            "abstracts": "show",
            "size": 200,
            "order": "announced_date_first",
            "start": 0,
        };

        this.crawler.add({
            url: urlPrefix,
            searchParams,
            callback: this.getPapersByPage,
        });
    }

    getPapersByPage = (err, res, done) => {
        this.taskNumber++;
        if (err) {
            this.crawler.add(res.options);
            return done();
        }

        const queryParams = res.options.searchParams;
        const $ = res.$;
        const pageTitle = $("main#main-container h1.title.is-clearfix").text().trim();

        if (pageTitle.startsWith("Sorry")) {
            return done();
        }

        const totalStr = pageTitle.match(/(\d+(,\d+)*)/g).pop();
        const total = parseInt(totalStr.replace(/,/g, ""));

        if (total >= 10000) {
            // 如果记录数超过10000，二分分片
            log.info(`"${queryParams["terms-0-term"]}" From ${queryParams["date-from_date"]} to ${queryParams["date-to_date"]} has ${total} records, spliting..`);
            const startDate = dayjs(queryParams["date-from_date"]);
            const endDate = dayjs(queryParams["date-to_date"]);
            const durations = endDate.diff(startDate, "day");
            const mid_date = startDate.add(Math.floor(durations / 2), "day");
            const firstQueryParams = {
                ...queryParams,
                start: 0,
                "date-from_date": startDate.format("YYYY-MM-DD"),
                "date-to_date": mid_date.format("YYYY-MM-DD"),
            };
            const secondQueryParams = {
                ...queryParams,
                start: 0,
                "date-from_date": mid_date.format("YYYY-MM-DD"),
                "date-to_date": endDate.format("YYYY-MM-DD"),
            };

            this.crawler.add({
                url: res.options.url,
                searchParams: firstQueryParams,
                callback: this.getPapersByPage,
            });
            this.crawler.add({
                url: res.options.url,
                searchParams: secondQueryParams,
                callback: this.getPapersByPage,
            });
        } else {
            // 如果记录数不超过10000，直接爬取
            const papers = $("main#main-container li.arxiv-result");
            const realRecords = this.parsePaper(papers, this.taskNumber, $);

            if (queryParams["start"] == 0) {
                log.info(`The total number of new papers is ${total}. From ${queryParams["date-from_date"]} to ${queryParams["date-to_date"]} on "${queryParams["terms-0-term"]}"`);

                if (queryParams["size"] < total) {
                    for (let i = queryParams["size"]; i < total; i += queryParams["size"]) {
                        const newQueryParams = { ...queryParams, start: i };
                        this.crawler.add({
                            url: res.options.url,
                            searchParams: newQueryParams,
                            callback: this.getPapersByPage,
                        });
                    }
                }
            }

            if (queryParams["start"] + queryParams["size"] >= total) {
                if (realRecords < total % 200) {
                    log.error(
                        `Task ${this.taskNumber} error: The number of records is ${total % 200}, but only ${realRecords} records were parsed`
                    );
                }
            } else {
                if (realRecords < queryParams["size"]) {
                    log.error(
                        `Task ${this.taskNumber} error: The number of records is 200, but only ${realRecords} records were parsed`
                    );
                }
            }
        }
        return done();
    }

    parsePaper(papers, paperNumber, $) {
        papers.map((_, paper) => {
            const url = $(paper).find("p.list-title.is-inline-block a").attr("href").trim();
            const arxiv_code = url.split("/").pop();
            const title = $(paper).find("p.title.is-5.mathjax").text().trim();
            const labels_array = $(paper).find("div.tags.is-inline-block span").get().map(tag => $(tag).attr("data-tooltip"));
            const labels = labels_array.sort().join(",");
            const authors = $(paper).find("p.authors a").get().map(author => $(author).text()).join(",");
            const total_date_str = $(paper).find("p.is-size-7:contains('Submitted')").text().trim();
            const submit_date_str = total_date_str.split(";")[0].substring("Submitted ".length);
            const last_submit = dayjs(submit_date_str, "DD MMM YYYY").format("YYYY-MM-DD");
            const comments = $(paper).find("p.comments.is-size-7 span").eq(1).text().trim();

            let abstract = $(paper).find("span.abstract-full.has-text-grey-dark.mathjax").text().trim();
            const lastLineIndex = abstract.lastIndexOf("\n");
            abstract = abstract.substring(0, lastLineIndex);

            const result = {
                arxiv_code,
                title,
                labels,
                authors,
                submit_date: last_submit,
                comments,
                url,
                abstract,
            }
            fs.appendFileSync(
                resultFile,
                papa.unparse([result], { header: false }) + "\n"
            );
        });
        log.info(`Task ${paperNumber} is done, ${papers.length} records are parsed.`);
        return papers.length;
    }
}

const task = new Task();
task.start();