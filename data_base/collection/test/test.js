import Crawler from "crawler";
import fs from "fs";
import path from "path";
import { getLogger } from "../lib/tslog.js";
import dayjs from "dayjs";
import papa from "papaparse";
import urlList from "./data/adidas.json" assert { type: "json" };

const resultDir = "./result";
const logDir = "./log";
const log = getLogger("adidas", path.resolve(logDir, `adidas_${dayjs().format("YYYY-MM-DD")}.log`));
const ProxyManager = {
    index: 0,
    proxies: JSON.parse(fs.readFileSync("../lib/proxies_dd.json")),
    setProxy: function (options) {
        let proxy = this.proxies[this.index];
        this.index = ++this.index % this.proxies.length;
        options.proxy = proxy;
        options.rateLimiterId = Math.floor(Math.random() * 10);
    },
};
const prefix = "https://www.adidas.com/api/plp/content-engine";
const identifier = "adidas";
const resultFile = path.resolve(resultDir, `${identifier}_${dayjs().format("YYYY-MM-DD")}.csv`);

class Task {
    constructor() {
        this.crawler = new Crawler({
            jQuery: false,
            isJson: true,
            http2: true,
            rejectUnauthorized: false,
            timeout: 30000,
            rateLimit: 1000,
            headers: {
                "user-agent":
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
                "content-type": "application/json",
                "accept": "*/*",
                "priority": "u=1, i",
            },
        });
        this.crawler.on("drain", () => {
            log.info(`Task Complete.`);
        }).on("schedule", options => {
            ProxyManager.setProxy(options);
            // options.proxy = "http://192.168.99.109:8888";
            // options.rateLimiterId = Math.floor(Math.random() * 10);
            // options.proxy = "http://s5.proxy.mayidaili.com:8123";
        });
    }

    start() {
        if (!fs.existsSync(resultDir)) fs.mkdirSync(resultDir, { recursive: true });
        if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

        const headers = ["type", "productId", "modelId", "displayName", "price", "salePrice", "salePercentage", "rating", "ratingCount", "link", "onlineFrom", "division", "category", "customBadge"];
        fs.writeFileSync(
            resultFile,
            papa.unparse([headers]) + "\n"
        );

        log.info(`API start.`);
        urlList.forEach(url => {
            this.crawler.add({
                url: prefix,
                searchParams: {
                    "sitePath": "us",
                    "query": url.split("/").pop(),
                },
                callback: this.getBasicInfo,
                userParams: {
                    type: url.split("/").pop(),
                    start: 0,
                }
            });
        });
    }

    getBasicInfo = (err, res, done) => {
        if (err) {
            log.error(err);
        } else {
            const { type, start } = res.options.userParams;
            const { count, items } = res.body.raw.itemList;
            items.forEach(item => {
                const { displayName, productId, modelId, price, salePrice, salePercentage, rating, ratingCount, link, onlineFrom, division, category, customBadge } = item;
                const data = {
                    type,
                    productId,
                    modelId,
                    displayName,
                    price,
                    salePrice,
                    salePercentage,
                    rating,
                    ratingCount,
                    link,
                    onlineFrom,
                    division,
                    category,
                    customBadge
                };
                fs.appendFileSync(resultFile, papa.unparse([data], { header: false }) + "\n");
            });
            if (start === 0) {
                for (let i = 1; i < Math.ceil(count / 48); ++i) {
                    this.crawler.add({
                        url: prefix,
                        searchParams: {
                            "sitePath": "us",
                            "query": type,
                            "start": i * 48,
                        },
                        callback: this.getBasicInfo,
                        userParams: {
                            type,
                            start: i * 48,
                        }
                    });
                }
            }
            log.info(`Type: ${type}, Start: ${start}, Count: ${items.length}, Total: ${count}`);
            return done();
        };
    }
}

const task = new Task();
task.start();