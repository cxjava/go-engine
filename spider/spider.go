package spider

import (
	"github.com/cxjava/go-engine/common"
	"github.com/cxjava/go-engine/loggo"
	"github.com/cxjava/go-engine/shell"
	"math"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {
	Threadnum    int
	Buffersize   int
	Deps         int
	FocusSpider  bool
	Crawlfunc    string // simple,puppeteer
	CrawlTimeout int
	CrawlRetry   int
}

type DBInfo struct {
	Host  string
	Title string
	Name  string
	Url   string
}

type PageLinkInfo struct {
	UI   URLInfo
	Name string
}

type PageInfo struct {
	UI    URLInfo
	Title string
	Son   []PageLinkInfo
}

type URLInfo struct {
	Url  string
	Deps int
}

func Ini() {
	if runtime.GOOS == "linux" {
		go startChrome()
		go getChrome()
		for i := 0; i < 10; i++ {
			if len(gSpiderData.chromeWSEndpoint) > 0 {
				break
			}
			time.Sleep(time.Second)
		}

		if len(gSpiderData.chromeWSEndpoint) <= 0 {
			panic("spider start chrome fail")
		}

		loggo.Info("spider start chrome %v", gSpiderData.chromeWSEndpoint)
	}
}

type SpiderData struct {
	chromeWSEndpoint string
}

var gSpiderData SpiderData

var gcb func(host string, title string, name string, url string)

func GetChromeWSEndpoint() string {
	return gSpiderData.chromeWSEndpoint
}

func getChrome() {
	defer common.CrashLog()

	for {
		ret := shell.Run(common.GetNodeDir()+"/get_chrome.sh", true, common.GetNodeDir())
		ret = strings.TrimSpace(ret)
		if len(ret) > 0 {
			if ret != gSpiderData.chromeWSEndpoint {
				gSpiderData.chromeWSEndpoint = ret
				loggo.Info("spider get chromeWSEndpoint %v", gSpiderData.chromeWSEndpoint)
			}
		}
		time.Sleep(time.Second)
	}
}

func startChrome() {
	defer common.CrashLog()

	for {
		shell.RunTimeout(common.GetNodeDir()+"/close_chrome.sh", true, 60)
		loggo.Info("spider restart chrome ")
		shell.Run(common.GetNodeDir()+"/start_chrome.sh", true, common.GetNodeDir())
		time.Sleep(time.Second)
	}
}

func SetCallback(cb func(host string, title string, name string, url string)) {
	gcb = cb
}

type Stat struct {
	CrawBePushJobNum int

	CrawChannelNum  int
	CrawFunc        string
	CrawNum         int
	CrawRetrtyNum   int
	CrawOKNum       int64
	CrawFailNum     int
	CrawOKTotalTime int64
	CrawOKAvgTime   int64

	ParseChannelNum int
	ParseNum        int
	ParseValidNum   int
	ParseSpawnNum   int
	ParseFinishNum  int
	ParseTooDeepNum int
	ParseJobNum     int

	SaveChannelNum int
	SaveNum        int

	InsertNum         int64
	InsertTotalTime   int64
	InsertCBTotalTime int64
	InsertAvgTime     int64
	InsertCBAvgTime   int64

	JobInsertNum       int64
	JobInsertTotalTime int64
	JobInsertAvgTime   int64
	JobPopNum          int64
	JobPopTotalTime    int64
	JobPopAvgTime      int64
	JobHasNum          int64
	JobHasTotalTime    int64
	JobHasAvgTime      int64

	DoneInsertNum       int64
	DoneInsertTotalTime int64
	DoneInsertAvgTime   int64
	DoneHasNum          int64
	DoneHasTotalTime    int64
	DoneHasAvgTime      int64
}

func Start(db *DB, config Config, url string, stat *Stat) {
	loggo.Info("Spider Start  %v", url)

	dsn := db.dsn
	conn := db.conn

	jbd := LoadJob(dsn, conn, url)
	if jbd == nil {
		loggo.Error("Spider job LoadJob fail %v", url)
		return
	}
	dbd := LoadDone(dsn, conn, url)
	if dbd == nil {
		loggo.Error("Spider job LoadDone fail %v", url)
		return
	}

	old := GetJobSize(jbd)
	if old == 0 {
		InsertSpiderJob(jbd, url, 0, stat)
		DeleteSpiderDone(dbd)
	}

	old = GetJobSize(jbd)
	if old == 0 {
		loggo.Error("Spider job no jobs %v", url)
		return
	}

	crawl := make(chan *URLInfo, config.Buffersize)
	parse := make(chan *PageInfo, config.Buffersize)
	save := make(chan *DBInfo, config.Buffersize)

	entry, deps := PopSpiderJob(jbd, int(math.Min(float64(old), float64(config.Buffersize))), stat)
	if len(entry) == 0 {
		loggo.Error("Spider job no jobs %v", url)
		return
	}

	for i, u := range entry {
		crawl <- &URLInfo{u, deps[i]}
	}

	var jobsCrawlerTotal int32
	var jobsCrawlerFail int32

	var wg sync.WaitGroup
	var running int32

	for i := 0; i < config.Threadnum; i++ {
		wg.Add(3)
		go Crawler(&running, &wg, jbd, dbd, config, crawl, parse, &jobsCrawlerTotal, &jobsCrawlerFail,
			config.Crawlfunc, config.CrawlTimeout, config.CrawlRetry, stat)
		go Parser(&running, &wg, jbd, dbd, config, crawl, parse, save, url, stat)
		go Saver(&running, &wg, db, save, stat)
	}

	for {
		tmpurls, tmpdeps := PopSpiderJob(jbd, config.Buffersize, stat)
		if len(tmpurls) == 0 {
			time.Sleep(time.Second)
			run := atomic.LoadInt32(&running)
			if run == 0 {
				time.Sleep(time.Second)
				tmpurls, tmpdeps = PopSpiderJob(jbd, config.Buffersize, stat)
				if len(tmpurls) == 0 && run == 0 &&
					len(crawl) == 0 && len(parse) == 0 && len(save) == 0 {
					break
				}
			}
		}

		for i, url := range tmpurls {
			stat.CrawBePushJobNum++
			crawl <- &URLInfo{url, tmpdeps[i]}
		}
	}

	loggo.Info("Spider jobs done %v crawl %v, failed %v", url, jobsCrawlerTotal, jobsCrawlerFail)

	crawl <- nil
	parse <- nil
	save <- nil
	wg.Wait()

	close(crawl)
	close(parse)
	close(save)

	loggo.Info("Spider end %v %v %v", url, GetSize(db), GetDoneSize(dbd))

	CloseJob(jbd)
	CloseDone(dbd)
}

func Crawler(running *int32, group *sync.WaitGroup, jbd *JobDB, dbd *DoneDB, config Config, crawl <-chan *URLInfo, parse chan<- *PageInfo, jobsCrawlerTotal *int32, jobsCrawlerTotalFail *int32, crawlfunc string, crawlTimeout int, crawlRetry int, stat *Stat) {
	defer common.CrashLog()

	defer group.Done()

	loggo.Info("Crawler start")
	for job := range crawl {
		if job == nil {
			break
		}
		atomic.AddInt32(running, 1)

		stat.CrawChannelNum = len(crawl)
		//loggo.Info("receive crawl job %v", job)

		ok := HasDone(dbd, job.Url, stat)
		if !ok {
			InsertSpiderDone(dbd, job.Url, stat)
			if job.Deps < config.Deps {
				atomic.AddInt32(jobsCrawlerTotal, 1)
				var pg *PageInfo
				b := time.Now()
				stat.CrawNum++
				stat.CrawFunc = crawlfunc
				for t := 0; t < crawlRetry; t++ {
					stat.CrawRetrtyNum++
					if crawlfunc == "simple" {
						pg = simplecrawl(job, crawlTimeout)
					} else if crawlfunc == "puppeteer" {
						pg = puppeteercrawl(job, crawlTimeout)
					}
					if pg != nil {
						break
					}
				}
				if pg != nil {
					stat.CrawOKNum++
					stat.CrawOKTotalTime += int64(time.Now().Sub(b))
					loggo.Info("crawl job ok %v %v %v %s", job.Url, pg.Title, len(pg.Son), time.Now().Sub(b).String())
					parse <- pg
				} else {
					stat.CrawFailNum++
					atomic.AddInt32(jobsCrawlerTotalFail, 1)
				}
			}
		}

		atomic.AddInt32(running, -1)
	}
	loggo.Info("Crawler end")
}

func Parser(running *int32, group *sync.WaitGroup, jbd *JobDB, dbd *DoneDB, config Config, crawl chan<- *URLInfo, parse <-chan *PageInfo, save chan<- *DBInfo, hosturl string, stat *Stat) {
	defer common.CrashLog()

	defer group.Done()

	loggo.Info("Parser start")

	for job := range parse {
		if job == nil {
			break
		}
		atomic.AddInt32(running, 1)

		stat.ParseChannelNum = len(parse)
		//loggo.Info("receive parse job %v %v", job.Title, job.UI.Url)

		stat.ParseNum++

		srcURL, err := url.Parse(job.UI.Url)
		if err != nil {
			atomic.AddInt32(running, -1)
			continue
		}

		stat.ParseValidNum++

		for _, s := range job.Son {
			sonurl := s.UI.Url

			stat.ParseSpawnNum++

			if strings.HasPrefix(sonurl, "#") {
				continue
			}

			if sonurl == "/" {
				continue
			}

			if strings.Contains(sonurl, "javascript:") {
				continue
			}

			ss := strings.ToLower(sonurl)

			ok := false
			if strings.HasPrefix(ss, "thunder://") || strings.HasPrefix(ss, "magnet:?") ||
				strings.HasPrefix(ss, "ed2k://") {
				ok = true
			}

			if strings.HasSuffix(ss, ".mp4") || strings.HasSuffix(ss, ".rmvb") || strings.HasSuffix(ss, ".mkv") ||
				strings.HasSuffix(ss, ".avi") || strings.HasSuffix(ss, ".mpg") || strings.HasSuffix(ss, ".mpeg") ||
				strings.HasSuffix(ss, ".wmv") ||
				strings.HasSuffix(ss, ".torrent") {
				ok = true
			}

			if ok {
				stat.ParseFinishNum++

				di := DBInfo{hosturl, job.Title, s.Name, sonurl}
				save <- &di

				//loggo.Info("receive parse ok %v %v %v", job.Title, s.Name, sonurl)
			} else {

				if s.UI.Deps >= config.Deps {
					stat.ParseTooDeepNum++
					continue
				}

				if strings.HasPrefix(ss, "http://") || strings.HasPrefix(ss, "https://") {

				} else if strings.HasPrefix(ss, "/") {
					sonurl = srcURL.Scheme + "://" + srcURL.Host + sonurl
				} else {
					dir := srcURL.Path

					dirIndex := strings.LastIndex(dir, "/")
					if dirIndex >= 0 {
						dir = dir[0:dirIndex]
					} else {
						dir = ""
					}
					sonurl = srcURL.Scheme + "://" + srcURL.Host + dir + "/" + sonurl

					mIndex := strings.Index(sonurl, "#")
					if mIndex >= 0 {
						sonurl = sonurl[0:mIndex]
					}
				}

				for {
					new := strings.Replace(sonurl, "/./", "/", -1)
					if new == sonurl {
						break
					}
					sonurl = new
				}

				_, err := url.Parse(sonurl)
				if err != nil {
					continue
				}

				var tmp *URLInfo

				finded := HasDone(dbd, sonurl, stat)
				if !finded {
					if config.FocusSpider {
						dstURL, dsterr := url.Parse(sonurl)

						if dsterr == nil {
							dstParams := strings.Split(dstURL.Host, ".")
							srcParams := strings.Split(srcURL.Host, ".")

							if len(dstParams) >= 2 && len(srcParams) >= 2 &&
								dstParams[len(dstParams)-1] == srcParams[len(srcParams)-1] &&
								dstParams[len(dstParams)-2] == srcParams[len(srcParams)-2] {
								tmp = &URLInfo{sonurl, s.UI.Deps}
							}
						}
					} else {
						tmp = &URLInfo{sonurl, s.UI.Deps}
					}
				}

				if tmp != nil {
					hasJob := HasJob(jbd, tmp.Url, stat)
					if !hasJob {
						stat.ParseJobNum++

						InsertSpiderJob(jbd, tmp.Url, tmp.Deps, stat)

						//loggo.Info("parse spawn job %v %v %v", job.UI.Url, sonurl, GetJobSize(src))
					}
				}
			}
		}
		atomic.AddInt32(running, -1)
	}
	loggo.Info("Parser end")
}

func Saver(running *int32, group *sync.WaitGroup, db *DB, save <-chan *DBInfo, stat *Stat) {
	defer common.CrashLog()

	defer group.Done()

	loggo.Info("Saver start")

	for job := range save {
		if job == nil {
			break
		}
		atomic.AddInt32(running, 1)

		stat.SaveChannelNum = len(save)
		//loggo.Info("receive save job %v %v %v", job.Title, job.Name, job.Url)

		stat.SaveNum++
		InsertSpider(db, job.Title, job.Name, job.Url, job.Host, stat)

		atomic.AddInt32(running, -1)
	}

	loggo.Info("Saver end")
}
