package main

import (
	"context"
	"flag"
	"fmt"
	log2 "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/version"

	"github.com/openimsdk/openim-sdk-core/v3/msgtest/module"
	"github.com/openimsdk/tools/log"
)

func init() {
	_ = runtime.GOMAXPROCS(7)
	InitWithFlag()
	if err := log.InitLoggerFromConfig("sdk.log", "sdk", "", "", 3,
		true, false, "./", 2, 24, version.Version, false); err != nil {
		panic(err)
	}
}

var (
	totalOnlineUserNum int     // total online users num
	randomSender       int     // random sender num
	randomReceiver     int     // random receiver num
	singleSamplingRate float64 // sampling rate for single chat
	GroupSenderRate    float64 // the random sender ratio for group chats
	GroupOnlineRate    float64 // group chat online user rate
	start              int
	end                int
	count              int
	sendInterval       int

	//recvMsgUserNum int // the number of message recipients, sampled accounts
	isRegisterUser  bool // If register users
	onlineUsersOnly bool
	pprofEnable     bool

	hundredThousandGroupNum int //
	tenThousandGroupNum     int
	thousandGroupNum        int
	hundredGroupNum         int
	fiftyGroupNum           int
	tenGroupNum             int

	liveMode            bool
	liveGroupID         string
	liveOnlineUserNum   int
	liveJoinConcurrency int
	liveSendConcurrency int
	liveSenderRate      float64
	liveRegisterOnly    bool
	livePlatformID      int
	liveSendForever     bool
	liveQPS             int
)

func InitWithFlag() {
	flag.IntVar(&totalOnlineUserNum, "o", 20000, "total online user num")
	flag.IntVar(&randomSender, "rs", 0, "random sender num")
	flag.IntVar(&randomReceiver, "rr", 0, "random receiver num")
	flag.IntVar(&start, "s", 0, "start user")

	flag.IntVar(&end, "e", 0, "end user")
	flag.Float64Var(&singleSamplingRate, "sr", 0.01, "single chat sampling rate")
	flag.Float64Var(&GroupSenderRate, "gsr", 0.1, "group chat sender rate")
	flag.Float64Var(&GroupOnlineRate, "gor", 0.0, "group online rate")
	flag.IntVar(&count, "c", 0, "number of messages per user")
	flag.IntVar(&sendInterval, "i", 1000, "send message interval per user(milliseconds)")
	flag.IntVar(&hundredThousandGroupNum, "htg", 0, "quantity of 100k user groups")
	flag.IntVar(&tenThousandGroupNum, "ttg", 0, "quantity of 10k user groups")
	flag.IntVar(&thousandGroupNum, "otg", 0, "quantity of 1k user groups")
	flag.IntVar(&hundredGroupNum, "hog", 0, "quantity of 100 user groups")
	flag.IntVar(&fiftyGroupNum, "fog", 0, "quantity of 50 user groups")
	flag.IntVar(&tenGroupNum, "teg", 0, "quantity of 10 user groups")
	flag.BoolVar(&liveMode, "live", false, "run live room pressure mode")
	flag.StringVar(&liveGroupID, "live_gid", "", "target live room groupID")
	flag.IntVar(&liveOnlineUserNum, "live_u", 10000, "online user count for live room pressure mode")
	flag.IntVar(&liveJoinConcurrency, "live_jc", 200, "join group concurrency for live room pressure mode")
	flag.IntVar(&liveSendConcurrency, "live_sc", 500, "send message concurrency for live room pressure mode")
	flag.Float64Var(&liveSenderRate, "live_sr", 1.0, "sender ratio for live room pressure mode")
	flag.BoolVar(&liveRegisterOnly, "live_reg_only", false, "register users for live room pressure mode and exit")
	flag.IntVar(&livePlatformID, "live_pid", int(module.PLATFORMID), "platformID for live room users (example: web is usually 5)")
	flag.BoolVar(&liveSendForever, "live_forever", false, "keep sending live room messages until interrupted")
	flag.IntVar(&liveQPS, "live_qps", 0, "global live room send speed limit(messages per second), 0 means disabled")

	//note: in go, bool flag do not set -r true(can not to use),must set (-r=true or -r) that means true
	flag.BoolVar(&isRegisterUser, "r", false, "register user to IM system")
	flag.BoolVar(&onlineUsersOnly, "u", false, "consider only online users")
	flag.BoolVar(&pprofEnable, "pp", false, "enable pprof")
}

func PrintQPS() {
	for {

		log.ZError(context.Background(), "QPS", nil, "qps", module.GetQPS())
		time.Sleep(time.Second * 1)
	}
}

func waitSignal(ctx context.Context, scene string) {
	log.ZWarn(ctx, scene, nil, "status", "blocking process, waiting interrupt signal")
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	<-signalChannel
	log.ZWarn(ctx, scene, nil, "status", "received interrupt signal. Exiting...")
}

func main() {
	flag.Parse()
	ctx := context.Background()
	log.ZWarn(ctx, "flag args", nil, "totalOnlineUserNum", totalOnlineUserNum,
		"randomSender", randomSender, "randomReceiver", randomReceiver,
		"singleSamplingRate", singleSamplingRate, "start", start, "end", end, "count", count, "sendInterval", sendInterval,
		"onlineUsersOnly", onlineUsersOnly, "isRegisterUser", isRegisterUser, "groupSenderRate", GroupSenderRate, "groupOnlineRate", GroupOnlineRate,
		"hundredThousandGroupNum", hundredThousandGroupNum, "tenThousandGroupNum", tenThousandGroupNum, "thousandGroupNum", thousandGroupNum,
		"hundredGroupNum", hundredGroupNum, "fiftyGroupNum", fiftyGroupNum, "tenGroupNum", tenGroupNum, "pprofEnable", pprofEnable,
		"liveMode", liveMode, "liveGroupID", liveGroupID, "liveOnlineUserNum", liveOnlineUserNum,
		"liveJoinConcurrency", liveJoinConcurrency, "liveSendConcurrency", liveSendConcurrency, "liveSenderRate", liveSenderRate,
		"liveRegisterOnly", liveRegisterOnly, "livePlatformID", livePlatformID, "liveSendForever", liveSendForever, "liveQPS", liveQPS)
	if pprofEnable {
		go func() {
			log2.Println(http.ListenAndServe("0.0.0.0:6060", nil))
		}()
	}
	p, err := module.NewPressureTester()
	if err != nil {
		fmt.Println(err)
	}
	if liveMode {
		if liveRegisterOnly {
			isRegisterUser = true
		}
		if !liveRegisterOnly && liveGroupID == "" {
			log.ZError(ctx, "live mode start failed", nil, "reason", "live_gid is empty")
			return
		}
		if livePlatformID > 0 {
			module.PLATFORMID = livePlatformID
		}
		if liveOnlineUserNum <= 0 {
			liveOnlineUserNum = 10000
		}
		if !isRegisterUser {
			log.ZWarn(ctx, "live mode uses existing users", nil, "tip", "if users are not pre-registered, use -r=true or run with -live_reg_only first")
		}
		if count <= 0 && !liveSendForever {
			count = 1
		}
		if liveSenderRate <= 0 || liveSenderRate > 1 {
			liveSenderRate = 1
		}
		liveUsers, _, _, err := p.SelectSample(liveOnlineUserNum, 1)
		if err != nil {
			log.ZError(ctx, "SelectSample failed", err, "liveOnlineUserNum", liveOnlineUserNum)
			return
		}
		if isRegisterUser {
			if err := p.RegisterUsers(liveUsers, nil, nil); err != nil {
				log.ZError(ctx, "RegisterUsers failed", err)
				return
			}
			if liveRegisterOnly {
				log.ZWarn(ctx, "live mode register only finished", nil, "userCount", len(liveUsers))
				return
			}
		}
		p.InitUserConns(liveUsers)
		time.Sleep(10 * time.Second)

		joined, failed := p.JoinUsersToGroup(ctx, liveGroupID, liveUsers, int32(module.PLATFORMID), liveJoinConcurrency)
		if joined == 0 {
			log.ZError(ctx, "live mode join group failed", nil, "groupID", liveGroupID, "joined", joined, "failed", failed)
			return
		}

		if onlineUsersOnly {
			waitSignal(ctx, "live mode")
			return
		}

		senderIDs := liveUsers
		if liveSenderRate < 1 {
			senderCount := int(float64(len(liveUsers)) * liveSenderRate)
			if senderCount < 1 {
				senderCount = 1
			}
			senderIDs = p.Shuffle(liveUsers, senderCount)
		}

		sendCount := count
		sendCtx := ctx
		if liveSendForever {
			sendCount = 0
			var cancel context.CancelFunc
			sendCtx, cancel = context.WithCancel(ctx)
			log.ZWarn(ctx, "live mode send loop started", nil, "hint", "press Ctrl+C to stop")
			go func() {
				signalChannel := make(chan os.Signal, 1)
				signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
				<-signalChannel
				log.ZWarn(ctx, "live mode send loop stopping", nil, "reason", "received interrupt signal")
				cancel()
			}()
		}

		p.SendLiveRoomMessages(sendCtx, liveGroupID, senderIDs, sendCount, time.Millisecond*time.Duration(sendInterval), liveSendConcurrency, liveQPS)
		log.ZWarn(ctx, "live mode finished", nil, "groupID", liveGroupID, "joined", joined, "failed", failed, "senderCount", len(senderIDs), "liveSendForever", liveSendForever)
		if !liveSendForever {
			time.Sleep(30 * time.Second)
		}
		return
	}
	var f, r, o []string
	if start != 0 {
		f, r, o, err = p.SelectSampleFromStarEnd(start, end, singleSamplingRate)
	} else {
		f, r, o, err = p.SelectSample(totalOnlineUserNum, singleSamplingRate)
	}
	if err != nil {
		log.ZError(ctx, "Sample UserID failed", err)
		return
	}
	p.SetOfflineUserIDs(o)
	log.ZWarn(ctx, "Sample UserID", nil, "sampleUserLength", len(r), "sampleUserID", r, "length", len(f))
	time.Sleep(10 * time.Second)
	//
	if isRegisterUser {
		if err := p.RegisterUsers(append(f, o...), nil, nil); err != nil {
			log.ZError(ctx, "Sample UserID failed", err)
			return
		}
	}
	err = p.CreateTestGroups(f, totalOnlineUserNum, GroupSenderRate, GroupOnlineRate, hundredThousandGroupNum,
		tenThousandGroupNum, thousandGroupNum, hundredGroupNum, fiftyGroupNum, tenGroupNum)
	if err != nil {
		log.ZError(ctx, "CreateTestGroups failed", err)
		return
	}
	p.FormatGroupInfo(ctx)

	//go PrintQPS()
	// init users
	p.InitUserConns(f)
	log.ZWarn(ctx, "all user init connect to server success,start send message", nil, "count", count)
	if onlineUsersOnly {
		waitSignal(ctx, "default mode")
		return
	}

	time.Sleep(10 * time.Second)
	p.SendSingleMessages(ctx, f, p.Shuffle(f, randomSender), randomReceiver, count, time.Millisecond*time.Duration(sendInterval))
	p.SendGroupMessage(ctx, count, time.Millisecond*time.Duration(sendInterval))
	log.ZWarn(ctx, "send all message over", nil, "singleNum", p.GetSingleSendNum())
	//p.SendSingleMessagesTo(f, 20000, time.Millisecond*1)
	//p.SendMessages("fastened_user_prefix_testv3new_0", "fastened_user_prefix_testv3new_1", 100000)
	time.Sleep(3 * time.Minute)
	p.CheckMsg(ctx)

	time.Sleep(time.Hour * 60)

}
