package module

import (
	"context"
	"fmt"
	"github.com/openimsdk/tools/utils/datautil"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openimsdk/openim-sdk-core/v3/pkg/utils"
	"github.com/openimsdk/openim-sdk-core/v3/sdk_struct"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/log"
)

var (
	TESTIP        = "127.0.0.1"
	APIADDR       = fmt.Sprintf("http://%v:10002", TESTIP)
	WSADDR        = fmt.Sprintf("ws://%v:10001", TESTIP)
	SECRET        = "openIM123"
	MANAGERUSERID = "imAdmin"

	PLATFORMID = constant.AndroidPlatformID
	LogLevel   = uint32(5)
)

var (
	totalOnlineUserNum    = 200000 // total online users num
	friendMsgSenderNum    = 200    // friend msg sender num
	NotFriendMsgSenderNum = 200    // not friend msg sender num
	groupMsgSenderNum     = 200    // group msg sender num
	msgSenderNumEvreyUser = 100    // the number of messages per user.
	fastenedUserNum       = 600    // fixed number of users

	recvMsgUserNum       = 20 // the number of message recipients, sampled accounts
	singleSampleUserList []string
)

const (
	HundredThousandGroupUserNum = 100000
	TenThousandGroupUserNum     = 10000
	ThousandGroupUserNum        = 1000
	HundredGroupUserNum         = 100
	FiftyGroupUserNum           = 50
	TenGroupUserNum             = 10

	HundredThousandGroupNum = 1
	TenThousandGroupNum     = 2
	ThousandGroupNum        = 5
	HundredGroupNum         = 50
	FiftyGroupNum           = 100
	TenGroupNum             = 1000

	FastenedUserPrefix  = "test_v3_u"
	OfflineUserPrefix   = "o"
	RecvMsgPrefix       = "recv_msg_prefix"
	singleMsgRecvPrefix = "single_msg_recv_prefix"
)

type PressureTester struct {
	friendManager          *TestFriendManager
	userManager            *TestUserManager
	groupManager           *TestGroupManager
	msgSender              map[string]*SendMsgUser
	userToken              map[string]string
	rw                     sync.RWMutex
	groupRandomSender      map[string][]string
	groupRandomOnlineUsers map[string][]string
	groupOwnerUserID       map[string]string
	groupMemberNum         map[string]int
	timeOffset             int64
	singleSendNum          atomic.Int64

	groupSenderUserIDs, friendSenderUserIDs, notfriendSenderUserIDs []string
	recvMsgUserIDs                                                  []string
	offlineUserIDs                                                  []string

	tenThousandGroupIDs, thousandGroupIDs, hundredGroupUserIDs, fiftyGroupUserIDs []string
}

func (p *PressureTester) SetOfflineUserIDs(offlineUserIDs []string) {
	p.offlineUserIDs = offlineUserIDs
}

func (p *PressureTester) FormatGroupInfo(ctx context.Context) {

	groupsByMemberNum := make(map[int][]string)

	for groupID, memberNum := range p.groupMemberNum {
		groupsByMemberNum[memberNum] = append(groupsByMemberNum[memberNum], groupID)
	}

	if len(p.groupMemberNum) == 0 {
		log.ZWarn(ctx, "no group created", nil)
		return
	}
	fmt.Println("---------------------------")
	for memberNum, groupIDs := range groupsByMemberNum {
		log.ZWarn(ctx, "member count", nil, "memberNum", memberNum)
		log.ZWarn(ctx, "group num", nil, "groupNum", len(groupIDs))
		fmt.Println("---------------------------")
	}
}

func (p *PressureTester) GetSingleSendNum() int64 {
	return p.singleSendNum.Load()
}
func NewPressureTester() (*PressureTester, error) {
	metaManager := NewMetaManager(APIADDR, SECRET, MANAGERUSERID)
	if err := metaManager.initToken(); err != nil {
		return nil, err
	}
	serverTime, err := metaManager.GetServerTime()
	if err != nil {
		return nil, err
	}
	log.ZWarn(context.Background(), "server time is", nil, "serverTime", serverTime, "current time",
		utils.GetCurrentTimestampByMill(), "time offset", serverTime-utils.GetCurrentTimestampByMill())

	return &PressureTester{friendManager: metaManager.NewFriendManager(), userManager: metaManager.NewUserManager(),
		groupManager:      metaManager.NewGroupMananger(),
		msgSender:         make(map[string]*SendMsgUser),
		userToken:         make(map[string]string),
		groupRandomSender: make(map[string][]string), groupOwnerUserID: make(map[string]string),
		groupMemberNum: make(map[string]int),
		timeOffset:     serverTime - utils.GetCurrentTimestampByMill()}, nil
}

func (p *PressureTester) genUserIDs() (userIDs, fastenedUserIDs, recvMsgUserIDs []string) {
	userIDs = p.userManager.GenUserIDs(totalOnlineUserNum - fastenedUserNum)                  // online users
	fastenedUserIDs = p.userManager.GenUserIDsWithPrefix(fastenedUserNum, FastenedUserPrefix) // specify the fixed users for sending messages
	recvMsgUserIDs = p.userManager.GenUserIDsWithPrefix(recvMsgUserNum, RecvMsgPrefix)        // complete SDK for sampling users
	return
}

// selectSample
func (p *PressureTester) SelectSample(total int, percentage float64) (fastenedUserIDs []string,
	sampleReceiver, offlineUserIDs []string, err error) {
	if percentage < 0 || percentage > 1 {
		return nil, nil, nil, fmt.Errorf("percentage must be between 0 and 1")
	}

	fastenedUserIDs = p.userManager.GenUserIDsWithPrefix(total, FastenedUserPrefix)
	offlineUserIDs = p.userManager.GenSEUserIDsWithPrefix(total, 2*total, OfflineUserPrefix)
	step := int(1.0 / percentage)
	for i := 0; i < total; i += step {
		sampleReceiver = append(sampleReceiver, fmt.Sprintf("%s_testv3_%d", FastenedUserPrefix, i))
	}
	singleSampleUserList = sampleReceiver
	return fastenedUserIDs, sampleReceiver, offlineUserIDs, nil

}
func (p *PressureTester) SelectSampleFromStarEnd(start, end int, percentage float64) (fastenedUserIDs []string,
	sampleReceiver, offlineUserIDs []string, err error) {
	if percentage < 0 || percentage > 1 {
		return nil, nil, nil, fmt.Errorf("percentage must be between 0 and 1")
	}
	fastenedUserIDs = p.userManager.GenSEUserIDsWithPrefix(start, end, FastenedUserPrefix)
	offlineUserIDs = p.userManager.GenSEUserIDsWithPrefix(end, 2*end-start, OfflineUserPrefix)
	step := int(1.0 / percentage)
	for i := start; i < end; i += step {
		sampleReceiver = append(sampleReceiver, fmt.Sprintf("%s%d", FastenedUserPrefix, i))
	}
	singleSampleUserList = sampleReceiver
	return fastenedUserIDs, sampleReceiver, offlineUserIDs, nil

}
func (p *PressureTester) SelectStartAndEnd(start, end int) (fastenedUserIDs []string) {
	return p.userManager.GenSEUserIDsWithPrefix(start, end, FastenedUserPrefix)
}

func (p *PressureTester) RegisterUsers(userIDs []string, fastenedUserIDs []string, recvMsgUserIDs []string) error {
	registerBatch := func(ids []string) error {
		if len(ids) == 0 {
			return nil
		}
		err := p.userManager.RegisterUsers(ids...)
		if err == nil {
			return nil
		}
		// idempotent register: if part of users already exist, retry one-by-one and skip duplicates.
		if strings.Contains(err.Error(), "1102 RegisteredAlreadyError") || strings.Contains(strings.ToLower(err.Error()), "registeredalreadyerror") {
			for _, uid := range ids {
				if e := p.userManager.RegisterUsers(uid); e != nil {
					msg := strings.ToLower(e.Error())
					if strings.Contains(msg, "1102 registeredalreadyerror") || strings.Contains(msg, "registeredalreadyerror") {
						continue
					}
					return e
				}
			}
			return nil
		}
		return err
	}

	for i := 0; i < len(userIDs); i += 1000 {
		end := i + 1000
		if end > len(userIDs) {
			end = len(userIDs)
		}
		userIDsSlice := userIDs[i:end]
		if err := registerBatch(userIDsSlice); err != nil {
			return err
		}
		if len(userIDsSlice) < 1000 {
			break
		}
	}
	if len(fastenedUserIDs) != 0 {
		if err := registerBatch(fastenedUserIDs); err != nil {
			return err
		}
	}
	if len(recvMsgUserIDs) != 0 {
		if err := registerBatch(recvMsgUserIDs); err != nil {
			return err
		}
	}
	return nil
}

func (p *PressureTester) InitUserConns(userIDs []string) {
	connected := 0
	tokenFailed := 0
	for _, userID := range userIDs {
		token, err := p.userManager.GetToken(userID, int32(PLATFORMID))
		if err != nil {
			tokenFailed++
			log.ZError(context.Background(), "get token failed", err, "userID", userID, "platformID", PLATFORMID)
			continue
		}
		user := NewUser(userID, token, p.timeOffset, p, sdk_struct.IMConfig{WsAddr: WSADDR, ApiAddr: APIADDR, PlatformID: int32(PLATFORMID)})
		p.rw.Lock()
		p.msgSender[userID] = user
		p.userToken[userID] = token
		p.rw.Unlock()
		connected++

	}
	log.ZWarn(context.Background(), "init user conns finished", nil, "targetUsers", len(userIDs), "connectedUsers", connected, "tokenFailedUsers", tokenFailed)

}

func (p *PressureTester) getUserToken(userID string) (string, bool) {
	p.rw.RLock()
	defer p.rw.RUnlock()
	token, ok := p.userToken[userID]
	return token, ok
}

func (p *PressureTester) getGroup(fastenedUserIDs []string, groupMemberNum int, groupSenderRate, groupOnlineRate float64) (ownerUserID string,
	userIDs []string, randomSender []string) {
	//get the group online users
	olineUserIDNum := int(float64(groupMemberNum) * groupOnlineRate)

	userIDs = p.Shuffle(fastenedUserIDs, olineUserIDNum)
	//get the group offline users
	offlineUserID := p.Shuffle(p.offlineUserIDs, groupMemberNum-olineUserIDNum)
	ownerUserID = p.Shuffle(userIDs, 1)[0]
	randomSender = p.Shuffle(userIDs, int(float64(groupMemberNum)*groupSenderRate))
	return ownerUserID, append(datautil.DeleteElems(userIDs, ownerUserID), offlineUserID...), randomSender
}

func (p *PressureTester) CreateTestGroups(fastenedUserIDs []string, total int, groupSenderRate, groupOnlineRate float64, hundredThousandGroupNum, tenThousandGroupNum, thousandGroupNum,
	hundredGroupNum, fiftyGroupNum, tenGroupNum int) (err error) {
	// create ten thousand group
	if hundredThousandGroupNum != 0 {
		if total < HundredThousandGroupUserNum {
			return fmt.Errorf("total user num must be greater than 100000")
		}
	}
	if tenThousandGroupNum != 0 {
		if total < TenThousandGroupUserNum {
			return fmt.Errorf("total user num must be greater than 10000")
		}

	}
	if thousandGroupNum != 0 {
		if total < ThousandGroupUserNum {
			return fmt.Errorf("total user num must be greater than 1000")
		}
	}

	if hundredGroupNum != 0 {
		if total < HundredGroupUserNum {
			return fmt.Errorf("total user num must be greater than 100")
		}

	}
	if fiftyGroupNum != 0 {
		if total < FiftyGroupUserNum {
			return fmt.Errorf("total user num must be greater than 50")
		}
	}

	if tenGroupNum != 0 {
		if total < TenGroupUserNum {
			return fmt.Errorf("total user num must be greater than 10")
		}

	}

	f := func(GroupNum int, GroupUserNum int, groupSenderRate, groupOnlineRate float64, groupIDAndNameString string) (err error) {
		for i := 1; i <= GroupNum; i++ {
			if groupOnlineRate < groupSenderRate {
				return fmt.Errorf("group online rate must > group sender rate")
			}
			ownerUserID, memberUserIDs, randomSenderUserIDs := p.getGroup(fastenedUserIDs, GroupUserNum, groupSenderRate, groupOnlineRate)
			groupID := p.groupManager.GenGroupID(fmt.Sprintf(groupIDAndNameString+"_%d", i))
			err = p.groupManager.CreateGroup(groupID, fmt.Sprintf(groupIDAndNameString+"_%d", i), ownerUserID,
				memberUserIDs)
			if err != nil {
				return
			}
			p.groupRandomSender[groupID] = randomSenderUserIDs
			p.groupOwnerUserID[groupID] = ownerUserID
			p.groupMemberNum[groupID] = GroupUserNum
		}
		return nil
	}
	err = f(hundredThousandGroupNum, HundredThousandGroupUserNum, groupSenderRate, groupOnlineRate, "hundredThousandGroupUserNum")
	if err != nil {
		return err
	}
	err = f(tenThousandGroupNum, TenThousandGroupUserNum, groupSenderRate, groupOnlineRate, "tenThousandGroupUserNum")
	if err != nil {
		return err
	}
	err = f(thousandGroupNum, ThousandGroupUserNum, groupSenderRate, groupOnlineRate, "thousandGroupUserNum")
	if err != nil {
		return err
	}
	err = f(hundredGroupNum, HundredGroupUserNum, groupSenderRate, groupOnlineRate, "hundredGroupUserNum")
	if err != nil {
		return err
	}
	err = f(fiftyGroupNum, FiftyGroupUserNum, groupSenderRate, groupOnlineRate, "fiftyGroupUserNum")
	if err != nil {
		return err
	}
	err = f(tenGroupNum, TenGroupUserNum, groupSenderRate, groupOnlineRate, "tenGroupUserNum")
	if err != nil {
		return err
	}

	return nil
}

func (p *PressureTester) SendGroupMessage(ctx context.Context, num int, duration time.Duration) {
	var wg sync.WaitGroup
	log.ZWarn(ctx, "send group message start", nil, "groupNum", len(p.groupOwnerUserID))
	if len(p.groupOwnerUserID) == 0 || num == 0 {
		log.ZWarn(ctx, "send group message over,do not need to send group message", nil)
	}
	defer log.ZWarn(ctx, "send group message over", nil, "groupNum", len(p.groupOwnerUserID))
	for groupID, _ := range p.groupOwnerUserID {
		wg.Add(1)
		go func(groupID string) {
			p.rw.RLock()
			if senderUserIDs, ok := p.groupRandomSender[groupID]; ok {
				p.rw.RUnlock()
				p.sendMessage2Groups(senderUserIDs, groupID, num, duration)
			}
			wg.Done()
		}(groupID)
	}
	wg.Wait()
}

func (p *PressureTester) sendMessage2Groups(senderIDs []string, groupID string, num int, duration time.Duration) {
	var wg sync.WaitGroup
	for _, senderID := range senderIDs {
		wg.Add(1)
		go func(senderID, groupID string) {
			defer wg.Done()
			for i := 0; i < num; i++ {
				if user, ok := p.msgSender[senderID]; ok {
					user.SendGroupMsgWithContext(groupID, i)
				}
				time.Sleep(duration)
			}
		}(senderID, groupID)

	}
	wg.Wait()
}

func (p *PressureTester) JoinUsersToGroup(ctx context.Context, groupID string, userIDs []string, _ int32, concurrency int) (joined int64, failed int64) {
	if groupID == "" {
		log.ZError(ctx, "join users to group failed", nil, "reason", "groupID is empty")
		return 0, int64(len(userIDs))
	}
	if len(userIDs) == 0 {
		return 0, 0
	}
	if concurrency <= 0 {
		concurrency = 200
	}

	var (
		wg      sync.WaitGroup
		sem     = make(chan struct{}, concurrency)
		success atomic.Int64
		fail    atomic.Int64
		mu      sync.Mutex
		reasons = make(map[string]int64)
	)
	start := time.Now()

	for _, uid := range userIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID string) {
			defer wg.Done()
			defer func() { <-sem }()
			token, ok := p.getUserToken(userID)
			if !ok || token == "" {
				fail.Add(1)
				mu.Lock()
				reasons["user token missing from active connections"]++
				mu.Unlock()
				log.ZDebug(ctx, "join group by user failed", "userID", userID, "groupID", groupID, "reason", "user token missing")
				return
			}
			var err error
			for attempt := 0; attempt < 3; attempt++ {
				err = p.groupManager.JoinGroupByUserToken(userID, token, groupID)
				if err == nil {
					break
				}
				if strings.Contains(err.Error(), "1507 TokenNotExistError") {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				break
			}
			if err != nil {
				if isBenignJoinError(err) {
					success.Add(1)
					mu.Lock()
					reasons["benign:"+err.Error()]++
					mu.Unlock()
					return
				}
				fail.Add(1)
				mu.Lock()
				reasons[err.Error()]++
				mu.Unlock()
				log.ZDebug(ctx, "join group by user failed", "userID", userID, "groupID", groupID, "err", err)
				return
			}
			success.Add(1)
		}(uid)
	}
	wg.Wait()

	joined = success.Load()
	failed = fail.Load()
	log.ZWarn(ctx, "join users to group finished", nil, "groupID", groupID, "userCount", len(userIDs),
		"joined", joined, "failed", failed, "concurrency", concurrency, "costMs", time.Since(start).Milliseconds())
	if failed > 0 {
		log.ZWarn(ctx, "join users to group fail reasons", nil, "groupID", groupID, "failReasonMap", reasons)
	}
	return joined, failed
}

func isBenignJoinError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// The user may already be in group (server returns ArgsError in this case).
	if strings.Contains(msg, "1001 argserror") {
		return true
	}
	// Some deployments create conversation records with a unique key conflict, but member insert is done.
	if strings.Contains(msg, "duplicate key error") &&
		strings.Contains(msg, "conversation_id_1") &&
		strings.Contains(msg, "sg_") {
		return true
	}
	return false
}

func (p *PressureTester) SendLiveRoomMessages(ctx context.Context, groupID string, senderIDs []string, count int, duration time.Duration, concurrency int, targetQPS int) {
	log.ZWarn(ctx, "send live room messages start", nil, "groupID", groupID, "senderCount", len(senderIDs), "count", count, "targetQPS", targetQPS)
	if groupID == "" || len(senderIDs) == 0 {
		log.ZWarn(ctx, "send live room messages skip", nil, "groupID", groupID, "senderCount", len(senderIDs), "count", count, "targetQPS", targetQPS)
		return
	}
	infinite := count <= 0
	rand.Seed(time.Now().UnixNano())
	if concurrency <= 0 {
		concurrency = 500
	}
	if targetQPS < 0 {
		targetQPS = 0
	}

	var qpsLimiter <-chan struct{}
	if targetQPS > 0 {
		interval := time.Second / time.Duration(targetQPS)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		tokens := make(chan struct{}, targetQPS)
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					close(tokens)
					return
				case <-ticker.C:
					select {
					case tokens <- struct{}{}:
					default:
					}
				}
			}
		}()
		qpsLimiter = tokens
	}
	var (
		wg     sync.WaitGroup
		sem    = make(chan struct{}, concurrency)
		sent   atomic.Int64
		failed atomic.Int64
	)
	start := time.Now()

	for _, uid := range senderIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID string) {
			defer wg.Done()
			defer func() { <-sem }()
			for i := 0; ; i++ {
				if !infinite && i >= count {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
				if qpsLimiter != nil {
					select {
					case <-ctx.Done():
						return
					case _, ok := <-qpsLimiter:
						if !ok {
							return
						}
					}
				}
				if user, ok := p.msgSender[userID]; ok {
					content := randomLiveRoomContent()
					if err := user.SendGroupCustomMsgWithContext(groupID, i, content); err != nil {
						failed.Add(1)
					} else {
						sent.Add(1)
					}
				} else {
					failed.Add(1)
				}
				if qpsLimiter == nil && duration > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(duration):
					}
				}
			}
		}(uid)
	}
	wg.Wait()
	log.ZWarn(ctx, "send live room messages finished", nil, "groupID", groupID, "senderCount", len(senderIDs),
		"count", count, "concurrency", concurrency, "costMs", time.Since(start).Milliseconds(),
		"infinite", infinite, "sent", sent.Load(), "failed", failed.Load(), "targetQPS", targetQPS)
}

var liveWordPool = []string{
	"hello", "nice", "awesome", "great", "amazing", "cool", "love", "wow", "fire", "hype",
	"stream", "live", "music", "dance", "party", "boss", "legend", "happy", "good", "night",
	"today", "moment", "energy", "crazy", "strong", "respect", "support", "fast", "smooth", "online",
}

var liveEmojiPool = []string{
	"\U0001F600", "\U0001F601", "\U0001F602", "\U0001F60E", "\U0001F60D", "\U0001F929", "\U0001F973", "\U0001F525", "\U0001F680", "\U0001F389",
	"\U0001F4AF", "\u2764\ufe0f", "\U0001F44D", "\U0001F44F", "\U0001F3B6", "\u2728", "\u26A1", "\U0001F606", "\U0001F91F", "\U0001F64C",
}

func randomLiveRoomContent() string {
	wordCount := 4 + rand.Intn(7)
	var builder strings.Builder
	for i := 0; i < wordCount; i++ {
		if i > 0 {
			builder.WriteByte(' ')
		}
		builder.WriteString(liveWordPool[rand.Intn(len(liveWordPool))])
	}
	builder.WriteByte(' ')
	emojiCount := 1 + rand.Intn(3)
	for i := 0; i < emojiCount; i++ {
		builder.WriteString(liveEmojiPool[rand.Intn(len(liveEmojiPool))])
	}
	return builder.String()
}

// func (p *PressureTester) SendSingleMessages(fastenedUserIDs []string, num int, duration time.Duration) {
// 	var wg sync.WaitGroup
// 	length := len(fastenedUserIDs)
// 	rand.Seed(time.Now().UnixNano())
// 	for i, userID := range fastenedUserIDs {
// 		counter:=0
// 		for counter < num {
// 			index := rand.Intn(length)
// 			if index != i {
// 				counter++
// 				wg.Add(1)
// 				go func(reciver string,sender string,counter int) {
// 					defer wg.Done()
// 					if user, ok := p.msgSender[sender]; ok {
// 						user.SendMsgWithContext(reciver, counter)
// 					}
// 					time.Sleep(duration)
// 				}(fastenedUserIDs[index],userID,counter)
// 			}
// 		}

// 	}
// 	wg.Wait()

// }

func (p *PressureTester) SendSingleMessages(ctx context.Context, fastenedUserIDs []string, randomSender []string, randomReceiver, num int, duration time.Duration) {
	log.ZWarn(ctx, "send single message start", nil, "randomSender", len(randomSender), "randomReceiver", randomReceiver)
	if len(randomSender) == 0 || randomReceiver == 0 || num == 0 {
		log.ZWarn(ctx, "send single message over,do not need to send single message", nil)
		return
	}
	defer log.ZWarn(ctx, "send single message over", nil, "randomSender", len(randomSender), "randomReceiver", randomReceiver)
	var wg sync.WaitGroup
	length := len(fastenedUserIDs)
	rand.Seed(time.Now().UnixNano())
	for _, userID := range randomSender {
		counter := 0
		var receiverUserIDs []string
		for counter < randomReceiver {
			index := rand.Intn(length)
			if fastenedUserIDs[index] != userID {
				counter++
				receiverUserIDs = append(receiverUserIDs, fastenedUserIDs[index])
			}
		}
		wg.Add(1)
		go func(receiverUserIDs []string, u string) {
			//log.ZError(context.Background(), "SendSingleMessages", nil, "length", len(receiverUserIDs))
			defer wg.Done()
			user, _ := p.msgSender[u]
			for _, rv := range receiverUserIDs {
				for x := 0; x < num; x++ {
					user.SendMsgWithContext(rv, x)
					p.singleSendNum.Add(1)

					time.Sleep(duration)
				}

			}
		}(receiverUserIDs, userID)
	}
	wg.Wait()

}

// Shuffle gets random userID from fastenedUserIDs and returns a slice of userID with length of needNum.
func (p *PressureTester) Shuffle(fastenedUserIDs []string, needNum int) []string {

	rand.Shuffle(len(fastenedUserIDs), func(i, j int) {
		fastenedUserIDs[i], fastenedUserIDs[j] = fastenedUserIDs[j], fastenedUserIDs[i]
	})

	selectedUserIDs := make([]string, 0, needNum)
	seen := make(map[string]bool)

	for _, userID := range fastenedUserIDs {
		if len(selectedUserIDs) == needNum {
			break
		}

		if !seen[userID] {
			selectedUserIDs = append(selectedUserIDs, userID)
			seen[userID] = true
		}
	}
	return selectedUserIDs
}

func (p *PressureTester) SendSingleMessagesTo(fastenedUserIDs []string, num int, duration time.Duration) {
	var wg sync.WaitGroup
	//length := len(fastenedUserIDs)
	rand.Seed(time.Now().UnixNano())
	for i, userID := range fastenedUserIDs {
		//counter := 0
		//var receiverUserIDs []string
		//for counter < num {
		//	index := rand.Intn(length)
		//	if index != i {
		//		counter++
		//		receiverUserIDs = append(receiverUserIDs, fastenedUserIDs[index])
		//	}
		//}
		var receiverUserIDs []string
		for i < num {
			receiverUserIDs = append(receiverUserIDs, utils.IntToString(i))
			i++
		}
		wg.Add(1)
		go func(receiverUserIDs []string, u string) {
			defer wg.Done()
			user, _ := p.msgSender[u]
			for j, rv := range receiverUserIDs {
				user.SendMsgWithContext(rv, j)
				time.Sleep(duration)

			}
		}(receiverUserIDs, userID)
	}
	wg.Wait()

}

func (p *PressureTester) SendMessages(sendID, recvID string, msgNum int) {
	var i = 0
	var ws sync.WaitGroup
	user, _ := p.msgSender[sendID]
	for i < msgNum {
		ws.Add(1)
		i++
		go func() {
			defer ws.Done()
			user.SendMsgWithContext(recvID, i)
		}()

	}
	ws.Wait()

}

func (p *PressureTester) importFriends(friendSenderUserIDs, recvMsgUserIDs []string) error {
	for _, recvMsgUserID := range recvMsgUserIDs {
		if err := p.friendManager.ImportFriends(recvMsgUserID, friendSenderUserIDs); err != nil {
			return err
		}
	}
	return nil
}

func (p *PressureTester) CheckMsg(ctx context.Context) {
	log.ZWarn(ctx, "message send finished checking", nil)
	var max, min, latencySum int64
	samepleReceiverFailedMap := make(map[string]*errorValue)
	failedMessageAllMap := make(map[string]*errorValue)
	sendSampleMessageAllMap := make(map[string]*msgValue)
	recvSampleMessageAllMap := make(map[string]*msgValue)
	groupSendSampleNum := make(map[string]int)
	groupSendFailedNum := make(map[string]int)
	groupRecvSampleInfo := make(map[string]*groupMessageValue)
	var sampleSendLength, sampleRecvLength, failedMessageLength int
	for _, user := range p.msgSender {
		if len(user.singleFailedMessageMap) != 0 {
			failedMessageLength += len(user.singleFailedMessageMap)
			for s, value := range user.singleFailedMessageMap {
				failedMessageAllMap[s] = value
				if utils.IsContain(value.RecvID, singleSampleUserList) {
					samepleReceiverFailedMap[s] = value
				}
			}
		}
		if len(user.singleSendSampleMessage) != 0 {
			sampleSendLength += len(user.singleSendSampleMessage)
			for s, value := range user.singleSendSampleMessage {
				sendSampleMessageAllMap[s] = value
			}
		}
		if len(user.singleRecvSampleMessage) != 0 {
			sampleRecvLength += len(user.singleRecvSampleMessage)
			for _, value := range user.singleRecvSampleMessage {
				if min == 0 && max == 0 {
					min = value.Latency
					max = value.Latency
				}
				if value.Latency < min {
					min = value.Latency
				}
				if value.Latency > max {
					max = value.Latency
				}
				latencySum += value.Latency
			}
			for s, value := range user.singleRecvSampleMessage {
				recvSampleMessageAllMap[s] = value
			}
		}

		//group check
		if len(user.groupSendSampleNum) != 0 {
			for group, num := range user.groupSendSampleNum {
				groupSendSampleNum[group] += num
			}
		}
		if len(user.groupFailedMessageMap) != 0 {
			for groupID, errInfo := range user.groupFailedMessageMap {
				groupSendFailedNum[groupID] += len(errInfo)
			}
		}
	}
	for groupID, ownerUserID := range p.groupOwnerUserID {
		if s, ok := p.msgSender[ownerUserID]; ok {
			if info, ok := s.groupRecvSampleInfo[groupID]; ok {
				info.Latency = info.LatencySum / info.Num
				groupRecvSampleInfo[groupID] = info
			}
		}
	}
	var latency string
	if sampleRecvLength != 0 {
		latency = utils.Int64ToString(latencySum/int64(sampleRecvLength)) + " ms"
	} else {
		latency = "0 ms"
	}
	log.ZWarn(context.Background(), "single message check result", nil, "failedMessageLength", failedMessageLength,
		"sampleSendLength", sampleSendLength, "sampleRecvLength", sampleRecvLength, "Average of message latency", latency,
		"max", utils.Int64ToString(max)+" ms",
		"min", utils.Int64ToString(min)+" ms")
	if len(groupSendSampleNum) > 0 {
		log.ZWarn(context.Background(), "group message check result", nil, "failedMessageLength", groupSendFailedNum,
			"sampleSendLength", groupSendSampleNum, "sampleRecvLength", groupRecvSampleInfo)
	}
	if len(failedMessageAllMap) > 0 {
		err := p.saveFailedMessageToFile(failedMessageAllMap, "failedMessageAllMap")
		if err != nil {
			log.ZWarn(ctx, "save failed message to file failed", err)
		}
	}

	if len(samepleReceiverFailedMap) > 0 {
		err := p.saveFailedMessageToFile(failedMessageAllMap, "sampleReceiverFailedMap")
		if err != nil {
			log.ZWarn(ctx, "save sampleReceiverFailedMap message to file failed", err)
		}
	}
	if sampleSendLength != sampleRecvLength {
		recvEx, sendEx := findMapIntersection(sendSampleMessageAllMap, recvSampleMessageAllMap)
		if len(recvEx) != 0 {
			p.saveSuccessMessageToFile(recvEx, "recvAdditional")
		}
		if len(sendEx) != 0 {
			p.saveSuccessMessageToFile(recvEx, "sendAdditional")
		}
	}
	log.ZDebug(ctx, "message send finished start to check message")
	os.Exit(1)
}

func (p *PressureTester) saveFailedMessageToFile(m map[string]*errorValue, filename string) error {
	file, err := os.Create(filename + ".txt")
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range m {

		line := fmt.Sprintf("Key: %s, Value: %v\n", key, value)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}
func (p *PressureTester) saveSuccessMessageToFile(m map[string]*msgValue, filename string) error {
	file, err := os.Create(filename + ".txt")
	if err != nil {
		return err
	}
	defer file.Close()

	for key, value := range m {

		line := fmt.Sprintf("Key: %s, Value: %v\n", key, value)
		_, err := file.WriteString(line)
		if err != nil {
			return err
		}
	}
	return nil
}

func findMapIntersection(map1, map2 map[string]*msgValue) (map[string]*msgValue, map[string]*msgValue) {
	InMap1NotInMap2 := make(map[string]*msgValue)
	InMap2NotInMap1 := make(map[string]*msgValue)
	for key := range map1 {
		if _, ok := map2[key]; !ok {
			InMap1NotInMap2[key] = map1[key]
		}
	}
	for key := range map2 {
		if _, ok := map1[key]; !ok {
			InMap2NotInMap1[key] = map2[key]
		}
	}

	return InMap1NotInMap2, InMap2NotInMap1
}
