package dtmcli

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dtm-labs/client/dtmcli/dtmimp"
	"github.com/dtm-labs/logger"
	"github.com/go-redis/redis/v8"
)

type KeyAmount struct {
	Key    string
	Amount int
	Err    error
}

func (bb *BranchBarrier) RedisCheckAdjustBatchAmounts(rd *redis.Client, amounts []*KeyAmount, barrierExpire int) error {
	if len(amounts) == 0 {
		return errors.New("empty amounts")
	}

	bid := bb.newBarrierID()
	opKey := fmt.Sprintf("%s-%s-%s-%s", bb.Gid, bb.BranchID, bb.Op, bid)
	originOp := map[string]string{
		dtmimp.OpCancel:     dtmimp.OpTry,
		dtmimp.OpCompensate: dtmimp.OpAction,
	}[bb.Op]
	originOpKey := fmt.Sprintf("%s-%s-%s-%s", bb.Gid, bb.BranchID, originOp, bid)

	keys := []string{opKey, originOpKey}
	values := []interface{}{originOp, barrierExpire}

	for _, v := range amounts {
		keys = append(keys, v.Key)
		values = append(values, v.Amount)
	}

	script := ` -- RedisCheckAdjustBatchAmounts
local opv = redis.call('GET', KEYS[1])
if opv ~= false then
	return 'DUPLICATE'
end

local verrs = {}
local result = "FAILURE"
`

	for i := 2; i < len(keys); i++ {
		luaIdx := i + 1
		script += fmt.Sprintf(`
local v%d = redis.call('GET', KEYS[%d])
if v%d == false or v%d + ARGV[%d] < 0 then
	verrs[%d] = ""
end
`, luaIdx, luaIdx, luaIdx, luaIdx, luaIdx, i)
	}

	script += `
for i in pairs(verrs) do
	result = result .. "-" .. i
end

if result ~= "FAILURE" then
	return result
end

redis.call('SET', KEYS[1], 'op', 'EX', ARGV[2])

if ARGV[1] ~= '' then
	local originOpv = redis.call('GET', KEYS[2])
	if originOpv == false then
		redis.call('SET', KEYS[2], 'rollback', 'EX', ARGV[2])
		return
	end
end

`

	for i := 2; i < len(keys); i++ {
		luaIdx := i + 1
		script += fmt.Sprintf(`
redis.call('INCRBY', KEYS[%d], ARGV[%d])
`, luaIdx, luaIdx)
	}

	v, err := rd.Eval(rd.Context(), script, keys, values...).Result()
	logger.Debugf("lua return v: %v err: %v", v, err)
	if err == redis.Nil {
		err = nil
	}

	if err == nil && bb.Op == dtmimp.MsgDoOp && v == "DUPLICATE" { // msg DoAndSubmit should be rejected when duplicate
		return ErrDuplicated
	}

	if err == nil {
		if strv, _ := v.(string); strv != "" {
			results := strings.Split(strv, "-")
			if results[0] == ResultFailure {
				err = ErrFailure

				for i := 1; i < len(results); i++ {
					if idx, _ := strconv.ParseInt(results[i], 0, 64); idx >= 2 {
						amounts[idx-2].Err = err
					}
				}
			}
		}
	}

	return err
}

// RedisCheckAdjustAmount check the value of key is valid and >= amount. then adjust the amount
func (bb *BranchBarrier) RedisCheckAdjustAmount(rd *redis.Client, key string, amount int, barrierExpire int) error {
	bid := bb.newBarrierID()
	bkey1 := fmt.Sprintf("%s-%s-%s-%s", bb.Gid, bb.BranchID, bb.Op, bid)
	originOp := map[string]string{
		dtmimp.OpCancel:     dtmimp.OpTry,
		dtmimp.OpCompensate: dtmimp.OpAction,
	}[bb.Op]
	bkey2 := fmt.Sprintf("%s-%s-%s-%s", bb.Gid, bb.BranchID, originOp, bid)
	v, err := rd.Eval(rd.Context(), ` -- RedisCheckAdjustAmount
local v = redis.call('GET', KEYS[1])
local e1 = redis.call('GET', KEYS[2])

if v == false or v + ARGV[1] < 0 then
	return 'FAILURE'
end

if e1 ~= false then
	return 'DUPLICATE'
end

redis.call('SET', KEYS[2], 'op', 'EX', ARGV[3])

if ARGV[2] ~= '' then
	local e2 = redis.call('GET', KEYS[3])
	if e2 == false then
		redis.call('SET', KEYS[3], 'rollback', 'EX', ARGV[3])
		return
	end
end
redis.call('INCRBY', KEYS[1], ARGV[1])
`, []string{key, bkey1, bkey2}, amount, originOp, barrierExpire).Result()
	logger.Debugf("lua return v: %v err: %v", v, err)
	if err == redis.Nil {
		err = nil
	}
	if err == nil && bb.Op == dtmimp.MsgDoOp && v == "DUPLICATE" { // msg DoAndSubmit should be rejected when duplicate
		return ErrDuplicated
	}
	if err == nil && v == ResultFailure {
		err = ErrFailure
	}
	return err
}

// RedisQueryPrepared query prepared for redis
func (bb *BranchBarrier) RedisQueryPrepared(rd *redis.Client, barrierExpire int) error {
	bkey1 := fmt.Sprintf("%s-%s-%s-%s", bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1)
	v, err := rd.Eval(rd.Context(), ` -- RedisQueryPrepared
local v = redis.call('GET', KEYS[1])
if v == false then
	redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
	v = 'rollback'
end
if v == 'rollback' then
	return 'FAILURE'
end
`, []string{bkey1}, barrierExpire).Result()
	logger.Debugf("lua return v: %v err: %v", v, err)
	if err == redis.Nil {
		err = nil
	}
	if err == nil && v == ResultFailure {
		err = ErrFailure
	}
	return err
}
