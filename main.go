package main

import "errors"

type Row struct {
	Timestamp int
	Lock      Lock
	Write     Write
	Data      int
	Notify    string
	Ack_O     string
}

type Write struct {
	StartTs int
}

type Lock struct {
	Enable bool
	Key    string
}

type Database struct {
	Db map[string][]Row
}

type TimestampOracle struct {
	Index int
}

func (t *TimestampOracle) GenNewTimestamp() int {
	t.Index++
	return t.Index
}

var timestampOracle TimestampOracle

type Transaction struct {
	Data []TransactionData
}

type TransactionData struct {
	key   string
	value int
}

func (d *Database) Write(transaction Transaction) error {

	//Prewrite
	startTs := timestampOracle.GenNewTimestamp()
	for _, t := range transaction.Data {
		for _, r := range d.Db[t.key] {
			//変更予定のデータにロックがないことを確認
			if r.Lock.Enable {
				return errors.New("the data is already locked")
			}
			//start_tsより新しいタイムスタンプがないことを確認
			if r.Timestamp > startTs {
				return errors.New("there are newer timestamp")
			}
		}
	}
	for _, t := range transaction.Data {
		//データを登録しロックする
		newRow := Row{
			Timestamp: startTs,
			Lock:      Lock{Enable: true, Key: transaction.Data[0].key},
			Data:      t.value,
		}
		d.Db[t.key] = append(d.Db[t.key], newRow)
	}

	//Commit
	commitTs := timestampOracle.GenNewTimestamp()
	for _, t := range transaction.Data {
		for _, r := range d.Db[t.key] {
			//対象のロックを外す
			if r.Timestamp == startTs {
				r.Lock.Enable = false
			}
		}
		newRow := Row{
			Timestamp: commitTs,
			Write: Write{
				StartTs: startTs,
			},
		}
		d.Db[t.key] = append(d.Db[t.key], newRow)
	}
	return nil
}

func (d *Database) Read(key string) (int, error) {

loop:
	for {
		startTs := timestampOracle.GenNewTimestamp()

		data := d.Db[key]
		//ロックがないかを検索
		for _, r := range data {
			// startTs以降のデータは読み取らない
			if r.Timestamp > startTs {
				continue
			}
			if r.Lock.Enable {
				//未コミットのログが存在するためstartTsの取得からやり直し
				continue loop
			}
		}

		//最新のデータを返却する
		for i := range data {
			if data[len(data)-i].Write.StartTs != 0 {
				for _, v := range data {
					if v.Timestamp == data[len(data)-i].Write.StartTs {
						return v.Data, nil
					}
				}
			}
		}
	}
}
