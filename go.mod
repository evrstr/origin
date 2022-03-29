module github.com/duanhf2012/origin

go 1.18

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/gomodule/redigo v1.8.3
	github.com/gorilla/websocket v1.4.2
	github.com/json-iterator/go v1.1.10
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

require github.com/golang/protobuf v1.5.0

require (
	github.com/modern-go/concurrent v0.0.0-20180228061459-e0a39a4cb421 // indirect
	github.com/modern-go/reflect2 v0.0.0-20180701023420-4b7aa43c6742 // indirect
	google.golang.org/protobuf v1.26.0-rc.1 // indirect
)

replace github.com/duanhf2012/origin => ../github.com/evrstr/origin
