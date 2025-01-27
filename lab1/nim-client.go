package main

import (
	"fmt"
	"os"
	"net"
	"time"
	"strconv"
	"io/ioutil"
	"encoding/gob"
	"encoding/json"

	"github.com/DistributedClocks/tracing"
)

/** Config struct **/

type ClientConfig struct {
	ClientAddress        string
	NimServerAddress     string
	TracingServerAddress string
	Secret               []byte
	TracingIdentity      string
}

/** Tracing structs **/

type GameStart struct {
	Seed int8
}

type ClientMove StateMoveMessage

type ServerMoveReceive StateMoveMessage

type GameComplete struct {
	Winner string
}

/** Message structs **/

type StateMoveMessage struct {
	GameState []uint8
	MoveRow   int8
	MoveCount int8
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: client.go [seed]")
		return
	}
	arg, err := strconv.Atoi(os.Args[1])
	CheckErr(err, "Provided seed could not be converted to integer", arg)
	seed := int8(arg)

	config := ReadConfig("config/nim-client-config.json")
	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddress,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	defer tracer.Close()

	trace := tracer.CreateTrace()
	trace.RecordAction(
		GameStart{
			Seed: seed,
		})

	/** YOUR CODE BELOW **/
	// connection to nim server
	conn, err := net.Dial("udp", config.NimServerAddress)
    defer conn.Close()
    if err != nil {
		fmt.Printf("Error: %v", err)
		return
	}

    enc := gob.NewEncoder(conn)
    dec := gob.NewDecoder(conn)

    CurGameState := ClientMove{
        GameState: nil,  // nil indicates "start"
        MoveRow:   -1,   // no move yet
        MoveCount: seed, // send seed
    }

    var CandGameState ServerMoveReceive

    stopChannel := make(chan bool)
    stopChannel <- true
    // go SendClientMove(CurGameState, enc, trace, stopChannel)

    for {
        select {
        case <-stopChannel:
        default:
            go SendClientMove(CurGameState, enc, trace, stopChannel)
        }
        if IsGameOver(CurGameState.GameState) {
            trace.RecordAction(GameComplete{ Winner: "client" })
            stopChannel <- true
            break
        }


        err = dec.Decode(&CandGameState)
        if err != nil {
            fmt.Printf("Error decoding server message: %v\n", err)
            return
        }else{

        trace.RecordAction(CandGameState)
        }
        if ValidateCandGameState(CurGameState, CandGameState) {
            stopChannel <- true

            // Check if server's move ends the game
            if IsGameOver(CandGameState.GameState) {
                trace.RecordAction(GameComplete{ Winner: "server" })
                break
            }

            MakeClientMove(CandGameState, &CurGameState)

            stopChannel = make(chan bool) // create a fresh channel
            go SendClientMove(CurGameState, enc, trace, stopChannel)

        } else {
            fmt.Println("Received an invalid/copy server message, ignoring.")
            continue
        }
    }

    fmt.Println("Game loop finished.")
}

func MakeClientMove(CandGameState ServerMoveReceive, CurGameState *ClientMove) {
    var ClientMoveRow int8 = -1
    var ClientMoveCount int8 = 0

    for i, coins := range CandGameState.GameState {
        if coins > 0 {
            ClientMoveRow = int8(i)
            ClientMoveCount = int8(coins)
            break
        }
    }

    newState := make([]uint8, len(CandGameState.GameState))
    copy(newState, CandGameState.GameState)
    if ClientMoveRow >= 0 && int(ClientMoveRow) < len(newState) {
        if newState[ClientMoveRow] >= uint8(ClientMoveCount) {
            newState[ClientMoveRow] -= uint8(ClientMoveCount)
        }
    }

    CurGameState.GameState = newState
    CurGameState.MoveRow = ClientMoveRow
    CurGameState.MoveCount = ClientMoveCount
}

func IsGameOver(gs []uint8) bool {
    if gs == nil || len(gs) == 0 {
        return false 
    }
    for _, v := range gs {
        if v != 0 {
            return false
        }
    }
    return true
}


func ValidateCandGameState(CurGameState ClientMove, CandGameState ServerMoveReceive) bool {
    if CurGameState.MoveRow == -1 {
        // Compare seed in MoveCount
        if CandGameState.MoveRow == -1 && CurGameState.MoveCount == CandGameState.MoveCount {
            return true
        }
        return false
    }

    if CandGameState.MoveRow < 0 || CandGameState.MoveCount <= 0 {
        return false
    }

    newState := make([]uint8, len(CurGameState.GameState))
    copy(newState, CurGameState.GameState)

    if int(CandGameState.MoveRow) < len(newState) {
        if newState[CandGameState.MoveRow] >= uint8(CandGameState.MoveCount) {
            newState[CandGameState.MoveRow] -= uint8(CandGameState.MoveCount)
        }
    }

    // Compare newState with CandGameState.GameState.
    if len(newState) != len(CandGameState.GameState) {
        return false
    }
    for i := range newState {
        if newState[i] != CandGameState.GameState[i] {
            return false
        }
    }

    return true
}

func SendClientMove(CurGameState ClientMove, enc *gob.Encoder, trace *tracing.Trace, stopChannel <-chan bool) {
    // We wait for a signal in a non-blocking select. 
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-stopChannel:
            // If we get anything from stopChannel, we stop sending.
            fmt.Println("Stopping SendClientMove goroutine.")
            return
        case <-ticker.C:
            // Every second, record the action and send it.
            trace.RecordAction(CurGameState)
            err := enc.Encode(CurGameState)
            if err != nil {
                fmt.Printf("Error encoding CurGameState: %v\n", err)
                return
            }
            fmt.Println("Sent CurGameState via gob.")
        }
    }
}


func ReadConfig(filepath string) *ClientConfig {
    configFile := filepath
    configData, err := ioutil.ReadFile(configFile)
    CheckErr(err, "reading config file")

    config := new(ClientConfig)
    err = json.Unmarshal(configData, config)
    CheckErr(err, "parsing config data")

    return config
}

func CheckErr(err error, errfmsg string, fargs ...interface{}) {
    if err != nil {
        fmt.Fprintf(os.Stderr, errfmsg, fargs...)
        os.Exit(1)
    }
}