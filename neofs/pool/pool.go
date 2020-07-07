package pool

import (
	"context"
	"crypto/ecdsa"
	crand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/grpclog"

	"github.com/nspcc-dev/neofs-api-go/service"
	"github.com/nspcc-dev/neofs-api-go/state"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

type (
	node struct {
		index   int32
		address string
		weight  uint32
		usedAt  time.Time
		conn    *grpc.ClientConn
	}

	Client interface {
		GetConnection(context.Context) (*grpc.ClientConn, error)
	}

	Pool interface {
		Client

		Close()
		Status() error
		ReBalance(ctx context.Context)
	}

	Peer struct {
		Address string
		Weight  float64
	}

	Config struct {
		keepalive.ClientParameters

		ConnectionTTL  time.Duration
		ConnectTimeout time.Duration
		RequestTimeout time.Duration

		Peers []Peer

		GRPCVerbose bool
		GRPCLogger  grpclog.LoggerV2

		Logger     *zap.Logger
		PrivateKey *ecdsa.PrivateKey
	}

	pool struct {
		log *zap.Logger

		ttl time.Duration

		conTimeout time.Duration
		reqTimeout time.Duration
		opts       keepalive.ClientParameters

		currentIdx  *atomic.Int32
		currentConn *grpc.ClientConn

		reqHealth *state.HealthRequest

		*sync.Mutex
		nodes []*node
		keys  []uint32
		conns map[uint32][]*node

		unhealthy *atomic.Error
	}
)

var (
	errBootstrapping        = errors.New("bootstrapping")
	errEmptyConnection      = errors.New("empty connection")
	errNoHealthyConnections = errors.New("no active connections")
)

func New(cfg *Config) (Pool, error) {
	p := &pool{
		log:   cfg.Logger,
		Mutex: new(sync.Mutex),
		keys:  make([]uint32, 0),
		nodes: make([]*node, 0),
		conns: make(map[uint32][]*node),

		currentIdx: atomic.NewInt32(-1),

		ttl: cfg.ConnectionTTL,

		conTimeout: cfg.ConnectTimeout,
		reqTimeout: cfg.RequestTimeout,
		opts:       cfg.ClientParameters,

		unhealthy: atomic.NewError(errBootstrapping),
	}

	if cfg.GRPCVerbose {
		grpclog.SetLoggerV2(cfg.GRPCLogger)
	}

	buf := make([]byte, 8)
	if _, err := crand.Read(buf); err != nil {
		return nil, err
	}

	seed := binary.BigEndian.Uint64(buf)
	rand.Seed(int64(seed))
	cfg.Logger.Info("used random seed", zap.Uint64("seed", seed))

	p.reqHealth = new(state.HealthRequest)
	p.reqHealth.SetTTL(service.NonForwardingTTL)

	if err := service.SignRequestData(cfg.PrivateKey, p.reqHealth); err != nil {
		return nil, errors.Wrap(err, "could not sign `HealthRequest`")
	}

	for i := range cfg.Peers {
		if cfg.Peers[i].Address == "" {
			cfg.Logger.Warn("skip, empty address")
			break
		}

		p.nodes = append(p.nodes, &node{
			index:   int32(i),
			address: cfg.Peers[i].Address,
			weight:  uint32(cfg.Peers[i].Weight * 100),
		})

		cfg.Logger.Info("add new peer",
			zap.String("address", p.nodes[i].address),
			zap.Uint32("weight", p.nodes[i].weight))
	}

	return p, nil
}

func (p *pool) Status() error {
	return p.unhealthy.Load()
}

func (p *pool) Close() {
	p.Lock()
	defer p.Unlock()

	for i := range p.nodes {
		if p.nodes[i] == nil || p.nodes[i].conn == nil {
			continue
		}

		p.log.Warn("close connection",
			zap.String("address", p.nodes[i].address),
			zap.Error(p.nodes[i].conn.Close()))
	}
}

func (p *pool) ReBalance(ctx context.Context) {
	p.Lock()
	defer func() {
		p.Unlock()

		_, err := p.GetConnection(ctx)
		p.unhealthy.Store(err)
	}()

	keys := make(map[uint32]struct{})

	p.log.Info("re-balancing connections")

	for i := range p.nodes {
		var (
			idx    = -1
			exists bool
			err    error
			start  = time.Now()
			conn   = p.nodes[i].conn
			weight = p.nodes[i].weight
		)

		if err = ctx.Err(); err != nil {
			p.log.Warn("something went wrong", zap.Error(err))
			p.unhealthy.Store(err)

			return
		}

		if conn == nil {
			p.log.Debug("empty connection, try to connect",
				zap.String("address", p.nodes[i].address))

			ctx, cancel := context.WithTimeout(ctx, p.conTimeout)
			conn, err = grpc.DialContext(ctx, p.nodes[i].address,
				grpc.WithBlock(),
				grpc.WithInsecure(),
				grpc.WithKeepaliveParams(p.opts))
			cancel()

			if err != nil || conn == nil {
				p.log.Warn("skip, could not connect to node",
					zap.String("address", p.nodes[i].address),
					zap.Stringer("elapsed", time.Since(start)),
					zap.Error(err))
				continue
			}

			p.nodes[i].conn = conn
			p.nodes[i].usedAt = time.Now()
			p.log.Debug("connected to node", zap.String("address", p.nodes[i].address))
		}

		for j := range p.conns[weight] {
			if p.conns[weight][j] != nil && p.conns[weight][j].conn == conn {
				idx = j
				exists = true
				break
			}
		}

		usedAt := time.Since(p.nodes[i].usedAt)

		// if something wrong with connection (bad state, unhealthy or not used a long time), try to close it and remove
		if err = p.isAlive(ctx, conn); err != nil || usedAt > p.ttl {
			p.log.Warn("connection not alive",
				zap.String("address", p.nodes[i].address),
				zap.Stringer("since", usedAt),
				zap.Error(err))

			if exists {
				// remove from connections
				p.conns[weight] = append(p.conns[weight][:idx], p.conns[weight][idx+1:]...)
			}

			if err = conn.Close(); err != nil {
				p.log.Warn("could not close bad connection",
					zap.String("address", p.nodes[i].address),
					zap.Stringer("since", usedAt),
					zap.Error(err))
			}

			if p.nodes[i].conn != nil {
				p.nodes[i].conn = nil
			}
			continue
		}

		keys[weight] = struct{}{}

		p.log.Debug("connection alive",
			zap.String("address", p.nodes[i].address),
			zap.Stringer("since", usedAt))

		if !exists {
			p.conns[weight] = append(p.conns[weight], p.nodes[i])
		}
	}

	p.keys = p.keys[:0]
	for w := range keys {
		p.keys = append(p.keys, w)
	}

	sort.Slice(p.keys, func(i, j int) bool {
		return p.keys[i] > p.keys[j]
	})
}

func (p *pool) GetConnection(ctx context.Context) (*grpc.ClientConn, error) {
	p.Lock()
	defer p.Unlock()

	if err := p.isAlive(ctx, p.currentConn); err == nil {
		if id := p.currentIdx.Load(); id != -1 && p.nodes[id] != nil {
			p.nodes[id].usedAt = time.Now()
		}

		return p.currentConn, nil
	}

	for _, w := range p.keys {
		switch ln := len(p.conns[w]); ln {
		case 0:
			continue
		case 1:
			p.currentConn = p.conns[w][0].conn
			p.conns[w][0].usedAt = time.Now()
			p.currentIdx.Store(p.conns[w][0].index)
			return p.currentConn, nil
		default: // > 1
			i := rand.Intn(ln)
			p.currentConn = p.conns[w][i].conn
			p.conns[w][i].usedAt = time.Now()
			p.currentIdx.Store(p.conns[w][i].index)
			return p.currentConn, nil
		}
	}

	p.currentConn = nil
	p.currentIdx.Store(-1)

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	return nil, errNoHealthyConnections
}

func (p *pool) isAlive(ctx context.Context, cur *grpc.ClientConn) error {
	if cur == nil {
		return errEmptyConnection
	}

	switch st := cur.GetState(); st {
	case connectivity.Idle, connectivity.Ready, connectivity.Connecting:
		ctx, cancel := context.WithTimeout(ctx, p.reqTimeout)
		defer cancel()

		res, err := state.NewStatusClient(cur).HealthCheck(ctx, p.reqHealth)
		if err != nil {
			p.log.Warn("could not fetch health-check", zap.Error(err))

			return err
		} else if !res.Healthy {
			return errors.New(res.Status)
		}

		return nil
	default:
		return errors.New(st.String())
	}
}
