package mongodb

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/microservices-demo/user/users"
	stdopentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	name     string
	password string
	host     string
	db       = "users"
	//ErrInvalidHexID represents a entity id that is not a valid bson ObjectID
	ErrInvalidHexID = errors.New("Invalid Id Hex")
)

const defaultMongoPeerService = "user-db"

// setMongoDBSpanTags sets common tags for MongoDB spans (called after span creation)
func setMongoDBSpanTags(span stdopentracing.Span, collection string) {
	// Database-related tags
	ext.DBType.Set(span, "mongodb")
	span.SetTag("db.system", "mongodb")
	if collection != "" {
		span.SetTag("db.collection", collection)
	}
	// Peer address information
	if host != "" {
		ext.PeerAddress.Set(span, host)
	}
}

func resolvePeerServiceName(rawHost string) string {
	if rawHost == "" {
		return defaultMongoPeerService
	}

	peer := strings.TrimSpace(strings.Split(rawHost, ",")[0])
	if peer == "" {
		return defaultMongoPeerService
	}

	if strings.Contains(peer, "://") {
		if parsedURL, err := url.Parse(peer); err == nil && parsedURL.Host != "" {
			peer = parsedURL.Host
		}
	}

	peer = strings.TrimPrefix(peer, "[")
	peer = strings.TrimSuffix(peer, "]")

	if hostOnly, _, err := net.SplitHostPort(peer); err == nil && hostOnly != "" {
		peer = hostOnly
	} else if strings.Count(peer, ":") == 1 {
		parts := strings.SplitN(peer, ":", 2)
		peer = parts[0]
	}

	peer = strings.TrimSpace(peer)
	if peer == "" {
		return defaultMongoPeerService
	}

	switch peer {
	case "localhost", "127.0.0.1", "::1":
		return defaultMongoPeerService
	}

	if strings.Contains(peer, ".") {
		parts := strings.Split(peer, ".")
		if parts[0] != "" {
			return parts[0]
		}
	}

	return peer
}

// startMongoDBSpan creates a new span with CLIENT kind for MongoDB operations.
func startMongoDBSpan(ctx context.Context, name string) stdopentracing.Span {
	startOpts := []stdopentracing.StartSpanOption{
		ext.SpanKindRPCClient,
		stdopentracing.Tag{Key: string(ext.PeerService), Value: resolvePeerServiceName(host)},
	}

	var span stdopentracing.Span
	if parentSpan := stdopentracing.SpanFromContext(ctx); parentSpan != nil {
		startOpts = append(startOpts, stdopentracing.ChildOf(parentSpan.Context()))
		span = stdopentracing.StartSpan(name, startOpts...)
	} else {
		span = stdopentracing.GlobalTracer().StartSpan(name, startOpts...)
	}
	return span
}

func recordSpanError(span stdopentracing.Span, err error) {
	if err == nil {
		return
	}
	span.SetTag("error", true)
	span.SetTag("error.type", fmt.Sprintf("%T", err))
	span.SetTag("error.message", err.Error())
}

func mongoOpError(op, collection string, err error) error {
	if err == nil {
		return nil
	}
	if collection == "" {
		return fmt.Errorf("mongodb op=%s: %w", op, err)
	}
	return fmt.Errorf("mongodb op=%s collection=%s: %w", op, collection, err)
}

func init() {
	flag.StringVar(&name, "mongo-user", os.Getenv("MONGO_USER"), "Mongo user")
	flag.StringVar(&password, "mongo-password", os.Getenv("MONGO_PASS"), "Mongo password")
	flag.StringVar(&host, "mongo-host", os.Getenv("MONGO_HOST"), "Mongo host")
}

// Mongo meets the Database interface requirements
type Mongo struct {
	//Session is a MongoDB Session
	Session *mgo.Session
}

// Init MongoDB
func (m *Mongo) Init() error {
	u := getURL()
	const dialTimeout = 5 * time.Second

	var err error
	m.Session, err = mgo.DialWithTimeout(u.String(), dialTimeout)
	if err != nil {
		return fmt.Errorf("mongodb dial host=%s timeout=%s: %w", host, dialTimeout, err)
	}
	if err := m.EnsureIndexes(); err != nil {
		return fmt.Errorf("mongodb init host=%s: %w", host, err)
	}
	return nil
}

// MongoUser is a wrapper for the users
type MongoUser struct {
	users.User `bson:",inline"`
	ID         bson.ObjectId   `bson:"_id"`
	AddressIDs []bson.ObjectId `bson:"addresses"`
	CardIDs    []bson.ObjectId `bson:"cards"`
}

// New Returns a new MongoUser
func New() MongoUser {
	u := users.New()
	return MongoUser{
		User:       u,
		AddressIDs: make([]bson.ObjectId, 0),
		CardIDs:    make([]bson.ObjectId, 0),
	}
}

// AddUserIDs adds userID as string to user
func (mu *MongoUser) AddUserIDs() {
	if mu.User.Addresses == nil {
		mu.User.Addresses = make([]users.Address, 0)
	}
	for _, id := range mu.AddressIDs {
		mu.User.Addresses = append(mu.User.Addresses, users.Address{
			ID: id.Hex(),
		})
	}
	if mu.User.Cards == nil {
		mu.User.Cards = make([]users.Card, 0)
	}
	for _, id := range mu.CardIDs {
		mu.User.Cards = append(mu.User.Cards, users.Card{ID: id.Hex()})
	}
	mu.User.UserID = mu.ID.Hex()
}

// MongoAddress is a wrapper for Address
type MongoAddress struct {
	users.Address `bson:",inline"`
	ID            bson.ObjectId `bson:"_id"`
}

// AddID ObjectID as string
func (m *MongoAddress) AddID() {
	m.Address.ID = m.ID.Hex()
}

// MongoCard is a wrapper for Card
type MongoCard struct {
	users.Card `bson:",inline"`
	ID         bson.ObjectId `bson:"_id"`
}

// AddID ObjectID as string
func (m *MongoCard) AddID() {
	m.Card.ID = m.ID.Hex()
}

// CreateUser Insert user to MongoDB, including connected addresses and cards, update passed in user with Ids
func (m *Mongo) CreateUser(ctx context.Context, u *users.User) error {
	span := startMongoDBSpan(ctx, "mongodb: create user")
	setMongoDBSpanTags(span, "customers")
	defer span.Finish()

	s := m.Session.Copy()
	defer s.Close()
	id := bson.NewObjectId()
	mu := New()
	mu.User = *u
	mu.ID = id
	var carderr error
	var addrerr error
	mu.CardIDs, carderr = m.createCards(ctx, u.Cards)
	mu.AddressIDs, addrerr = m.createAddresses(ctx, u.Addresses)
	c := s.DB("").C("customers")
	_, err := c.UpsertId(mu.ID, mu)
	if err != nil {
		wrappedErr := mongoOpError("create_user", "customers", err)
		recordSpanError(span, wrappedErr)
		// Gonna clean up if we can, ignore error
		// because the user save error takes precedence.
		if cleanupErr := m.cleanAttributes(ctx, mu); cleanupErr != nil {
			return errors.Join(wrappedErr, cleanupErr)
		}
		return wrappedErr
	}
	mu.User.UserID = mu.ID.Hex()
	if carderr != nil || addrerr != nil {
		joinedErr := errors.Join(carderr, addrerr)
		recordSpanError(span, joinedErr)
		return joinedErr
	}
	*u = mu.User
	return nil
}

func (m *Mongo) createCards(_ context.Context, cs []users.Card) ([]bson.ObjectId, error) {
	s := m.Session.Copy()
	defer s.Close()
	ids := make([]bson.ObjectId, 0)
	for k, ca := range cs {
		id := bson.NewObjectId()
		mc := MongoCard{Card: ca, ID: id}
		c := s.DB("").C("cards")
		_, err := c.UpsertId(mc.ID, mc)
		if err != nil {
			return ids, mongoOpError("create_card_attribute", "cards", err)
		}
		ids = append(ids, id)
		cs[k].ID = id.Hex()
	}
	return ids, nil
}

func (m *Mongo) createAddresses(_ context.Context, as []users.Address) ([]bson.ObjectId, error) {
	ids := make([]bson.ObjectId, 0)
	s := m.Session.Copy()
	defer s.Close()
	for k, a := range as {
		id := bson.NewObjectId()
		ma := MongoAddress{Address: a, ID: id}
		c := s.DB("").C("addresses")
		_, err := c.UpsertId(ma.ID, ma)
		if err != nil {
			return ids, mongoOpError("create_address_attribute", "addresses", err)
		}
		ids = append(ids, id)
		as[k].ID = id.Hex()
	}
	return ids, nil
}

func (m *Mongo) cleanAttributes(_ context.Context, mu MongoUser) error {
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("addresses")
	_, err := c.RemoveAll(bson.M{"_id": bson.M{"$in": mu.AddressIDs}})
	if err != nil {
		return mongoOpError("cleanup_addresses", "addresses", err)
	}
	c = s.DB("").C("cards")
	_, err = c.RemoveAll(bson.M{"_id": bson.M{"$in": mu.CardIDs}})
	return mongoOpError("cleanup_cards", "cards", err)
}

func (m *Mongo) appendAttributeId(_ context.Context, attr string, id bson.ObjectId, userid string) error {
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("customers")
	return mongoOpError("append_attribute_id", "customers", c.Update(
		bson.M{"_id": bson.ObjectIdHex(userid)},
		bson.M{"$addToSet": bson.M{attr: id}},
	))
}

func (m *Mongo) removeAttributeId(_ context.Context, attr string, id bson.ObjectId, userid string) error {
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("customers")
	return mongoOpError("remove_attribute_id", "customers", c.Update(
		bson.M{"_id": bson.ObjectIdHex(userid)},
		bson.M{"$pull": bson.M{attr: id}},
	))
}

// GetUserByName Get user by their name
func (m *Mongo) GetUserByName(ctx context.Context, name string) (users.User, error) {
	span := startMongoDBSpan(ctx, "mongodb: find user by name")
	setMongoDBSpanTags(span, "customers")
	defer span.Finish()

	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("customers")
	mu := New()
	err := c.Find(bson.M{"username": name}).One(&mu)
	if err != nil {
		err = mongoOpError("find_user_by_name", "customers", err)
		recordSpanError(span, err)
	}
	mu.AddUserIDs()
	return mu.User, err
}

// GetUser Get user by their object id
func (m *Mongo) GetUser(ctx context.Context, id string) (users.User, error) {
	span := startMongoDBSpan(ctx, "mongodb: find user by id")
	setMongoDBSpanTags(span, "customers")
	span.SetTag("user.id", id)
	defer span.Finish()

	s := m.Session.Copy()
	defer s.Close()
	if !bson.IsObjectIdHex(id) {
		err := fmt.Errorf("mongodb get_user id=%s: %w", id, ErrInvalidHexID)
		recordSpanError(span, err)
		return users.New(), err
	}
	c := s.DB("").C("customers")
	mu := New()
	err := c.FindId(bson.ObjectIdHex(id)).One(&mu)
	if err != nil {
		err = mongoOpError("find_user_by_id", "customers", err)
		recordSpanError(span, err)
	}
	mu.AddUserIDs()
	return mu.User, err
}

// GetUsers Get all users
func (m *Mongo) GetUsers(ctx context.Context) ([]users.User, error) {
	span := startMongoDBSpan(ctx, "mongodb: find all users")
	setMongoDBSpanTags(span, "customers")
	defer span.Finish()

	// TODO: add paginations
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("customers")
	var mus []MongoUser
	err := c.Find(nil).All(&mus)
	if err != nil {
		err = mongoOpError("find_all_users", "customers", err)
		recordSpanError(span, err)
	} else {
		span.SetTag("result.count", len(mus))
	}
	us := make([]users.User, 0)
	for _, mu := range mus {
		mu.AddUserIDs()
		us = append(us, mu.User)
	}
	return us, err
}

// GetUserAttributes given a user, load all cards and addresses connected to that user
func (m *Mongo) GetUserAttributes(ctx context.Context, u *users.User) error {
	s := m.Session.Copy()
	defer s.Close()

	// Fetch addresses - directly connect to HTTP request span
	addrSpan := startMongoDBSpan(ctx, "mongodb: find addresses")
	setMongoDBSpanTags(addrSpan, "addresses")
	addrSpan.SetTag("user.id", u.UserID)
	ids := make([]bson.ObjectId, 0)
	for _, a := range u.Addresses {
		if !bson.IsObjectIdHex(a.ID) {
			err := fmt.Errorf("mongodb get_user_attributes address_id=%s: %w", a.ID, ErrInvalidHexID)
			recordSpanError(addrSpan, err)
			addrSpan.Finish()
			return err
		}
		ids = append(ids, bson.ObjectIdHex(a.ID))
	}
	var ma []MongoAddress
	c := s.DB("").C("addresses")
	err := c.Find(bson.M{"_id": bson.M{"$in": ids}}).All(&ma)
	if err != nil {
		err = mongoOpError("find_user_addresses", "addresses", err)
		recordSpanError(addrSpan, err)
		addrSpan.Finish()
		return err
	}
	addrSpan.SetTag("result.count", len(ma))
	addrSpan.Finish()

	na := make([]users.Address, 0)
	for _, a := range ma {
		a.Address.ID = a.ID.Hex()
		na = append(na, a.Address)
	}
	u.Addresses = na

	// Fetch cards - directly connect to HTTP request span
	cardSpan := startMongoDBSpan(ctx, "mongodb: find cards")
	setMongoDBSpanTags(cardSpan, "cards")
	cardSpan.SetTag("user.id", u.UserID)
	ids = make([]bson.ObjectId, 0)
	for _, c := range u.Cards {
		if !bson.IsObjectIdHex(c.ID) {
			err := fmt.Errorf("mongodb get_user_attributes card_id=%s: %w", c.ID, ErrInvalidHexID)
			recordSpanError(cardSpan, err)
			cardSpan.Finish()
			return err
		}
		ids = append(ids, bson.ObjectIdHex(c.ID))
	}
	var mc []MongoCard
	c = s.DB("").C("cards")
	err = c.Find(bson.M{"_id": bson.M{"$in": ids}}).All(&mc)
	if err != nil {
		err = mongoOpError("find_user_cards", "cards", err)
		recordSpanError(cardSpan, err)
		cardSpan.Finish()
		return err
	}
	cardSpan.SetTag("result.count", len(mc))
	cardSpan.Finish()

	nc := make([]users.Card, 0)
	for _, ca := range mc {
		ca.Card.ID = ca.ID.Hex()
		nc = append(nc, ca.Card)
	}
	u.Cards = nc
	return nil
}

// GetCard Gets card by objects Id
func (m *Mongo) GetCard(ctx context.Context, id string) (users.Card, error) {
	span := startMongoDBSpan(ctx, "mongodb: find card by id")
	setMongoDBSpanTags(span, "cards")
	span.SetTag("card.id", id)
	defer span.Finish()

	s := m.Session.Copy()
	defer s.Close()
	if !bson.IsObjectIdHex(id) {
		err := fmt.Errorf("mongodb get_card id=%s: %w", id, ErrInvalidHexID)
		recordSpanError(span, err)
		return users.Card{}, err
	}
	c := s.DB("").C("cards")
	mc := MongoCard{}
	err := c.FindId(bson.ObjectIdHex(id)).One(&mc)
	if err != nil {
		err = mongoOpError("find_card_by_id", "cards", err)
		recordSpanError(span, err)
	}
	mc.AddID()
	return mc.Card, err
}

// GetCards Gets all cards
func (m *Mongo) GetCards(ctx context.Context) ([]users.Card, error) {
	span := startMongoDBSpan(ctx, "mongodb: find all cards")
	setMongoDBSpanTags(span, "cards")
	defer span.Finish()

	// TODO: add pagination
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("cards")
	var mcs []MongoCard
	err := c.Find(nil).All(&mcs)
	if err != nil {
		err = mongoOpError("find_all_cards", "cards", err)
		recordSpanError(span, err)
	} else {
		span.SetTag("result.count", len(mcs))
	}
	cs := make([]users.Card, 0)
	for _, mc := range mcs {
		mc.AddID()
		cs = append(cs, mc.Card)
	}
	return cs, err
}

// CreateCard adds card to MongoDB
func (m *Mongo) CreateCard(ctx context.Context, ca *users.Card, userid string) error {
	span := startMongoDBSpan(ctx, "mongodb: create card")
	setMongoDBSpanTags(span, "cards")
	span.SetTag("user.id", userid)
	defer span.Finish()

	if userid != "" && !bson.IsObjectIdHex(userid) {
		err := fmt.Errorf("mongodb create_card user_id=%s: %w", userid, ErrInvalidHexID)
		recordSpanError(span, err)
		return err
	}
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("cards")
	id := bson.NewObjectId()
	mc := MongoCard{Card: *ca, ID: id}
	_, err := c.UpsertId(mc.ID, mc)
	if err != nil {
		err = mongoOpError("create_card", "cards", err)
		recordSpanError(span, err)
		return err
	}
	// Address for anonymous user
	if userid != "" {
		err = m.appendAttributeId(ctx, "cards", mc.ID, userid)
		if err != nil {
			recordSpanError(span, err)
			return err
		}
	}
	mc.AddID()
	*ca = mc.Card
	return err
}

// GetAddress Gets an address by object Id
func (m *Mongo) GetAddress(ctx context.Context, id string) (users.Address, error) {
	span := startMongoDBSpan(ctx, "mongodb: find address by id")
	setMongoDBSpanTags(span, "addresses")
	span.SetTag("address.id", id)
	defer span.Finish()

	s := m.Session.Copy()
	defer s.Close()
	if !bson.IsObjectIdHex(id) {
		err := fmt.Errorf("mongodb get_address id=%s: %w", id, ErrInvalidHexID)
		recordSpanError(span, err)
		return users.Address{}, err
	}
	c := s.DB("").C("addresses")
	ma := MongoAddress{}
	err := c.FindId(bson.ObjectIdHex(id)).One(&ma)
	if err != nil {
		err = mongoOpError("find_address_by_id", "addresses", err)
		recordSpanError(span, err)
	}
	ma.AddID()
	return ma.Address, err
}

// GetAddresses gets all addresses
func (m *Mongo) GetAddresses(ctx context.Context) ([]users.Address, error) {
	span := startMongoDBSpan(ctx, "mongodb: find all addresses")
	setMongoDBSpanTags(span, "addresses")
	defer span.Finish()

	// TODO: add pagination
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("addresses")
	var mas []MongoAddress
	err := c.Find(nil).All(&mas)
	if err != nil {
		err = mongoOpError("find_all_addresses", "addresses", err)
		recordSpanError(span, err)
	} else {
		span.SetTag("result.count", len(mas))
	}
	as := make([]users.Address, 0)
	for _, ma := range mas {
		ma.AddID()
		as = append(as, ma.Address)
	}
	return as, err
}

// CreateAddress Inserts Address into MongoDB
func (m *Mongo) CreateAddress(ctx context.Context, a *users.Address, userid string) error {
	span := startMongoDBSpan(ctx, "mongodb: create address")
	setMongoDBSpanTags(span, "addresses")
	span.SetTag("user.id", userid)
	defer span.Finish()

	if userid != "" && !bson.IsObjectIdHex(userid) {
		err := fmt.Errorf("mongodb create_address user_id=%s: %w", userid, ErrInvalidHexID)
		recordSpanError(span, err)
		return err
	}
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C("addresses")
	id := bson.NewObjectId()
	ma := MongoAddress{Address: *a, ID: id}
	_, err := c.UpsertId(ma.ID, ma)
	if err != nil {
		err = mongoOpError("create_address", "addresses", err)
		recordSpanError(span, err)
		return err
	}
	// Address for anonymous user
	if userid != "" {
		err = m.appendAttributeId(ctx, "addresses", ma.ID, userid)
		if err != nil {
			recordSpanError(span, err)
			return err
		}
	}
	ma.AddID()
	*a = ma.Address
	return err
}

// Delete removes an entity from MongoDB
func (m *Mongo) Delete(ctx context.Context, entity, id string) error {
	span := startMongoDBSpan(ctx, "mongodb: delete entity")
	setMongoDBSpanTags(span, entity)
	span.SetTag("entity.id", id)
	defer span.Finish()

	if !bson.IsObjectIdHex(id) {
		err := fmt.Errorf("mongodb delete entity=%s id=%s: %w", entity, id, ErrInvalidHexID)
		recordSpanError(span, err)
		return err
	}
	s := m.Session.Copy()
	defer s.Close()
	c := s.DB("").C(entity)
	if entity == "customers" {
		u, err := m.GetUser(ctx, id)
		if err != nil {
			recordSpanError(span, err)
			return err
		}
		aids := make([]bson.ObjectId, 0)
		for _, a := range u.Addresses {
			aids = append(aids, bson.ObjectIdHex(a.ID))
		}
		cids := make([]bson.ObjectId, 0)
		for _, c := range u.Cards {
			cids = append(cids, bson.ObjectIdHex(c.ID))
		}
		ac := s.DB("").C("addresses")
		if _, err := ac.RemoveAll(bson.M{"_id": bson.M{"$in": aids}}); err != nil {
			err = mongoOpError("delete_customer_addresses", "addresses", err)
			recordSpanError(span, err)
			return err
		}
		cc := s.DB("").C("cards")
		if _, err := cc.RemoveAll(bson.M{"_id": bson.M{"$in": cids}}); err != nil {
			err = mongoOpError("delete_customer_cards", "cards", err)
			recordSpanError(span, err)
			return err
		}
	} else {
		c := s.DB("").C("customers")
		if _, err := c.UpdateAll(
			bson.M{},
			bson.M{"$pull": bson.M{entity: bson.ObjectIdHex(id)}},
		); err != nil {
			err = mongoOpError("remove_customer_reference", "customers", err)
			recordSpanError(span, err)
			return err
		}
	}
	err := c.Remove(bson.M{"_id": bson.ObjectIdHex(id)})
	if err != nil {
		err = mongoOpError("delete_entity", entity, err)
		recordSpanError(span, err)
	}
	return err
}

func getURL() url.URL {
	ur := url.URL{
		Scheme: "mongodb",
		Host:   host,
		Path:   db,
	}
	if name != "" {
		u := url.UserPassword(name, password)
		ur.User = u
	}
	return ur
}

// EnsureIndexes ensures username is unique
func (m *Mongo) EnsureIndexes() error {
	s := m.Session.Copy()
	defer s.Close()
	i := mgo.Index{
		Key:        []string{"username"},
		Unique:     true,
		DropDups:   true,
		Background: true,
		Sparse:     false,
	}
	c := s.DB("").C("customers")
	if err := c.EnsureIndex(i); err != nil {
		return mongoOpError("ensure_index", "customers", err)
	}
	return nil
}

func (m *Mongo) Ping(context.Context) error {
	s := m.Session.Copy()
	defer s.Close()
	return mongoOpError("ping", "users", s.Ping())
}
