package services

import (
	"context"
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// BaseStorage
type BaseStorage struct {
	Client         *mongo.Client
	DBName         string
	CollectionName string
	Tracer         opentracing.Tracer
	BeforeFunc     func(ctx context.Context, methodName string) context.Context
	AfterFunc      func(ctx context.Context, methodName string) context.Context
}

// Model
type Model interface {
	GetID() primitive.ObjectID
	GetHexID() string
	SetHexID(hexID string) error
	SetJSONID(jsonB []byte) error
	SetupTimestamps()
}

func NewMongoClient(ctx context.Context, mongoURI string) (*mongo.Client, error) {
	return mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
}

// NewBaseStorage() is a constructor for BaseStorage struct
func NewBaseStorage(ctx context.Context, mongoURI, dbName, collectionName string) (*BaseStorage, error) {
	client, err := NewMongoClient(ctx, mongoURI)
	if err != nil {
		return nil, err
	}

	return NewBaseStorageWithClient(client, dbName, collectionName), nil
}

func NewBaseStorageWithClient(client *mongo.Client, dbName, collectionName string) *BaseStorage {
	return &BaseStorage{
		Client:         client,
		DBName:         dbName,
		CollectionName: collectionName,
		Tracer:         opentracing.GlobalTracer(),
	}
}

func (s *BaseStorage) before(ctx context.Context, methodName string) context.Context {
	if s.BeforeFunc != nil {
		ctx = s.BeforeFunc(ctx, methodName)
	}

	if s.Tracer != nil {
		_, ctx = opentracing.StartSpanFromContextWithTracer(ctx, s.Tracer, fmt.Sprintf("MongoStorage: %s", methodName))
	}

	return ctx
}

func (s *BaseStorage) after(ctx context.Context, methodName string) context.Context {
	if s.AfterFunc != nil {
		s.AfterFunc(ctx, methodName)
	}

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		span.Finish()
	}

	return ctx
}

// Ping() the mongo server
func (s *BaseStorage) Ping(ctx context.Context) error {
	return s.Client.Ping(ctx, nil)
}

// GetCollection() returns storage collection
func (s *BaseStorage) GetCollection() *mongo.Collection {
	return s.Client.Database(s.DBName).Collection(s.CollectionName)
}

func (s *BaseStorage) CreateIndex(ctx context.Context, k interface{}, o *options.IndexOptions) (string, error) {
	ctx = s.before(ctx, "CreateIndex")
	defer s.after(ctx, "CreateIndex")

	return s.GetCollection().Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    k,
		Options: o,
	})
}

// InsertOne() inserts given Model and returns an ID of inserted document
func (s *BaseStorage) InsertOne(ctx context.Context, m Model, opts ...*options.InsertOneOptions) (string, error) {
	ctx = s.before(ctx, "InsertOne")
	defer s.after(ctx, "InsertOne")

	m.SetupTimestamps()

	b, err := bson.Marshal(m)
	if err != nil {
		return "", err
	}

	res, err := s.GetCollection().InsertOne(ctx, b, opts...)
	if err != nil {
		return "", HandleDuplicationErr(err)
	}

	objectID, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return "", ErrInvalidObjectID
	}

	hexID := objectID.Hex()
	if err := m.SetHexID(hexID); err != nil {
		return "", err
	}

	return hexID, nil
}

// InsertMany()
func (s *BaseStorage) InsertMany(ctx context.Context, docs []interface{}, opts ...*options.InsertManyOptions) ([]string, error) {
	ctx = s.before(ctx, "InsertMany")
	defer s.after(ctx, "InsertMany")

	res, err := s.GetCollection().InsertMany(ctx, docs, opts...)

	if err != nil {
		return []string{}, HandleDuplicationErr(err)
	}

	hexIDs := []string{}

	for _, insertedID := range res.InsertedIDs {
		objectID, ok := insertedID.(primitive.ObjectID)

		if !ok {
			return []string{}, ErrInvalidObjectID
		}

		hexIDs = append(hexIDs, objectID.Hex())
	}

	return hexIDs, nil
}

// UpdateOne() updates given Model
func (s *BaseStorage) UpdateOne(ctx context.Context, m Model, opts ...*options.UpdateOptions) error {
	ctx = s.before(ctx, "UpdateOne")
	defer s.after(ctx, "UpdateOne")

	filter := bson.M{"_id": bson.M{"$eq": m.GetID()}}
	return s.UpdateOneByFilter(ctx, filter, m, opts...)
}

// UpdateByFilter() updates given Model according to provided filter
func (s *BaseStorage) UpdateOneByFilter(ctx context.Context, filter interface{}, m Model, opts ...*options.UpdateOptions) error {
	ctx = s.before(ctx, "UpdateOneByFilter")
	defer s.after(ctx, "UpdateOneByFilter")

	m.SetupTimestamps()

	res, err := s.GetCollection().UpdateOne(
		ctx,
		filter,
		bson.D{primitive.E{Key: "$set", Value: m}},
		opts...,
	)
	if err != nil {
		return HandleDuplicationErr(err)
	}

	if res.MatchedCount != 1 {
		return ErrDocumentNotFound
	}

	if res.ModifiedCount != 1 {
		return ErrDocumentNotModified
	}

	return nil
}

// UpdateMany()
func (s *BaseStorage) UpdateMany(
	ctx context.Context,
	filter, update interface{},
	opts ...*options.UpdateOptions,
) (*mongo.UpdateResult, error) {
	ctx = s.before(ctx, "UpdateMany")
	defer s.after(ctx, "UpdateMany")

	return s.GetCollection().UpdateMany(
		ctx,
		filter,
		update,
		opts...,
	)
}

// ReplaceOne
func (s *BaseStorage) ReplaceOne(
	ctx context.Context,
	filter interface{},
	m Model,
	opts ...*options.ReplaceOptions,
) (*mongo.UpdateResult, error) {
	ctx = s.before(ctx, "ReplaceOne")
	defer s.after(ctx, "ReplaceOne")

	m.SetupTimestamps()

	return s.GetCollection().ReplaceOne(
		ctx,
		filter,
		m,
		opts...,
	)
}

// ReplaceOneByID()
func (s *BaseStorage) ReplaceOneByID(
	ctx context.Context,
	recordID string,
	m Model,
	opts ...*options.ReplaceOptions,
) (*mongo.UpdateResult, error) {
	ctx = s.before(ctx, "ReplaceOneByID")
	defer s.after(ctx, "ReplaceOneByID")

	oid, err := primitive.ObjectIDFromHex(recordID)
	if err != nil {
		return nil, ErrInvalidObjectID
	}

	return s.ReplaceOne(
		ctx,
		bson.M{"_id": bson.M{"$eq": oid}},
		m,
		opts...,
	)
}

// GetOneByID() is trying to find Model by given recordID
func (s *BaseStorage) GetOneByID(
	ctx context.Context,
	recordID string,
	m Model,
	opts ...*options.FindOneOptions,
) error {
	ctx = s.before(ctx, "GetOneByID")
	defer s.after(ctx, "GetOneByID")

	oid, err := primitive.ObjectIDFromHex(recordID)
	if err != nil {
		return ErrInvalidObjectID
	}

	fmt.Println(oid)

	filter := bson.M{"_id": bson.M{"$eq": oid}}

	return s.GetOneByFilter(ctx, filter, m, opts...)
}

// GetOneByFilter() is trying to find Model by provided filter
func (s *BaseStorage) GetOneByFilter(
	ctx context.Context,
	filter interface{},
	m Model,
	opts ...*options.FindOneOptions,
) error {
	ctx = s.before(ctx, "GetOneByFilter")
	defer s.after(ctx, "GetOneByFilter")

	res := s.GetCollection().FindOne(ctx, filter, opts...)
	if err := res.Decode(m); err != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return ErrDocumentNotFound
		}

		return err
	}

	b, err := res.DecodeBytes()

	if err != nil {
		return err
	}

	if err := m.SetJSONID(b.Lookup("_id").Value); err != nil {
		return err
	}

	return nil
}

// GetManyByFilter()
func (s *BaseStorage) GetManyByFilter(
	ctx context.Context,
	filter interface{},
	modelBuilder func() Model,
	opts ...*options.FindOptions,
) ([]Model, error) {
	ctx = s.before(ctx, "GetManyByFilter")
	defer s.after(ctx, "GetManyByFilter")

	filterCtx, filterCancel := context.WithTimeout(ctx, time.Second*3)
	defer filterCancel()

	cur, err := s.FindManyByFilter(filterCtx, filter, opts...)
	if err != nil {
		return nil, err
	}

	closeCtx, closeCancel := context.WithTimeout(ctx, time.Second*1)
	defer closeCancel()
	defer cur.Close(closeCtx)

	var l []Model

	nextCtx, nextCancel := context.WithTimeout(context.Background(), time.Second*3)

	defer nextCancel()

	for cur.Next(nextCtx) {
		m := modelBuilder()
		if err := cur.Decode(m); err != nil {
			return nil, err
		}

		if err := m.SetJSONID(cur.Current.Lookup("_id").Value); err != nil {
			return nil, err
		}

		l = append(l, m)
	}

	return l, nil
}

// FindManyByFilter()
func (s *BaseStorage) FindManyByFilter(
	ctx context.Context,
	filter interface{},
	opts ...*options.FindOptions,
) (*mongo.Cursor, error) {
	ctx = s.before(ctx, "FindManyByFilter")
	defer s.after(ctx, "FindManyByFilter")

	cur, err := s.GetCollection().Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}

	if cur.Err() == nil {
		return cur, nil
	}

	closeCtx, closeCancel := context.WithTimeout(ctx, time.Second*3)
	defer closeCancel()
	defer cur.Close(closeCtx)

	return nil, cur.Err()
}

// UpsertRecord - insert or update existing record. Returns updated model.
func (s *BaseStorage) UpsertRecord(
	ctx context.Context,
	filter interface{},
	update interface{},
	m Model,
) (Model, error) {
	ctx = s.before(ctx, "UpsertRecord")
	defer s.after(ctx, "UpsertRecord")

	opts := options.FindOneAndUpdate().
		SetReturnDocument(options.After).
		SetUpsert(true)

	res := s.GetCollection().FindOneAndUpdate(ctx, filter, update, opts)
	if res.Err() != nil {
		return nil, HandleDuplicationErr(res.Err())
	}
	if err := res.Decode(m); err != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, ErrDocumentNotFound
		}

		return nil, err
	}

	b, err := res.DecodeBytes()
	if err != nil {
		return nil, err
	}

	if err := m.SetJSONID(b.Lookup("_id").Value); err != nil {
		return nil, err
	}

	return m, nil
}

// DeleteManyByFilter() documents by given filters
func (s *BaseStorage) DeleteManyByFilter(
	ctx context.Context,
	filter interface{},
	opts ...*options.DeleteOptions,
) (*mongo.DeleteResult, error) {
	ctx = s.before(ctx, "DeleteManyByFilter")
	defer s.after(ctx, "DeleteManyByFilter")

	return s.GetCollection().DeleteMany(ctx, filter, opts...)
}

// DeleteOneByID() deletes document by given ID
func (s *BaseStorage) DeleteOneByID(ctx context.Context, docID string) error {
	ctx = s.before(ctx, "DeleteOneByID")
	defer s.after(ctx, "DeleteOneByID")

	oid, err := primitive.ObjectIDFromHex(docID)
	if err != nil {
		return ErrInvalidObjectID
	}

	filter := bson.M{"_id": oid}

	r, err := s.DeleteManyByFilter(ctx, filter)
	if r.DeletedCount != 1 {
		return ErrDocumentNotFound
	}

	return err
}

// DropAll() deletes collection from database
func (s *BaseStorage) DropAll(ctx context.Context) error {
	ctx = s.before(ctx, "DropAll")
	defer s.after(ctx, "DropAll")

	return s.GetCollection().Drop(ctx)
}
